import sqlite3
import time
import logging
import logging.handlers
import sys
import os
import threading
from collections import deque
from datetime import datetime
from pymodbus.client import ModbusTcpClient
from tag_loader import load_tags

# Marca de tiempo del arranque del proceso — para calcular uptime sin
# depender de systemd. Si el proceso se reinicia via os.execv (auto-update),
# esto se resetea al momento del nuevo proceso, lo cual es lo correcto.
PROCESS_START = time.time()

# Ring buffer con las últimas N latencias del ciclo Modbus. Se expone vía
# /api/status para alimentar el sparkline del dashboard.
LATENCY_HISTORY: deque = deque(maxlen=60)

# ---------------------------------------------------------------------------
# LOGGING SETUP
# Salida a consola Y a archivo rotativo (5 archivos de 1 MB c/u)
# ---------------------------------------------------------------------------

LOG_DIR  = "logs"
LOG_FILE = os.path.join(LOG_DIR, "logger.log")

os.makedirs(LOG_DIR, exist_ok=True)

log_formatter = logging.Formatter(
    fmt="%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

file_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=1_000_000, backupCount=5, encoding="utf-8"
)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)   # cambiar a DEBUG para ver todo en consola

# Logger raíz del proyecto
root_log = logging.getLogger("plc_logger")
root_log.setLevel(logging.DEBUG)
root_log.addHandler(file_handler)
root_log.addHandler(console_handler)

# Silenciar pymodbus excepto warnings
logging.getLogger("pymodbus").setLevel(logging.WARNING)

log        = logging.getLogger("plc_logger.main")
log_db     = logging.getLogger("plc_logger.db")
log_modbus = logging.getLogger("plc_logger.modbus")
log_events = logging.getLogger("plc_logger.events")

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------

PLC_IP         = os.environ.get("PLC_IP", "10.10.145.244")
PLC_PORT       = int(os.environ.get("PLC_PORT", "502"))
MAX_EVENTS     = 1_000_000
# Schneider M221: por default el server Modbus responde con ID 255.
# Si en EcoStruxure Machine Expert Basic se habilita "Modbus Mapping",
# se puede definir un ID custom (típicamente 1). Confirmar con el PLC físico.
DEVICE_ID      = 1
MIN_DELTA      = 0.1       # Debounce en segundos
POLL_INTERVAL  = 0.5       # Tiempo entre lecturas (segundos)

# Parámetros del cliente Modbus
MODBUS_TIMEOUT = 3     # segundos — socket timeout
MODBUS_RETRIES = 3     # reintentos automáticos por request fallido
BACKOFF_BASE   = 1.0   # backoff inicial en segundos
BACKOFF_MAX    = 30.0  # backoff máximo en segundos

# ---------------------------------------------------------------------------
# ESTADO GLOBAL DE CONEXIÓN
# ---------------------------------------------------------------------------

connection_status = {
    "connected":      False,
    "last_connected": None,
    "last_error":     None,
    "retries":        0,
    "last_cycle_ms":  None,
}

event_counter = 0

# Señal que dispara una recarga en vivo de los tags (xlsx u overrides cambiaron).
# La setea main.py al aceptar un upload / edit / rollback. El loop la chequea
# al inicio de cada ciclo y la limpia tras recargar.
reload_event = threading.Event()


# ---------------------------------------------------------------------------
# TAGS
# ---------------------------------------------------------------------------

def fetch_overrides(conn):
    """Devuelve overrides como dict {address: {symbol, description, signal_type}}."""
    try:
        cur = conn.execute(
            "SELECT address, symbol, description, signal_type FROM tag_overrides"
        )
        return {
            r[0]: {"symbol": r[1], "description": r[2], "signal_type": r[3]}
            for r in cur.fetchall()
        }
    except sqlite3.OperationalError:
        # Tabla aún no creada (primer arranque antes de init_db).
        return {}


def load_tags_safe(db_conn=None):
    """
    Carga los tags del xlsx activo aplicando overrides de la DB si está disponible.
    Sin tags no hay mapeo I/O y el logger no arranca.
    """
    log.info("Cargando tags desde tag_loader...")
    try:
        overrides = fetch_overrides(db_conn) if db_conn is not None else {}
        tags = load_tags(overrides=overrides)
        if not tags:
            log.critical("tag_loader devolvió 0 tags — revisar xlsx y formato.")
            return []
        n_in  = sum(1 for t in tags if t["type"] == "INPUT")
        n_out = sum(1 for t in tags if t["type"] == "OUTPUT")
        n_ov  = sum(1 for t in tags if t.get("overridden"))
        log.info(f"Tags cargados: {len(tags)} ({n_in} INPUT, {n_out} OUTPUT, {n_ov} con override)")
        return tags
    except Exception as e:
        log.critical(f"No se pudieron cargar los tags: {e}", exc_info=True)
        return []


def _word_range(subset):
    """Rango contiguo de %MW que cubre todos los mw_word de un subset de tags."""
    if not subset:
        return 0, 0
    words = [t["mw_word"] for t in subset]
    base  = min(words)
    count = max(words) - base + 1
    return base, count


# ---------------------------------------------------------------------------
# BASE DE DATOS
# ---------------------------------------------------------------------------

def init_db(conn):
    log_db.info("Inicializando base de datos SQLite (events.db)...")
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events(
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                tag         TEXT,
                address     TEXT,
                signal_type TEXT,
                state       TEXT,
                description TEXT,
                timestamp   TEXT
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_id        ON events(id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_tag       ON events(tag)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_events(
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type  TEXT,
                description TEXT,
                timestamp   TEXT
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sysev_timestamp ON system_events(timestamp)")
        # Overrides editables desde la UI — pisan los campos del xlsx por address.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tag_overrides(
                address     TEXT PRIMARY KEY,
                symbol      TEXT,
                description TEXT,
                signal_type TEXT,
                updated_at  TEXT
            )
        """)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM events")
        total = cursor.fetchone()[0]
        log_db.info(f"DB lista — eventos existentes: {total:,} / {MAX_EVENTS:,}")
    except Exception as e:
        log_db.critical(f"Error al inicializar la DB: {e}", exc_info=True)
        raise


def enforce_fifo(conn):
    log_db.debug("Verificando límite FIFO...")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM events")
        total = cursor.fetchone()[0]
        if total > MAX_EVENTS:
            to_delete = total - MAX_EVENTS
            cursor.execute("""
                DELETE FROM events WHERE id IN (
                    SELECT id FROM events ORDER BY id ASC LIMIT ?
                )
            """, (to_delete,))
            conn.commit()
            log_db.info(f"FIFO: eliminados {to_delete:,} eventos antiguos (total era {total:,})")
        else:
            log_db.debug(f"FIFO OK — {total:,}/{MAX_EVENTS:,} eventos")
    except Exception as e:
        log_db.error(f"Error en enforce_fifo: {e}", exc_info=True)


def save_system_event(conn, event_type, description):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        conn.execute(
            "INSERT INTO system_events(event_type, description, timestamp) VALUES (?, ?, ?)",
            (event_type, description, timestamp)
        )
        conn.commit()
        log.info(f"[SISTEMA] {event_type} — {description}")
    except Exception as e:
        log_db.error(f"Error al guardar evento de sistema: {e}", exc_info=True)


def save_event(conn, tag, address, signal_type, state, description):
    global event_counter
    try:
        cursor = conn.cursor()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        cursor.execute(
            "INSERT INTO events(tag, address, signal_type, state, description, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (tag, address, signal_type, state, description, timestamp)
        )
        event_counter += 1
        log_db.debug(f"Evento #{event_counter} guardado: [{signal_type}] {tag} = {state}")
        if event_counter % 1000 == 0:
            log_db.info(f"Checkpoint: {event_counter:,} eventos guardados en esta sesión")
            enforce_fifo(conn)
    except Exception as e:
        log_db.error(f"Error al guardar evento ({tag}, {state}): {e}", exc_info=True)


# ---------------------------------------------------------------------------
# CLIENTE MODBUS
# ---------------------------------------------------------------------------

def build_client():
    log_modbus.debug(
        f"Creando ModbusTcpClient — "
        f"host={PLC_IP} port={PLC_PORT} "
        f"timeout={MODBUS_TIMEOUT}s retries={MODBUS_RETRIES}"
    )
    return ModbusTcpClient(
        host=PLC_IP,
        port=PLC_PORT,
        timeout=MODBUS_TIMEOUT,
        retries=MODBUS_RETRIES,
    )


# ---------------------------------------------------------------------------
# LECTURAS MODBUS — FC03 únicamente (M221 no expone %I/%Q vía Modbus)
# ---------------------------------------------------------------------------

def _check_result(result, label):
    if result.isError():
        raise Exception(f"Error Modbus en {label}: {result}")


def read_register_block(client, addr, count, label):
    """Lee `count` Holding Registers a partir de `addr` (FC03)."""
    log_modbus.debug(f"FC03 {label}: addr={addr} count={count}")
    result = client.read_holding_registers(address=addr, count=count, device_id=DEVICE_ID)
    _check_result(result, f"{label} addr={addr} count={count}")
    return list(result.registers[:count])


def decode_bits(registers, base_word, tags_subset):
    """Para cada tag, extrae el bit correspondiente del registro espejado."""
    return [
        (registers[t["mw_word"] - base_word] >> t["mw_bit"]) & 1
        for t in tags_subset
    ]


# ---------------------------------------------------------------------------
# LOOP PRINCIPAL
# ---------------------------------------------------------------------------

def _compute_tag_state(tags):
    """Calcula todas las estructuras derivadas de la lista de tags.

    Devuelve un dict con: input/output tags, bases y counts de %MW espejo,
    arrays planos para el hot loop y vectores de estado anterior reseteados.
    """
    input_tags  = [t for t in tags if t["type"] == "INPUT"]
    output_tags = [t for t in tags if t["type"] == "OUTPUT"]
    in_base,  in_count  = _word_range(input_tags)
    out_base, out_count = _word_range(output_tags)
    io_tags = input_tags + output_tags
    total   = len(io_tags)
    return {
        "input_tags":       input_tags,
        "output_tags":      output_tags,
        "in_base":          in_base,
        "in_count":         in_count,
        "out_base":         out_base,
        "out_count":        out_count,
        "total":            total,
        "tag_names":        [t["tag"]         for t in io_tags],
        "tag_addresses":    [t["address"]     for t in io_tags],
        "tag_descs":        [t["description"] for t in io_tags],
        "signal_types":     [t["type"]        for t in io_tags],
        "previous_values":  [None] * total,
        "last_change_time": [0.0]  * total,
    }


def start_logger():
    global connection_status

    db_conn = sqlite3.connect("events.db")
    init_db(db_conn)
    enforce_fifo(db_conn)

    tags = load_tags_safe(db_conn)
    if not tags:
        log.critical("Sin tags no se puede iniciar el logger. Abortando.")
        return

    st = _compute_tag_state(tags)

    log.info("=" * 60)
    log.info("LOGGER INICIADO")
    log.info(f"  PLC:              {PLC_IP}:{PLC_PORT}")
    log.info(f"  Device ID:        {DEVICE_ID}")
    log.info(f"  Inputs:           {len(st['input_tags'])}  (espejo en %MW{st['in_base']}..%MW{st['in_base'] + st['in_count'] - 1})")
    log.info(f"  Outputs:          {len(st['output_tags'])}  (espejo en %MW{st['out_base']}..%MW{st['out_base'] + st['out_count'] - 1})")
    log.info(f"  Total signals:    {st['total']}")
    log.info(f"  Poll interval:    {POLL_INTERVAL}s")
    log.info(f"  Debounce:         {MIN_DELTA}s")
    log.info(f"  Max events (DB):  {MAX_EVENTS:,}")
    log.info(f"  Timeout Modbus:   {MODBUS_TIMEOUT}s")
    log.info(f"  Retries Modbus:   {MODBUS_RETRIES}")
    log.info(f"  Backoff base:     {BACKOFF_BASE}s")
    log.info(f"  Backoff max:      {BACKOFF_MAX}s")
    log.info(f"  Log file:         {LOG_FILE}")
    log.info("=" * 60)

    client          = build_client()
    reconnect_delay = BACKOFF_BASE

    # En el primer ciclo solo sembramos previous_values, sin generar eventos:
    # de lo contrario, cada arranque del logger guardaría ~270 "cambios"
    # espurios (None → estado inicial) por cada signal. Mismo flag al recargar.
    first_cycle = True

    log.info("Entrando al loop de polling...")
    save_system_event(db_conn, "INICIO", f"Sistema iniciado — PLC {PLC_IP}:{PLC_PORT}")

    was_connected = False

    try:
        while True:
            try:
                # ── Recarga en vivo de tags (xlsx u overrides) ────────
                if reload_event.is_set():
                    reload_event.clear()
                    new_tags = load_tags_safe(db_conn)
                    if new_tags:
                        st = _compute_tag_state(new_tags)
                        first_cycle = True
                        save_system_event(
                            db_conn, "RELOAD_TAGS",
                            f"Tags recargados: {st['total']} signals "
                            f"({len(st['input_tags'])} IN, {len(st['output_tags'])} OUT)"
                        )
                        log.info(f"Tags recargados en vivo: {st['total']} signals")
                    else:
                        log.error("Recarga de tags falló — se mantiene el set anterior.")

                # ── Conexión ──────────────────────────────────────────
                if not client.is_socket_open():
                    log.info(f"Socket cerrado — conectando a {PLC_IP}:{PLC_PORT}...")
                    if not client.connect():
                        raise ConnectionError(
                            f"client.connect() devolvió False para {PLC_IP}:{PLC_PORT}"
                        )
                    log.info(
                        f"Conexión establecida con {PLC_IP}:{PLC_PORT} "
                        f"(tras {connection_status['retries']} reintento(s))"
                    )
                    if not was_connected:
                        save_system_event(db_conn, "CONEXION", f"Conexión establecida con {PLC_IP}:{PLC_PORT}")
                    elif connection_status["retries"] > 0:
                        save_system_event(db_conn, "RECONEXION", f"Reconexión exitosa con {PLC_IP}:{PLC_PORT} tras {connection_status['retries']} reintento(s)")
                    was_connected = True
                    reconnect_delay = BACKOFF_BASE

                # ── Lecturas (todas FC03) ─────────────────────────────
                t_start = time.perf_counter()
                in_regs  = read_register_block(client, st["in_base"],  st["in_count"],  "INPUT mirror")
                out_regs = read_register_block(client, st["out_base"], st["out_count"], "OUTPUT mirror")
                t_ms     = (time.perf_counter() - t_start) * 1000

                input_values  = decode_bits(in_regs,  st["in_base"],  st["input_tags"])
                output_values = decode_bits(out_regs, st["out_base"], st["output_tags"])
                values        = input_values + output_values

                connection_status["connected"]      = True
                connection_status["last_connected"] = datetime.now().strftime("%d/%m/%y %H:%M:%S")
                connection_status["last_error"]     = None
                connection_status["retries"]        = 0
                connection_status["last_cycle_ms"]  = round(t_ms, 1)
                LATENCY_HISTORY.append(round(t_ms, 1))

                log_modbus.debug(f"Ciclo de lectura completo en {t_ms:.1f} ms")

                # ── Detección de cambios I/O ──────────────────────────
                now     = time.time()
                changes = 0
                total_signals = st["total"]

                if first_cycle:
                    for i in range(total_signals):
                        st["previous_values"][i]  = bool(values[i])
                        st["last_change_time"][i] = now
                    first_cycle = False
                    log.info(f"Estado inicial sembrado ({total_signals} signals) — los próximos ciclos detectan cambios")
                else:
                    for i in range(total_signals):
                        current = bool(values[i])

                        if current != st["previous_values"][i] and (now - st["last_change_time"][i]) > MIN_DELTA:
                            st["last_change_time"][i] = now
                            st["previous_values"][i]  = current

                            state = "ON" if current else "OFF"

                            save_event(db_conn, st["tag_names"][i], st["tag_addresses"][i], st["signal_types"][i], state, st["tag_descs"][i])
                            log_events.info(
                                f"[{st['signal_types'][i]}] {st['tag_names'][i]} | {st['tag_addresses'][i]} | {state} | {st['tag_descs'][i]}"
                            )
                            changes += 1

                    if changes:
                        db_conn.commit()
                        log.debug(f"{changes} cambio(s) detectado(s) en este ciclo")

            except Exception as e:
                connection_status["connected"]  = False
                connection_status["last_error"] = str(e)
                connection_status["retries"]   += 1
                is_conn_error = isinstance(e, ConnectionError)
                if connection_status["retries"] == 1:
                    save_system_event(db_conn, "DESCONEXION", f"Conexión perdida con {PLC_IP}:{PLC_PORT} — {e}")
                    was_connected = False
                log.error(
                    f"{'Conexión' if is_conn_error else 'Lectura'} error "
                    f"(reintento #{connection_status['retries']}): {e} "
                    f"— esperando {reconnect_delay:.1f}s",
                    exc_info=not is_conn_error
                )
                try:
                    client.close()
                except Exception:
                    pass
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, BACKOFF_MAX)
                client = build_client()
                continue

            time.sleep(POLL_INTERVAL)

    finally:
        log.info("Cerrando conexiones...")
        try:
            client.close()
        except Exception:
            pass
        try:
            db_conn.close()
        except Exception:
            pass
        log.info("Logger detenido.")


if __name__ == "__main__":
    start_logger()
