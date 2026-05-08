import sqlite3
import time
import logging
import logging.handlers
import sys
import os
from datetime import datetime
from pymodbus.client import ModbusTcpClient
from tag_loader import load_tags

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

PLC_IP         = "192.168.200.10"
PLC_PORT       = 502
MAX_EVENTS     = 1_000_000
# Schneider M221: por default el server Modbus responde con ID 255.
# Si en EcoStruxure Machine Expert Basic se habilita "Modbus Mapping",
# se puede definir un ID custom (típicamente 1). Confirmar con el PLC físico.
DEVICE_ID      = 1
MIN_DELTA      = 0.1       # Debounce en segundos
POLL_INTERVAL  = 0.5       # Tiempo entre lecturas (segundos)

# Holding Registers analógicos/numéricos — %MW, dos bloques separados.
# Estos NO contienen el espejo de I/O — son registros propios del programa.
HR_BLOCK1_ADDR  = 0    # %MW0  → %MW50
HR_BLOCK1_COUNT = 51
HR_BLOCK2_ADDR  = 100  # %MW100 → %MW114
HR_BLOCK2_COUNT = 20

HR_COUNT = HR_BLOCK1_COUNT + HR_BLOCK2_COUNT   # 71

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
    "retries":        0
}

event_counter = 0


# ---------------------------------------------------------------------------
# TAGS
# ---------------------------------------------------------------------------

def load_tags_safe():
    """
    Carga los tags del xlsx. Sin tags no hay mapeo I/O y el logger no arranca.
    """
    log.info("Cargando tags desde tag_loader...")
    try:
        tags = load_tags()
        if not tags:
            log.critical("tag_loader devolvió 0 tags — revisar xlsx y formato.")
            return []
        n_in  = sum(1 for t in tags if t["type"] == "INPUT")
        n_out = sum(1 for t in tags if t["type"] == "OUTPUT")
        log.info(f"Tags cargados: {len(tags)} ({n_in} INPUT, {n_out} OUTPUT)")
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

def start_logger():
    global connection_status

    tags = load_tags_safe()
    if not tags:
        log.critical("Sin tags no se puede iniciar el logger. Abortando.")
        return

    input_tags  = [t for t in tags if t["type"] == "INPUT"]
    output_tags = [t for t in tags if t["type"] == "OUTPUT"]

    in_base,  in_count  = _word_range(input_tags)
    out_base, out_count = _word_range(output_tags)

    TOTAL_INPUTS  = len(input_tags)
    TOTAL_OUTPUTS = len(output_tags)
    TOTAL_SIGNALS = TOTAL_INPUTS + TOTAL_OUTPUTS

    log.info("=" * 60)
    log.info("LOGGER INICIADO")
    log.info(f"  PLC:              {PLC_IP}:{PLC_PORT}")
    log.info(f"  Device ID:        {DEVICE_ID}")
    log.info(f"  Inputs:           {TOTAL_INPUTS}  (espejo en %MW{in_base}..%MW{in_base + in_count - 1})")
    log.info(f"  Outputs:          {TOTAL_OUTPUTS}  (espejo en %MW{out_base}..%MW{out_base + out_count - 1})")
    log.info(f"  HR analógicos:    {HR_COUNT}  (%MW{HR_BLOCK1_ADDR}..%MW{HR_BLOCK1_ADDR + HR_BLOCK1_COUNT - 1}, "
             f"%MW{HR_BLOCK2_ADDR}..%MW{HR_BLOCK2_ADDR + HR_BLOCK2_COUNT - 1})")
    log.info(f"  Total signals:    {TOTAL_SIGNALS}")
    log.info(f"  Poll interval:    {POLL_INTERVAL}s")
    log.info(f"  Debounce:         {MIN_DELTA}s")
    log.info(f"  Max events (DB):  {MAX_EVENTS:,}")
    log.info(f"  Timeout Modbus:   {MODBUS_TIMEOUT}s")
    log.info(f"  Retries Modbus:   {MODBUS_RETRIES}")
    log.info(f"  Backoff base:     {BACKOFF_BASE}s")
    log.info(f"  Backoff max:      {BACKOFF_MAX}s")
    log.info(f"  Log file:         {LOG_FILE}")
    log.info("=" * 60)

    db_conn = sqlite3.connect("events.db")
    init_db(db_conn)
    enforce_fifo(db_conn)

    client          = build_client()
    reconnect_delay = BACKOFF_BASE

    # Pre-computar arrays para evitar dict lookups en el hot loop.
    io_tags          = input_tags + output_tags
    tag_names_io     = [t["tag"]         for t in io_tags]
    tag_addresses_io = [t["address"]     for t in io_tags]
    tag_descs_io     = [t["description"] for t in io_tags]
    signal_types_io  = [t["type"]        for t in io_tags]

    # Tags genéricos para los HR analógicos (no vienen del xlsx).
    hr_addresses = (
        [f"%MW{HR_BLOCK1_ADDR + i}" for i in range(HR_BLOCK1_COUNT)] +
        [f"%MW{HR_BLOCK2_ADDR + i}" for i in range(HR_BLOCK2_COUNT)]
    )
    hr_names = [f"REG_{a[1:]}"  for a in hr_addresses]
    hr_descs = [f"Register {a}" for a in hr_addresses]

    # Vectores de estado anterior
    previous_values    = [None] * TOTAL_SIGNALS
    last_change_time   = [0.0]  * TOTAL_SIGNALS
    previous_registers = [None] * HR_COUNT
    last_register_time = [0.0]  * HR_COUNT

    log.info("Entrando al loop de polling...")
    save_system_event(db_conn, "INICIO", f"Sistema iniciado — PLC {PLC_IP}:{PLC_PORT}")

    was_connected = False

    try:
        while True:
            try:
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
                in_regs  = read_register_block(client, in_base,        in_count,        "INPUT mirror")
                out_regs = read_register_block(client, out_base,       out_count,       "OUTPUT mirror")
                hr1      = read_register_block(client, HR_BLOCK1_ADDR, HR_BLOCK1_COUNT, "HR block1")
                hr2      = read_register_block(client, HR_BLOCK2_ADDR, HR_BLOCK2_COUNT, "HR block2")
                t_ms     = (time.perf_counter() - t_start) * 1000

                input_values  = decode_bits(in_regs,  in_base,  input_tags)
                output_values = decode_bits(out_regs, out_base, output_tags)
                values        = input_values + output_values
                registers     = hr1 + hr2

                connection_status["connected"]      = True
                connection_status["last_connected"] = datetime.now().strftime("%d/%m/%y %H:%M:%S")
                connection_status["last_error"]     = None
                connection_status["retries"]        = 0

                log_modbus.debug(f"Ciclo de lectura completo en {t_ms:.1f} ms")

                # ── Detección de cambios I/O ──────────────────────────
                now     = time.time()
                changes = 0

                for i in range(TOTAL_SIGNALS):
                    current = bool(values[i])

                    if current != previous_values[i] and (now - last_change_time[i]) > MIN_DELTA:
                        last_change_time[i] = now
                        previous_values[i]  = current

                        state = "ON" if current else "OFF"

                        save_event(db_conn, tag_names_io[i], tag_addresses_io[i], signal_types_io[i], state, tag_descs_io[i])
                        log_events.info(
                            f"[{signal_types_io[i]}] {tag_names_io[i]} | {tag_addresses_io[i]} | {state} | {tag_descs_io[i]}"
                        )
                        changes += 1

                # ── Holding Registers analógicos ──────────────────────
                for k, current in enumerate(registers):
                    if current != previous_registers[k] and (now - last_register_time[k]) > MIN_DELTA:
                        last_register_time[k]   = now
                        previous_registers[k]   = current

                        save_event(db_conn, hr_names[k], hr_addresses[k], "REGISTER", str(current), hr_descs[k])
                        log_events.info(
                            f"[REGISTER] {hr_names[k]} | {hr_addresses[k]} | {current} | {hr_descs[k]}"
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
