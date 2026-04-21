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
DEVICE_ID      = 1         # Cambiar a 255 para Schneider M340/M580
MIN_DELTA      = 0.1       # Debounce en segundos
POLL_INTERVAL  = 0.5       # Tiempo entre lecturas (segundos)

# Entradas — un bloque continuo desde DI_ADDR
DI_ADDR        = 0         # %I0.0
TOTAL_INPUTS   = 56        # %I0.0 → %I2.15 (3 palabras × 16 bits)

# Salidas — dos bloques separados en el mapa Modbus
COIL_BLOCK1_ADDR  = 0      # %Q0.0 → %Q0.15
COIL_BLOCK1_COUNT = 16
COIL_BLOCK2_ADDR  = 48     # %Q3.0 → %Q4.15
COIL_BLOCK2_COUNT = 32

TOTAL_OUTPUTS  = COIL_BLOCK1_COUNT + COIL_BLOCK2_COUNT   # 48
TOTAL_SIGNALS  = TOTAL_INPUTS + TOTAL_OUTPUTS

# Holding Registers (FC03) — %MW, dos bloques separados
HR_BLOCK1_ADDR  = 0    # %MW0  → %MW50
HR_BLOCK1_COUNT = 51
HR_BLOCK2_ADDR  = 100  # %MW100 → %MW114
HR_BLOCK2_COUNT = 20

HR_COUNT = HR_BLOCK1_COUNT + HR_BLOCK2_COUNT   # 66

# Parámetros del cliente Modbus (según docs oficiales)
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
    log.info("Cargando tags desde tag_loader...")
    try:
        tags = load_tags()
        log.info(f"Tags cargados: {len(tags)} entradas")
        if len(tags) < TOTAL_SIGNALS:
            log.warning(
                f"Solo hay {len(tags)} tags pero se esperan {TOTAL_SIGNALS}. "
                "Las señales sin tag usarán nombres genéricos."
            )
        elif len(tags) > TOTAL_SIGNALS:
            log.warning(
                f"Hay {len(tags)} tags pero solo se van a usar {TOTAL_SIGNALS}. "
                "El resto se ignorará."
            )
        return tags
    except Exception as e:
        log.critical(f"No se pudieron cargar los tags: {e}", exc_info=True)
        log.warning("Se continuará con tags genéricos (TAG_0, TAG_1, ...)")
        return []


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
    """
    Crea un ModbusTcpClient con los parámetros recomendados.
    El cliente sincrónico NO reconecta automáticamente.
    El backoff exponencial se maneja manualmente en el loop principal.
    """
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
# LECTURAS MODBUS
# ---------------------------------------------------------------------------

def _check_result(result, label):
    """Verifica un resultado Modbus usando isError() (pymodbus >= 3.x)."""
    if result.isError():
        raise Exception(f"Error Modbus en {label}: {result}")


def read_discrete_inputs(client):
    """
    Lee entradas digitales físicas — tabla 1x (%I).
    Función Modbus 0x02 (Read Discrete Inputs).
    """
    log_modbus.debug(f"Leyendo {TOTAL_INPUTS} Discrete Inputs desde addr {DI_ADDR}...")
    result = client.read_discrete_inputs(
        address=DI_ADDR, count=TOTAL_INPUTS, device_id=DEVICE_ID
    )
    _check_result(result, f"DI addr={DI_ADDR} count={TOTAL_INPUTS}")
    values = list(result.bits[:TOTAL_INPUTS])
    log_modbus.debug(f"Total Discrete Inputs leídos: {len(values)}")
    return values


def read_coils(client):
    """
    Lee salidas digitales / coils — tabla 0x (%Q).
    Función Modbus 0x01 (Read Coils). Dos bloques de direcciones separados.
    """
    log_modbus.debug(f"Leyendo Coils: bloque1 addr={COIL_BLOCK1_ADDR} count={COIL_BLOCK1_COUNT}, "
                     f"bloque2 addr={COIL_BLOCK2_ADDR} count={COIL_BLOCK2_COUNT}...")
    result1 = client.read_coils(address=COIL_BLOCK1_ADDR, count=COIL_BLOCK1_COUNT, device_id=DEVICE_ID)
    _check_result(result1, f"Coil bloque1 addr={COIL_BLOCK1_ADDR} count={COIL_BLOCK1_COUNT}")

    result2 = client.read_coils(address=COIL_BLOCK2_ADDR, count=COIL_BLOCK2_COUNT, device_id=DEVICE_ID)
    _check_result(result2, f"Coil bloque2 addr={COIL_BLOCK2_ADDR} count={COIL_BLOCK2_COUNT}")

    values = list(result1.bits[:COIL_BLOCK1_COUNT]) + list(result2.bits[:COIL_BLOCK2_COUNT])
    log_modbus.debug(f"Total Coils leídos: {len(values)}")
    return values


def read_holding_registers(client):
    """
    Lee registros de memoria — tabla 4x (%MW).
    Función Modbus 0x03 (Read Holding Registers). Dos bloques separados.
    """
    log_modbus.debug(f"Leyendo HRs: bloque1 addr={HR_BLOCK1_ADDR} count={HR_BLOCK1_COUNT}, "
                     f"bloque2 addr={HR_BLOCK2_ADDR} count={HR_BLOCK2_COUNT}...")
    result1 = client.read_holding_registers(address=HR_BLOCK1_ADDR, count=1, device_id=DEVICE_ID)
    _check_result(result1, f"HR bloque1 addr={HR_BLOCK1_ADDR} count={HR_BLOCK1_COUNT}")

    result2 = client.read_holding_registers(address=HR_BLOCK2_ADDR, count=HR_BLOCK2_COUNT, device_id=DEVICE_ID)
    _check_result(result2, f"HR bloque2 addr={HR_BLOCK2_ADDR} count={HR_BLOCK2_COUNT}")

    values = list(result1.registers[:HR_BLOCK1_COUNT]) + list(result2.registers[:HR_BLOCK2_COUNT])
    log_modbus.debug(f"Total Holding Registers leídos: {len(values)}")
    return values


# ---------------------------------------------------------------------------
# LOOP PRINCIPAL
# ---------------------------------------------------------------------------

def start_logger():
    global connection_status

    log.info("=" * 60)
    log.info("LOGGER INICIADO")
    log.info(f"  PLC:              {PLC_IP}:{PLC_PORT}")
    log.info(f"  Device ID:        {DEVICE_ID}")
    log.info(f"  Inputs  (DI):     {TOTAL_INPUTS} desde addr {DI_ADDR}")
    log.info(f"  Outputs (Coils):  {TOTAL_OUTPUTS}")
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

    tags    = load_tags_safe()
    db_conn = sqlite3.connect("events.db")
    init_db(db_conn)
    enforce_fifo(db_conn)

    client          = build_client()
    reconnect_delay = BACKOFF_BASE

    # Pre-computar arrays de tags para evitar dict lookups en el hot loop
    total_all = TOTAL_SIGNALS + HR_COUNT

    def _fallback_address(i):
        """Genera dirección %Ix.y / %Qx.y / %MWn cuando el Excel no tiene tag para el índice i."""
        if i < TOTAL_INPUTS:
            return f"%I{i // 16}.{i % 16}"
        if i < TOTAL_SIGNALS:
            j = i - TOTAL_INPUTS
            if j < COIL_BLOCK1_COUNT:
                return f"%Q0.{j}"
            k = j - COIL_BLOCK1_COUNT
            return f"%Q{3 + k // 16}.{k % 16}"
        k = i - TOTAL_SIGNALS
        if k < HR_BLOCK1_COUNT:
            return f"%MW{HR_BLOCK1_ADDR + k}"
        return f"%MW{HR_BLOCK2_ADDR + (k - HR_BLOCK1_COUNT)}"

    tag_names        = [tags[i]["tag"]         if i < len(tags) else f"TAG_{i}"       for i in range(total_all)]
    tag_descriptions = [tags[i]["description"] if i < len(tags) else f"Signal {i}"    for i in range(total_all)]
    signal_types     = (
        ["INPUT"    if i < TOTAL_INPUTS  else "OUTPUT" for i in range(TOTAL_SIGNALS)] +
        ["REGISTER"] * HR_COUNT
    )
    tag_addresses    = [
        ("%Q" + (tags[i]["address"] if i < len(tags) else _fallback_address(i))[2:])
        if signal_types[i] == "OUTPUT" and (tags[i]["address"] if i < len(tags) else "").startswith("%I")
        else (tags[i]["address"] if i < len(tags) else _fallback_address(i))
        for i in range(total_all)
    ]

    # Vectores de estado anterior — booleanos para I/O, enteros para registros
    previous_values    = [False] * TOTAL_SIGNALS
    last_change_time   = [0.0]   * TOTAL_SIGNALS
    previous_registers = [None]  * HR_COUNT
    last_register_time = [0.0]   * HR_COUNT

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

                # ── Lecturas ──────────────────────────────────────────
                t_start    = time.perf_counter()
                inputs     = read_discrete_inputs(client)
                outputs    = read_coils(client)
                registers  = read_holding_registers(client)
                t_ms       = (time.perf_counter() - t_start) * 1000

                values = inputs + outputs

                connection_status["connected"]      = True
                connection_status["last_connected"] = datetime.now().strftime("%d/%m/%y %H:%M:%S")
                connection_status["last_error"]     = None
                connection_status["retries"]        = 0

                log_modbus.debug(f"Ciclo de lectura completo en {t_ms:.1f} ms")

                # ── Detección de cambios ──────────────────────────────
                now     = time.time()
                changes = 0

                for i in range(TOTAL_SIGNALS):
                    current = bool(values[i])

                    if current != previous_values[i] and (now - last_change_time[i]) > MIN_DELTA:
                        last_change_time[i] = now
                        previous_values[i]  = current

                        state = "ON" if current else "OFF"

                        save_event(db_conn, tag_names[i], tag_addresses[i], signal_types[i], state, tag_descriptions[i])
                        log_events.info(
                            f"[{signal_types[i]}] {tag_names[i]} | {tag_addresses[i]} | {state} | {tag_descriptions[i]}"
                        )
                        changes += 1

                # ── Holding Registers ─────────────────────────────────
                for k, current in enumerate(registers):
                    if current != previous_registers[k] and (now - last_register_time[k]) > MIN_DELTA:
                        last_register_time[k]   = now
                        previous_registers[k]   = current
                        idx = TOTAL_SIGNALS + k

                        save_event(db_conn, tag_names[idx], tag_addresses[idx], "REGISTER", str(current), tag_descriptions[idx])
                        log_events.info(
                            f"[REGISTER] {tag_names[idx]} | {tag_addresses[idx]} | {current} | {tag_descriptions[idx]}"
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
