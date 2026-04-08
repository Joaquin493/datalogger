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
TOTAL_INPUTS   = 30        # Discrete Inputs (%I) — read_discrete_inputs
TOTAL_OUTPUTS  = 15        # Coils (%Q)           — read_coils
TOTAL_SIGNALS  = TOTAL_INPUTS + TOTAL_OUTPUTS
MAX_EVENTS     = 1_000_000
DEVICE_ID      = 1         # Cambiar a 255 para Schneider M340/M580
START_ADDRESS  = 0         # Cambiar a 1 si el PLC lo requiere
MIN_DELTA      = 0.1       # Debounce en segundos
POLL_INTERVAL  = 0.5       # Tiempo entre lecturas (segundos)

# Parámetros del cliente Modbus (según docs oficiales)
MODBUS_TIMEOUT       = 3     # segundos — socket timeout
MODBUS_RETRIES       = 3     # reintentos automáticos por request fallido
RECONNECT_DELAY_BASE = 1.0   # backoff inicial en segundos
RECONNECT_DELAY_MAX  = 30.0  # backoff máximo en segundos

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

def init_db():
    log_db.info("Inicializando base de datos SQLite (events.db)...")
    try:
        conn = sqlite3.connect("events.db")
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
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM events")
        total = cursor.fetchone()[0]
        conn.close()
        log_db.info(f"DB lista — eventos existentes: {total:,} / {MAX_EVENTS:,}")
    except Exception as e:
        log_db.critical(f"Error al inicializar la DB: {e}", exc_info=True)
        raise


def enforce_fifo():
    log_db.debug("Verificando límite FIFO...")
    try:
        conn = sqlite3.connect("events.db")
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
        conn.close()
    except Exception as e:
        log_db.error(f"Error en enforce_fifo: {e}", exc_info=True)


def save_event(tag, address, signal_type, state, description):
    global event_counter
    try:
        conn = sqlite3.connect("events.db")
        cursor = conn.cursor()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        cursor.execute(
            "INSERT INTO events(tag, address, signal_type, state, description, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (tag, address, signal_type, state, description, timestamp)
        )
        conn.commit()
        conn.close()
        event_counter += 1
        log_db.debug(f"Evento #{event_counter} guardado: [{signal_type}] {tag} = {state}")
        if event_counter % 1000 == 0:
            log_db.info(f"Checkpoint: {event_counter:,} eventos guardados en esta sesión")
            enforce_fifo()
    except Exception as e:
        log_db.error(f"Error al guardar evento ({tag}, {state}): {e}", exc_info=True)


# ---------------------------------------------------------------------------
# CLIENTE MODBUS
# ---------------------------------------------------------------------------

def build_client():
    """
    Crea un ModbusTcpClient con los parámetros recomendados por la doc oficial.
    IMPORTANTE: el cliente sincrónico NO reconecta automáticamente.
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
    if hasattr(result, 'isError') and result.isError():
        raise Exception(f"Error Modbus en {label}: {result}")
    if not hasattr(result, 'bits'):
        raise Exception(f"Respuesta inválida en {label}: {result}")


def read_discrete_inputs(client):
    """
    Lee entradas digitales físicas — tabla 1x (%I).
    Función Modbus 0x02 (Read Discrete Inputs).
    """
    values = []
    log_modbus.debug(f"Leyendo {TOTAL_INPUTS} Discrete Inputs desde addr {START_ADDRESS}...")
    for address in range(START_ADDRESS, TOTAL_INPUTS + START_ADDRESS, 8):
        count  = min(8, TOTAL_INPUTS - (address - START_ADDRESS))
        result = client.read_discrete_inputs(
            address=address, count=count, device_id=DEVICE_ID
        )
        _check_result(result, f"DI addr={address}")
        values.extend(result.bits[:count])
        log_modbus.debug(f"  DI addr={address} count={count} bits={result.bits[:count]}")
    log_modbus.debug(f"Total Discrete Inputs leídos: {len(values)}")
    return values


def read_coils(client):
    """
    Lee salidas digitales / coils — tabla 0x (%Q).
    Función Modbus 0x01 (Read Coils).
    """
    values = []
    log_modbus.debug(f"Leyendo {TOTAL_OUTPUTS} Coils desde addr {START_ADDRESS}...")
    for address in range(START_ADDRESS, TOTAL_OUTPUTS + START_ADDRESS, 8):
        count  = min(8, TOTAL_OUTPUTS - (address - START_ADDRESS))
        result = client.read_coils(
            address=address, count=count, device_id=DEVICE_ID
        )
        _check_result(result, f"Coil addr={address}")
        values.extend(result.bits[:count])
        log_modbus.debug(f"  Coil addr={address} count={count} bits={result.bits[:count]}")
    log_modbus.debug(f"Total Coils leídos: {len(values)}")
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
    log.info(f"  Start Address:    {START_ADDRESS}")
    log.info(f"  Inputs  (DI):     {TOTAL_INPUTS}")
    log.info(f"  Outputs (Coils):  {TOTAL_OUTPUTS}")
    log.info(f"  Total signals:    {TOTAL_SIGNALS}")
    log.info(f"  Poll interval:    {POLL_INTERVAL}s")
    log.info(f"  Debounce:         {MIN_DELTA}s")
    log.info(f"  Max events (DB):  {MAX_EVENTS:,}")
    log.info(f"  Timeout Modbus:   {MODBUS_TIMEOUT}s")
    log.info(f"  Retries Modbus:   {MODBUS_RETRIES}")
    log.info(f"  Backoff base:     {RECONNECT_DELAY_BASE}s")
    log.info(f"  Backoff max:      {RECONNECT_DELAY_MAX}s")
    log.info(f"  Log file:         {LOG_FILE}")
    log.info("=" * 60)

    tags   = load_tags_safe()
    init_db()

    client          = build_client()
    reconnect_delay = RECONNECT_DELAY_BASE

    # Vector de estado anterior y timestamps de debounce
    # Índices 0..TOTAL_INPUTS-1             → Discrete Inputs (%I)
    # Índices TOTAL_INPUTS..TOTAL_SIGNALS-1 → Coils (%Q)
    previous_values  = [False] * TOTAL_SIGNALS
    last_change_time = [0.0]   * TOTAL_SIGNALS

    log.info("Entrando al loop de polling...")

    while True:
        try:
            # ── Conexión ──────────────────────────────────────────────────
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
                reconnect_delay = RECONNECT_DELAY_BASE  # resetear backoff

            # ── Lecturas ──────────────────────────────────────────────────
            t_start = time.perf_counter()
            inputs  = read_discrete_inputs(client)   # TOTAL_INPUTS valores
            outputs = read_coils(client)              # TOTAL_OUTPUTS valores
            t_ms    = (time.perf_counter() - t_start) * 1000

            values = inputs + outputs  # vector unificado de TOTAL_SIGNALS bool

            # Actualizar estado global
            connection_status["connected"]      = True
            connection_status["last_connected"] = datetime.now().strftime("%d/%m/%y %H:%M:%S")
            connection_status["last_error"]     = None
            connection_status["retries"]        = 0

            log_modbus.debug(f"Ciclo de lectura completo en {t_ms:.1f} ms")

            # ── Detección de cambios ──────────────────────────────────────
            now     = time.time()
            changes = 0

            for i in range(TOTAL_SIGNALS):
                current = bool(values[i])

                if current != previous_values[i] and (now - last_change_time[i]) > MIN_DELTA:
                    last_change_time[i] = now

                    tag         = tags[i]["tag"]         if i < len(tags) else f"TAG_{i}"
                    description = tags[i]["description"] if i < len(tags) else f"Signal {i}"
                    address     = tags[i]["address"]     if i < len(tags) else f"addr_{i}"
                    signal_type = "INPUT" if i < TOTAL_INPUTS else "OUTPUT"
                    if signal_type == "OUTPUT" and address.startswith("%I"):
                        address = "%Q" + address[2:]
                    state       = "ON" if current else "OFF"

                    save_event(tag, address, signal_type, state, description)
                    log_events.info(
                        f"[{signal_type}] {tag} | {address} | {state} | {description}"
                    )
                    changes += 1

                previous_values[i] = current

            if changes:
                log.debug(f"{changes} cambio(s) detectado(s) en este ciclo")

        except ConnectionError as ce:
            # Error de conexión — backoff exponencial
            connection_status["connected"]  = False
            connection_status["last_error"] = str(ce)
            connection_status["retries"]   += 1
            log.error(
                f"Error de conexión (reintento #{connection_status['retries']}): {ce} "
                f"— esperando {reconnect_delay:.1f}s antes de reintentar"
            )
            try:
                client.close()
            except Exception:
                pass
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_DELAY_MAX)
            client = build_client()
            continue

        except Exception as e:
            # Error de lectura Modbus (no necesariamente de conexión)
            connection_status["connected"]  = False
            connection_status["last_error"] = str(e)
            connection_status["retries"]   += 1
            log.error(
                f"Error de lectura (reintento #{connection_status['retries']}): {e} "
                f"— esperando {reconnect_delay:.1f}s",
                exc_info=True
            )
            try:
                client.close()
            except Exception:
                pass
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_DELAY_MAX)
            client = build_client()
            continue

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    start_logger()