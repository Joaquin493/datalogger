import sqlite3
import time
from datetime import datetime
from pymodbus.client import ModbusTcpClient
from tag_loader import load_tags
import logging

logging.getLogger("pymodbus").setLevel(logging.WARNING)

PLC_IP         = "127.0.0.1"
PLC_PORT       = 5020
TOTAL_INPUTS   = 30   # Discrete Inputs (%I) — read_discrete_inputs
TOTAL_OUTPUTS  = 15   # Coils (%Q)           — read_coils
TOTAL_SIGNALS  = TOTAL_INPUTS + TOTAL_OUTPUTS
MAX_EVENTS     = 1000000
DEVICE_ID      = 1    # cambiar a 255 para Schneider M340/M580
START_ADDRESS  = 0    # cambiar a 1 si el PLC lo requiere
MIN_DELTA      = 0.1  # debounce en segundos

tags = load_tags()

connection_status = {
    "connected":      False,
    "last_connected": None,
    "last_error":     None,
    "retries":        0
}

event_counter = 0


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def init_db():
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
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_id ON events(id)")
    conn.commit()
    conn.close()

def enforce_fifo():
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
        print("FIFO CLEAN:", to_delete, "old events removed")
    conn.commit()
    conn.close()

def save_event(tag, address, signal_type, state, description):
    global event_counter
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
    if event_counter % 1000 == 0:
        enforce_fifo()


# ---------------------------------------------------------------------------
# Modbus readers
# ---------------------------------------------------------------------------

def read_discrete_inputs(client):
    """Lee entradas digitales físicas (tabla 1x — %I) con read_discrete_inputs."""
    values = []
    for address in range(START_ADDRESS, TOTAL_INPUTS + START_ADDRESS, 8):
        count = min(8, TOTAL_INPUTS - (address - START_ADDRESS))
        result = client.read_discrete_inputs(
            address=address, count=count, device_id=DEVICE_ID
        )
        if not hasattr(result, 'bits'):
            raise Exception(f"Error discrete input en address {address}: {result}")
        values.extend(result.bits[:count])
    return values  # lista de TOTAL_INPUTS booleanos

def read_coils(client):
    """Lee salidas digitales / coils (tabla 0x — %Q) con read_coils."""
    values = []
    for address in range(START_ADDRESS, TOTAL_OUTPUTS + START_ADDRESS, 8):
        count = min(8, TOTAL_OUTPUTS - (address - START_ADDRESS))
        result = client.read_coils(
            address=address, count=count, device_id=DEVICE_ID
        )
        if not hasattr(result, 'bits'):
            raise Exception(f"Error coil en address {address}: {result}")
        values.extend(result.bits[:count])
    return values  # lista de TOTAL_OUTPUTS booleanos


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def start_logger():
    global connection_status
    print("LOGGER STARTED")
    print(f"  Inputs  (Discrete Inputs): {TOTAL_INPUTS}")
    print(f"  Outputs (Coils):           {TOTAL_OUTPUTS}")
    print(f"  Total signals:             {TOTAL_SIGNALS}")
    init_db()

    client = ModbusTcpClient(PLC_IP, port=PLC_PORT)

    # Estado anterior e historial de debounce para TODAS las señales
    # Índices 0..TOTAL_INPUTS-1          → inputs
    # Índices TOTAL_INPUTS..TOTAL_SIGNALS-1 → outputs
    previous_values  = [False] * TOTAL_SIGNALS
    last_change_time = [0.0]   * TOTAL_SIGNALS

    while True:
        try:
            if not client.is_socket_open():
                print(f"Conectando a {PLC_IP}:{PLC_PORT}...")
                if not client.connect():
                    raise Exception("No se pudo conectar")
                print("Conexión establecida")

            # Lecturas separadas por tipo
            inputs  = read_discrete_inputs(client)   # 30 valores
            outputs = read_coils(client)              # 15 valores

            # Vector unificado: inputs primero, luego outputs
            values = inputs + outputs

            connection_status["connected"]      = True
            connection_status["last_connected"] = datetime.now().strftime("%d:%m:%y / %H:%M:%S")
            connection_status["last_error"]     = None
            connection_status["retries"]        = 0

            now = time.time()

            for i in range(TOTAL_SIGNALS):
                current = bool(values[i])

                if current != previous_values[i] and (now - last_change_time[i]) > MIN_DELTA:
                    last_change_time[i] = now

                    # Metadatos del tag
                    tag         = tags[i]["tag"]         if i < len(tags) else f"TAG_{i}"
                    description = tags[i]["description"] if i < len(tags) else f"Signal {i}"
                    address     = tags[i]["address"]     if i < len(tags) else f"addr_{i}"

                    # Tipo según posición en el vector
                    signal_type = "INPUT" if i < TOTAL_INPUTS else "OUTPUT"

                    state = "ON" if current else "OFF"
                    save_event(tag, address, signal_type, state, description)
                    print(f"EVENT [{signal_type}]: {tag} | {address} | {state}")

                previous_values[i] = current

        except Exception as e:
            connection_status["connected"]  = False
            connection_status["last_error"] = str(e)
            connection_status["retries"]   += 1
            print(f"LOGGER ERROR (reintento {connection_status['retries']}): {e}")
            try:
                client.close()
            except:
                pass
            time.sleep(1)
            client = ModbusTcpClient(PLC_IP, port=PLC_PORT)
            continue

        time.sleep(0.5)


if __name__ == "__main__":
    start_logger()