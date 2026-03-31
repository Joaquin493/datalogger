import sqlite3
import time
from datetime import datetime
from pymodbus.client import ModbusTcpClient
from tag_loader import load_tags
import logging

logging.getLogger("pymodbus").setLevel(logging.WARNING)

PLC_IP = "127.0.0.1"
PLC_PORT = 502
TOTAL_SIGNALS = 200
MAX_EVENTS = 1000000

tags = load_tags()

# estado de conexión global accesible desde main.py
connection_status = {
    "connected": False,
    "last_connected": None,
    "last_error": None,
    "retries": 0
}

def init_db():
    conn = sqlite3.connect("events.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT,
            address TEXT,
            state TEXT,
            description TEXT,
            timestamp TEXT
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

def save_event(tag, address, state, description):
    conn = sqlite3.connect("events.db")
    cursor = conn.cursor()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    cursor.execute(
        "INSERT INTO events(tag,address,state,description,timestamp) VALUES (?,?,?,?,?)",
        (tag, address, state, description, timestamp)
    )
    conn.commit()
    conn.close()
    enforce_fifo()

def read_all_coils(client):
    values = []
    for address in range(0, TOTAL_SIGNALS, 8):
        count = min(8, TOTAL_SIGNALS - address)
        result = client.read_coils(address=address, count=count, device_id=1)
        if not hasattr(result, 'bits'):
            raise Exception(f"Error en address {address}: {result}")
        values.extend(result.bits[:count])
    return values

def start_logger():
    global connection_status
    print("LOGGER STARTED")
    init_db()

    client = ModbusTcpClient(PLC_IP, port=PLC_PORT)
    previous_values = [False] * TOTAL_SIGNALS
    retry_delay = 2  # segundos, crece con backoff

    while True:
        try:
            if not client.is_socket_open():
                print(f"Conectando a {PLC_IP}:{PLC_PORT}...")
                connected = client.connect()
                if not connected:
                    raise Exception("No se pudo conectar")
                print("Conexión establecida")
                retry_delay = 2  # reset backoff al conectar

            values = read_all_coils(client)

            # conexión exitosa
            connection_status["connected"]      = True
            connection_status["last_connected"] = datetime.now().strftime("%d:%m:%y / %H:%M:%S")
            connection_status["last_error"]     = None
            connection_status["retries"]        = 0
            retry_delay = 2

            for i in range(TOTAL_SIGNALS):
                current = bool(values[i])
                if current != previous_values[i]:
                    tag         = tags[i]["tag"]         if i < len(tags) else f"TAG_{i}"
                    description = tags[i]["description"] if i < len(tags) else f"Digital Input {i}"
                    address     = tags[i]["address"]     if i < len(tags) else f"%I{i//16}.{i%16}"
                    state       = "ON" if current else "OFF"
                    save_event(tag, address, state, description)
                    print("EVENT:", tag, address, state)
                previous_values[i] = current

        except Exception as e:
            connection_status["connected"]  = False
            connection_status["last_error"] = str(e)
            connection_status["retries"]   += 1
            print(f"LOGGER ERROR (reintento {connection_status['retries']} en {retry_delay}s): {e}")
            try:
                client.close()
            except:
                pass
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)  # backoff: 2s, 4s, 8s... máx 60s
            client = ModbusTcpClient(PLC_IP, port=PLC_PORT)
            continue

        time.sleep(0.5)