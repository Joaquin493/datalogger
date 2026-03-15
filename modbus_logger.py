import sqlite3
import time
from datetime import datetime
from pymodbus.client import ModbusTcpClient
from tag_loader import load_tags

PLC_IP = "127.0.0.1"
PLC_PORT = 502
TOTAL_SIGNALS = 200
MAX_EVENTS = 100000

tags = load_tags()

def init_db():
    conn = sqlite3.connect("events.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT,
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

def save_event(tag, state, description):
    conn = sqlite3.connect("events.db")
    cursor = conn.cursor()
    timestamp = datetime.now().strftime("%d:%m:%y / %H:%M:%S")
    cursor.execute(
        "INSERT INTO events(tag,state,description,timestamp) VALUES (?,?,?,?)",
        (tag, state, description, timestamp)
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
    print("LOGGER STARTED")
    init_db()

    client = ModbusTcpClient(PLC_IP, port=PLC_PORT)
    client.connect()
    previous_values = [False] * TOTAL_SIGNALS

    while True:
        try:
            values = read_all_coils(client)

            for i in range(TOTAL_SIGNALS):
                current = bool(values[i])
                if current != previous_values[i]:
                    tag = tags[i]["tag"] if i < len(tags) else f"TAG_{i}"
                    description = tags[i]["description"] if i < len(tags) else f"Digital Input {i}"
                    state = "ON" if current else "OFF"
                    save_event(tag, state, description)
                    print("EVENT:", tag, state)
                previous_values[i] = current

        except Exception as e:
            print("LOGGER ERROR:", e)
            client.close()
            time.sleep(2)
            client.connect()

        time.sleep(0.5)