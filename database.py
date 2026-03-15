import sqlite3
from datetime import datetime

conn = sqlite3.connect("events.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag TEXT,
    state TEXT,
    description TEXT,
    timestamp TEXT
)
""")

conn.commit()


def insert_event(tag, state, description):

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(
        "INSERT INTO events (tag, state, description, timestamp) VALUES (?, ?, ?, ?)",
        (tag, state, description, timestamp)
    )

    conn.commit()


def get_events():

    cursor.execute("SELECT * FROM events ORDER BY id DESC LIMIT 100")

    rows = cursor.fetchall()

    events = []

    for row in rows:

        events.append({
            "id": row[0],
            "tag": row[1],
            "state": row[2],
            "description": row[3],
            "timestamp": row[4]
        })

    return events