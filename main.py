from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from fastapi.staticfiles import StaticFiles
import sqlite3
import pandas as pd
import threading
import logging
from datetime import datetime

from modbus_logger import start_logger, connection_status
from tag_loader import load_tags

logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
logging.getLogger("pymodbus").setLevel(logging.WARNING)

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
tags = load_tags()

threading.Thread(target=start_logger, daemon=True).start()

import os
if os.environ.get("RAILWAY_ENVIRONMENT"):
    from modbus_simulator import start_simulator
    threading.Thread(target=start_simulator, daemon=True).start()

@app.get("/")
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/status")
def get_status():
    return connection_status

@app.get("/events/count")
def get_event_counts():
    conn = sqlite3.connect("events.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT tag, address, description,
               COUNT(*) as total,
               SUM(CASE WHEN state='ON'  THEN 1 ELSE 0 END) as total_on,
               SUM(CASE WHEN state='OFF' THEN 1 ELSE 0 END) as total_off,
               MAX(timestamp) as last_event
        FROM events
        GROUP BY tag
        ORDER BY total DESC
    """)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/events")
def get_events(limit: int = 100, date_from: str = "", date_to: str = "", tag: str = ""):
    conn = sqlite3.connect("events.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query  = "SELECT * FROM events WHERE 1=1"
    params = []
    if tag:
        query += " AND tag=?"
        params.append(tag)
    if date_from:
        query += " AND timestamp >= ?"
        params.append(date_from)
    if date_to:
        query += " AND timestamp <= ?"
        params.append(date_to)
    query += " ORDER BY id DESC LIMIT ?"
    params.append(1000000 if tag else min(limit, 1000))
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/signals")
def get_signals():
    conn = sqlite3.connect("events.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    signals = []
    for t in tags:
        cursor.execute(
            "SELECT state FROM events WHERE tag=? ORDER BY id DESC LIMIT 1",
            (t["tag"],)
        )
        row = cursor.fetchone()
        signals.append({
            "tag":         t["tag"],
            "address":     t["address"],
            "description": t["description"],
            "state":       row["state"] if row else "OFF"
        })
    conn.close()
    return signals

@app.get("/export")
def export_events():
    conn = sqlite3.connect("events.db")
    df = pd.read_sql_query("SELECT id, tag, address, state, description, timestamp FROM events", conn)
    conn.close()
    file = "events_export.xlsx"
    df.to_excel(file, index=False)
    nombre = datetime.now().strftime("eventos_%d-%m-%Y_%H-%M-%S.xlsx")
    return FileResponse(file, filename=nombre)