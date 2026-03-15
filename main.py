from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import sqlite3
import pandas as pd
import threading

from modbus_logger import start_logger
from tag_loader import load_tags

app = FastAPI()
templates = Jinja2Templates(directory="templates")
tags = load_tags()

threading.Thread(target=start_logger, daemon=True).start()

@app.get("/")
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/events")
def get_events():
    conn = sqlite3.connect("events.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM events ORDER BY id DESC LIMIT 100")
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
            "tag": t["tag"],
            "address": t["address"],
            "description": t["description"],
            "state": row["state"] if row else "OFF"
        })
    conn.close()
    return signals

@app.get("/export")
def export_events():
    conn = sqlite3.connect("events.db")
    df = pd.read_sql_query("SELECT * FROM events", conn)
    conn.close()
    file = "events_export.xlsx"
    df.to_excel(file, index=False)
    return FileResponse(file, filename="events.xlsx")