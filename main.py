from fastapi import FastAPI, Request, Response, Depends
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import sqlite3
import pandas as pd
import threading
import logging
from datetime import datetime
import os
import secrets

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

if os.environ.get("RAILWAY_ENVIRONMENT"):
    from modbus_simulator import start_simulator
    threading.Thread(target=start_simulator, daemon=True).start()

# ── AUTH ──
SESSION_TOKEN = secrets.token_hex(32)
USERS = {"admin": "admin"}

def check_session(request: Request):
    return request.cookies.get("session") == SESSION_TOKEN

@app.get("/login")
def login_page(request: Request):
    if check_session(request):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse(request=request, name="login.html", context={"error": ""}
)

@app.post("/login")
async def login(request: Request):
    form = await request.form()
    username = form.get("username", "")
    password = form.get("password", "")
    if USERS.get(username) == password:
        response = RedirectResponse("/", status_code=302)
        response.set_cookie("session", SESSION_TOKEN, httponly=True)
        return response
    return templates.TemplateResponse("login.html", {"request": request, "error": "Usuario o contraseña incorrectos"})

@app.get("/logout")
def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie("session")
    return response

@app.get("/")
def home(request: Request):
    if not check_session(request):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/status")
def get_status(request: Request):
    if not check_session(request):
        return Response(status_code=401)
    return connection_status

@app.get("/events/count")
def get_event_counts(request: Request):
    if not check_session(request):
        return Response(status_code=401)
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
def get_events(request: Request, limit: int = 100, date_from: str = "", date_to: str = "", tag: str = "", search: str = ""):
    if not check_session(request):
        return Response(status_code=401)
    conn = sqlite3.connect("events.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query  = "SELECT * FROM events WHERE 1=1"
    params = []
    if tag:
        query += " AND tag=?"
        params.append(tag)
    if search:
        query += " AND (tag LIKE ? OR address LIKE ? OR description LIKE ? OR timestamp LIKE ?)"
        s = f"%{search}%"
        params.extend([s, s, s, s])
    if date_from:
        try:
            dt = datetime.strptime(date_from, "%Y-%m-%dT%H:%M")
            query += " AND timestamp >= ?"
            params.append(dt.strftime("%Y-%m-%d %H:%M:%S"))
        except:
            pass
    if date_to:
        try:
            dt = datetime.strptime(date_to, "%Y-%m-%dT%H:%M")
            query += " AND timestamp <= ?"
            params.append(dt.strftime("%Y-%m-%d %H:%M:%S"))
        except:
            pass
    has_filter = tag or search or date_from or date_to
    query += " ORDER BY id DESC LIMIT ?"
    params.append(1000000 if has_filter else min(limit, 1000))
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/signals")
def get_signals(request: Request):
    if not check_session(request):
        return Response(status_code=401)
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
def export_events(request: Request, tag: str = "", search: str = "", date_from: str = "", date_to: str = ""):
    if not check_session(request):
        return Response(status_code=401)
    conn = sqlite3.connect("events.db")
    query  = "SELECT id, tag, address, state, description, timestamp FROM events WHERE 1=1"
    params = []
    if tag:
        query += " AND tag=?"
        params.append(tag)
    if search:
        query += " AND (tag LIKE ? OR address LIKE ? OR description LIKE ? OR timestamp LIKE ?)"
        s = f"%{search}%"
        params.extend([s, s, s, s])
    if date_from:
        try:
            dt = datetime.strptime(date_from, "%Y-%m-%dT%H:%M")
            query += " AND timestamp >= ?"
            params.append(dt.strftime("%Y-%m-%d %H:%M:%S"))
        except:
            pass
    if date_to:
        try:
            dt = datetime.strptime(date_to, "%Y-%m-%dT%H:%M")
            query += " AND timestamp <= ?"
            params.append(dt.strftime("%Y-%m-%d %H:%M:%S"))
        except:
            pass
    query += " ORDER BY id DESC"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    file = "events_export.xlsx"
    df.to_excel(file, index=False)
    nombre = datetime.now().strftime("eventos_%d-%m-%Y_%H-%M-%S.xlsx")
    return FileResponse(file, filename=nombre)