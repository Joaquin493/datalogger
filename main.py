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

from modbus_logger import start_logger, connection_status, load_tags_safe, save_system_event

logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
logging.getLogger("pymodbus").setLevel(logging.WARNING)

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
tags = load_tags_safe()

threading.Thread(target=start_logger, daemon=True).start()

if os.environ.get("RAILWAY_ENVIRONMENT"):
    from modbus_simulator import start_simulator
    threading.Thread(target=start_simulator, daemon=True).start()

# ── AUTH ──
SESSION_TOKEN = secrets.token_hex(32)
USERS = {
    os.environ.get("APP_USER", "admin"): os.environ.get("APP_PASSWORD", "admin")
}

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
        response.set_cookie(
            "session", SESSION_TOKEN,
            httponly=True,
            samesite="lax",
        )
        return response
    return templates.TemplateResponse(request=request, name="login.html", context={"error": "Usuario o contraseña incorrectos"})

@app.get("/logout")
def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie("session")
    return response

@app.get("/")
def home(request: Request):
    if not check_session(request):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse(request=request, name="index.html", context={})

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
    query += " ORDER BY id DESC LIMIT ?"
    MAX_QUERY_LIMIT = 10_000
    params.append(min(limit, MAX_QUERY_LIMIT))
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
    cursor.execute("""
        SELECT tag, state FROM events
        WHERE id IN (SELECT MAX(id) FROM events GROUP BY tag)
    """)
    latest = {row["tag"]: row["state"] for row in cursor.fetchall()}
    conn.close()
    return [
        {
            "tag":         t["tag"],
            "address":     t["address"],
            "description": t["description"],
            "state":       latest.get(t["tag"], "OFF")
        }
        for t in tags
        if not t["address"].startswith("%Q")
    ]

@app.get("/system-events")
def get_system_events(request: Request, limit: int = 200):
    if not check_session(request):
        return Response(status_code=401)
    conn = sqlite3.connect("events.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, event_type, description, timestamp FROM system_events ORDER BY id DESC LIMIT ?",
        (min(limit, 10000),)
    )
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows

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