"""FastAPI app del datalogger.

Sirve la SPA estática (index.html / login.html) y expone los endpoints `/api/*`
que consume el frontend portado del proyecto v2. Los endpoints adaptan la
forma de los datos al schema que espera el JS de v2:

  - /api/status     -> {link: {connected, last_error, last_cycle_ms}, events_total, max_events}
  - /api/variables  -> [{symbol, address, type, state, description}]    state ∈ {0,1,null}
  - /api/events     -> {items, total}                                    items con state ∈ {0,1,int}
  - /api/stats      -> [{symbol, address, description, total, total_on, total_off, last_event}]
  - /api/sysevents  -> [{type, description, ts}]
  - /api/export.xlsx/.csv

Los timestamps en la DB se guardan como "YYYY-MM-DD HH:MM:SS.fff" (hora local,
sin TZ). El frontend envía filtros como ISO UTC; convertimos a local antes de
comparar contra la DB.
"""

import csv
import io
import logging
import os
import re
import secrets
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.responses import FileResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from modbus_logger import (
    LATENCY_HISTORY,
    MAX_EVENTS,
    PROCESS_START,
    connection_status,
    fetch_overrides,
    load_tags_safe,
    reload_event,
    start_logger,
)
from tag_loader import ACTIVE_XLSX, BACKUPS_DIR, validate_xlsx, load_tags

logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
logging.getLogger("pymodbus").setLevel(logging.WARNING)

app = FastAPI(title="Datalogger V2", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Tags cargados al arrancar y refrescables vía /api/tags/reload. Bajo lock
# porque /api/variables y los endpoints de admin pueden mutar / leer en paralelo.
_tags_lock = threading.Lock()
_tags_cache: list = []


def _refresh_tags_cache():
    global _tags_cache
    conn = _db_connect_unrowed()
    try:
        new_tags = load_tags_safe(conn)
    finally:
        conn.close()
    with _tags_lock:
        _tags_cache = new_tags
    return new_tags


def _db_connect_unrowed() -> sqlite3.Connection:
    # connection sin row_factory — para load_tags_safe que usa índices numéricos
    return sqlite3.connect("events.db")


threading.Thread(target=start_logger, daemon=True).start()

# Pequeña pausa para que el thread del logger corra init_db antes de que el
# refresh del cache intente leer tag_overrides. fetch_overrides es robusto a
# tabla inexistente, así que esto es defensivo, no requerido.
_refresh_tags_cache()

if os.environ.get("RAILWAY_ENVIRONMENT"):
    from modbus_simulator import start_simulator
    threading.Thread(target=start_simulator, daemon=True).start()

# ---------------------------------------------------------------------------
# AUTH
# ---------------------------------------------------------------------------

SESSION_TOKEN = secrets.token_hex(32)
USERS = {
    os.environ.get("APP_USER", "admin"): os.environ.get("APP_PASSWORD", "admin"),
}

# Password adicional para la pestaña Configuración (tag overrides + auto-update).
# Es una segunda capa sobre el login normal — limita qué operadores pueden
# tocar la configuración del sistema y el código.
CONFIG_PASSWORD = os.environ.get("CONFIG_PASSWORD", "pro986")
CONFIG_TOKEN = secrets.token_hex(32)


def check_session(request: Request) -> bool:
    return request.cookies.get("session") == SESSION_TOKEN


def require_session(request: Request):
    if not check_session(request):
        raise HTTPException(status_code=401, detail="No autorizado")


def check_config_session(request: Request) -> bool:
    return request.cookies.get("config_session") == CONFIG_TOKEN


def require_config_session(request: Request):
    """Doble gate: requiere login normal + auth de config."""
    if not check_session(request):
        raise HTTPException(status_code=401, detail="No autorizado")
    if not check_config_session(request):
        raise HTTPException(status_code=403, detail="Requiere contraseña de configuración")


# ---------------------------------------------------------------------------
# UTILIDADES — conversión de timestamps y estados
# ---------------------------------------------------------------------------

# Whitelist de columnas para sort_by — evita inyección SQL.
_SORT_COLUMNS = {"id", "address", "tag", "state", "timestamp"}


def _iso_to_db_ts(iso: str) -> Optional[str]:
    """Convierte un ISO ('2026-05-07T13:30:00.000Z' o local) al formato local de la DB.

    La DB guarda 'YYYY-MM-DD HH:MM:SS.fff' en hora local. El frontend manda ISO
    UTC tras el `new Date(...).toISOString()`. Hacemos parse + conversión a local.
    """
    if not iso:
        return None
    try:
        s = iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone()  # a TZ local del server
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def _db_ts_to_iso(ts: Optional[str]) -> Optional[str]:
    """'2026-05-07 13:30:00.123' -> '2026-05-07T13:30:00.123' (ISO sin TZ).

    El JS de v2 hace `new Date(iso)` y reformatea — basta con la T en el medio.
    """
    if not ts:
        return None
    return ts.replace(" ", "T", 1)


def _state_to_int(s: Optional[str]) -> Optional[int]:
    """Convierte el state TEXT de la DB a int. ON->1, OFF->0, num->int, otro->None."""
    if s is None:
        return None
    if s == "ON":
        return 1
    if s == "OFF":
        return 0
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def _state_param_to_db(val: Optional[str]) -> Optional[str]:
    """Normaliza el query `state` del front a 'ON'/'OFF' para filtrar en DB."""
    if val is None or val == "":
        return None
    v = val.strip().lower()
    if v in ("1", "on", "true"):
        return "ON"
    if v in ("0", "off", "false"):
        return "OFF"
    raise HTTPException(400, f"Valor de state inválido: {val!r}")


def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect("events.db")
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# RUTAS HTML
# ---------------------------------------------------------------------------

@app.get("/login")
def login_page(request: Request, error: str = ""):
    if check_session(request):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse(
        request=request, name="login.html", context={"error": error}
    )


@app.post("/login")
async def login(request: Request):
    form = await request.form()
    username = form.get("username", "")
    password = form.get("password", "")
    if USERS.get(username) == password:
        response = RedirectResponse("/", status_code=302)
        response.set_cookie("session", SESSION_TOKEN, httponly=True, samesite="lax")
        return response
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"error": "Usuario o contraseña incorrectos"},
        status_code=401,
    )


@app.get("/logout")
def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie("session")
    response.delete_cookie("config_session")
    return response


@app.get("/api/config/status", dependencies=[Depends(require_session)])
def api_config_status(request: Request):
    """Indica si el usuario actual ya pasó el gate de config."""
    return {"authenticated": check_config_session(request)}


@app.post("/api/config/auth", dependencies=[Depends(require_session)])
async def api_config_auth(request: Request):
    body = await request.json()
    password = (body.get("password") or "").strip()
    if password != CONFIG_PASSWORD:
        raise HTTPException(401, "Contraseña incorrecta")
    response = Response(content='{"ok":true}', media_type="application/json")
    response.set_cookie("config_session", CONFIG_TOKEN, httponly=True, samesite="lax")
    return response


@app.post("/api/config/logout", dependencies=[Depends(require_session)])
def api_config_logout():
    """Cierra solo la sesión de config, manteniendo el login principal."""
    response = Response(content='{"ok":true}', media_type="application/json")
    response.delete_cookie("config_session")
    return response


@app.get("/")
def home(request: Request):
    if not check_session(request):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse(request=request, name="index.html", context={})


@app.get("/healthz")
def healthz():
    """Liveness sin auth — apto para systemd/monitores externos."""
    return {"status": "ok" if connection_status["connected"] else "degraded",
            "modbus_connected": connection_status["connected"]}


# ---------------------------------------------------------------------------
# API — schema compatible con el frontend v2
# ---------------------------------------------------------------------------

@app.get("/api/status", dependencies=[Depends(require_session)])
def api_status():
    conn = _db_connect()
    try:
        total = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        # Eventos de hoy desde 00:00 local — usa el índice de timestamp.
        today_floor = datetime.now().strftime("%Y-%m-%d 00:00:00.000")
        events_today = conn.execute(
            "SELECT COUNT(*) FROM events WHERE timestamp >= ?",
            (today_floor,),
        ).fetchone()[0]
        # MAX(timestamp) — O(1) con índice. Es el ts del último cambio detectado.
        last_event_ts = conn.execute("SELECT MAX(timestamp) FROM events").fetchone()[0]
    finally:
        conn.close()

    return {
        "link": {
            "connected":      connection_status["connected"],
            "last_connected": connection_status["last_connected"],
            "last_error":     connection_status["last_error"],
            "last_cycle_ms":  connection_status["last_cycle_ms"],
        },
        "events_total":    total,
        "max_events":      MAX_EVENTS,
        "events_today":    events_today,
        "last_event_ts":   _db_ts_to_iso(last_event_ts),
        "uptime_seconds":  int(time.time() - PROCESS_START),
        "latency_history": list(LATENCY_HISTORY),
    }


@app.get("/api/variables", dependencies=[Depends(require_session)])
def api_variables():
    """Snapshot del último estado de cada tag (desde la última fila por tag en events)."""
    conn = _db_connect()
    try:
        cur = conn.execute("""
            SELECT tag, state FROM events
            WHERE id IN (SELECT MAX(id) FROM events GROUP BY tag)
        """)
        latest = {row["tag"]: row["state"] for row in cur.fetchall()}
    finally:
        conn.close()

    with _tags_lock:
        tags_snapshot = list(_tags_cache)

    out = []
    for t in tags_snapshot:
        st = _state_to_int(latest.get(t["tag"]))
        out.append({
            "symbol":      t["tag"],
            "address":     t["address"],
            "type":        t.get("type", "INPUT"),
            "state":       st,
            "description": t["description"],
        })
    return out


def _build_events_query(
    *, address, symbol, description, state, ts_from, ts_to, search,
    sort_by, order, limit, offset, count_only=False,
):
    """Arma SELECT/COUNT con los filtros aplicados. Devuelve (sql, params)."""
    if sort_by not in _SORT_COLUMNS:
        raise HTTPException(400, f"sort_by inválido: {sort_by!r}")
    direction = "ASC" if str(order).lower() == "asc" else "DESC"

    where = []
    params: list = []

    if address:
        where.append("address LIKE ?"); params.append(f"%{address}%")
    if symbol:
        where.append("tag = ?"); params.append(symbol)
    if description:
        where.append("description LIKE ?"); params.append(f"%{description}%")
    if state is not None:
        where.append("state = ?"); params.append(state)
    if ts_from:
        db_from = _iso_to_db_ts(ts_from)
        if db_from:
            where.append("timestamp >= ?"); params.append(db_from)
    if ts_to:
        db_to = _iso_to_db_ts(ts_to)
        if db_to:
            where.append("timestamp <= ?"); params.append(db_to + ".999")
    if search:
        where.append("(tag LIKE ? OR address LIKE ? OR description LIKE ? OR timestamp LIKE ?)")
        s = f"%{search}%"
        params.extend([s, s, s, s])

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    if count_only:
        return f"SELECT COUNT(*) FROM events{where_sql}", params

    sql = (
        "SELECT id, tag, address, signal_type, state, description, timestamp "
        f"FROM events{where_sql} "
        f"ORDER BY {sort_by} {direction} "
        "LIMIT ? OFFSET ?"
    )
    params2 = list(params) + [limit, offset]
    return sql, params2


@app.get("/api/events", dependencies=[Depends(require_session)])
def api_events(
    address: Optional[str] = None,
    symbol:  Optional[str] = None,
    description: Optional[str] = None,
    state:   Optional[str] = None,
    ts_from: Optional[str] = None,
    ts_to:   Optional[str] = None,
    search:  Optional[str] = None,
    sort_by: str = Query("id"),
    order:   str = Query("desc"),
    limit:   int = Query(50, ge=1, le=5000),
    offset:  int = Query(0, ge=0),
):
    db_state = _state_param_to_db(state)
    common = dict(
        address=address, symbol=symbol, description=description, state=db_state,
        ts_from=ts_from, ts_to=ts_to, search=search,
        sort_by=sort_by, order=order, limit=limit, offset=offset,
    )

    conn = _db_connect()
    try:
        count_sql, count_params = _build_events_query(**common, count_only=True)
        total = conn.execute(count_sql, count_params).fetchone()[0]
        sql, params = _build_events_query(**common)
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    items = [{
        "id":          r["id"],
        "address":     r["address"],
        "symbol":      r["tag"],
        "state":       _state_to_int(r["state"]),
        "description": r["description"],
        "ts":          _db_ts_to_iso(r["timestamp"]),
    } for r in rows]
    return {"items": items, "total": total}


@app.get("/api/stats", dependencies=[Depends(require_session)])
def api_stats(
    ts_from: Optional[str] = None,
    ts_to:   Optional[str] = None,
):
    where = []
    params: list = []
    if ts_from:
        db_from = _iso_to_db_ts(ts_from)
        if db_from:
            where.append("timestamp >= ?"); params.append(db_from)
    if ts_to:
        db_to = _iso_to_db_ts(ts_to)
        if db_to:
            where.append("timestamp <= ?"); params.append(db_to + ".999")
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    conn = _db_connect()
    try:
        sql = f"""
            SELECT tag, address, description,
                   COUNT(*) as total,
                   SUM(CASE WHEN state='ON'  THEN 1 ELSE 0 END) as total_on,
                   SUM(CASE WHEN state='OFF' THEN 1 ELSE 0 END) as total_off,
                   MAX(timestamp) as last_event
            FROM events{where_sql}
            GROUP BY tag
            ORDER BY total DESC
        """
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    return [{
        "symbol":      r["tag"],
        "address":     r["address"],
        "description": r["description"],
        "total":       r["total"],
        "total_on":    r["total_on"],
        "total_off":   r["total_off"],
        "last_event":  _db_ts_to_iso(r["last_event"]),
    } for r in rows]


@app.get("/api/sysevents", dependencies=[Depends(require_session)])
def api_sysevents(limit: int = Query(500, ge=1, le=5000)):
    conn = _db_connect()
    try:
        rows = conn.execute(
            "SELECT id, event_type, description, timestamp "
            "FROM system_events ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return [{
        "type":        r["event_type"],
        "description": r["description"],
        "ts":          _db_ts_to_iso(r["timestamp"]),
    } for r in rows]


# ---------------------------------------------------------------------------
# EXPORT
# ---------------------------------------------------------------------------

def _export_filename(prefix: str, ext: str) -> str:
    return datetime.now().strftime(f"{prefix}_%Y-%m-%d_%H-%M-%S.{ext}")


@app.get("/api/export.xlsx", dependencies=[Depends(require_session)])
def api_export_xlsx(
    address: Optional[str] = None,
    symbol:  Optional[str] = None,
    description: Optional[str] = None,
    state:   Optional[str] = None,
    ts_from: Optional[str] = None,
    ts_to:   Optional[str] = None,
    search:  Optional[str] = None,
    sort_by: str = Query("id"),
    order:   str = Query("desc"),
    limit:   int = Query(50_000, ge=1, le=100_000),
):
    db_state = _state_param_to_db(state)
    sql, params = _build_events_query(
        address=address, symbol=symbol, description=description, state=db_state,
        ts_from=ts_from, ts_to=ts_to, search=search,
        sort_by=sort_by, order=order, limit=limit, offset=0,
    )
    conn = _db_connect()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()
    df = df.rename(columns={"tag": "symbol", "timestamp": "ts"})
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return Response(
        buf.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{_export_filename("eventos", "xlsx")}"'},
    )


@app.get("/api/export.csv", dependencies=[Depends(require_session)])
def api_export_csv(
    address: Optional[str] = None,
    symbol:  Optional[str] = None,
    description: Optional[str] = None,
    state:   Optional[str] = None,
    ts_from: Optional[str] = None,
    ts_to:   Optional[str] = None,
    search:  Optional[str] = None,
    sort_by: str = Query("id"),
    order:   str = Query("desc"),
    limit:   int = Query(1_000_000, ge=1, le=1_000_000),
):
    """CSV en streaming — apto para el FIFO completo (1M filas) sin OOM."""
    db_state = _state_param_to_db(state)
    sql, params = _build_events_query(
        address=address, symbol=symbol, description=description, state=db_state,
        ts_from=ts_from, ts_to=ts_to, search=search,
        sort_by=sort_by, order=order, limit=limit, offset=0,
    )

    def _gen():
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(["id", "ts", "address", "symbol", "description", "state"])
        yield buf.getvalue()
        buf.seek(0); buf.truncate()

        conn = _db_connect()
        try:
            cur = conn.execute(sql, params)
            n = 0
            for r in cur:
                w.writerow([
                    r["id"], _db_ts_to_iso(r["timestamp"]) or "",
                    r["address"], r["tag"], r["description"] or "",
                    r["state"],
                ])
                n += 1
                if n % 1000 == 0:
                    yield buf.getvalue()
                    buf.seek(0); buf.truncate()
            if buf.tell():
                yield buf.getvalue()
        finally:
            conn.close()

    return StreamingResponse(
        _gen(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{_export_filename("eventos", "csv")}"'},
    )


# ---------------------------------------------------------------------------
# TAGS — administración desde la pestaña Sistema
# ---------------------------------------------------------------------------

def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _backup_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def _backup_active_xlsx(reason: str) -> Optional[str]:
    """Copia el xlsx activo a `xlsx_backups/` con timestamp. Devuelve el nombre."""
    if not os.path.exists(ACTIVE_XLSX):
        return None
    os.makedirs(BACKUPS_DIR, exist_ok=True)
    name = f"{reason}_{_backup_stamp()}.xlsx"
    dest = os.path.join(BACKUPS_DIR, name)
    shutil.copy2(ACTIVE_XLSX, dest)
    return name


def _list_backups() -> list[dict]:
    if not os.path.isdir(BACKUPS_DIR):
        return []
    items = []
    for entry in sorted(os.scandir(BACKUPS_DIR), key=lambda e: e.stat().st_mtime, reverse=True):
        if not entry.name.lower().endswith(".xlsx"):
            continue
        try:
            n_valid = validate_xlsx(entry.path)
        except Exception:
            n_valid = None
        st = entry.stat()
        items.append({
            "name":     entry.name,
            "size":     st.st_size,
            "mtime":    datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%dT%H:%M:%S"),
            "tags":     n_valid,
            "valid":    n_valid is not None,
        })
    return items


@app.get("/api/tags", dependencies=[Depends(require_config_session)])
def api_tags():
    """Tags efectivos = xlsx activo con overrides aplicados.

    Devuelve además los campos base del xlsx para que la UI pueda comparar
    y mostrar el valor original cuando hay un override.
    """
    # Recargamos desde xlsx + DB para que la UI siempre vea lo más fresco.
    try:
        base_tags = load_tags()
    except FileNotFoundError:
        raise HTTPException(404, f"No existe el xlsx activo ({ACTIVE_XLSX}).")
    except Exception as e:
        raise HTTPException(500, f"Error leyendo xlsx: {e}")

    conn = _db_connect_unrowed()
    try:
        ovr = fetch_overrides(conn)
        try:
            row = conn.execute("SELECT MAX(updated_at) FROM tag_overrides").fetchone()
            last_override_at = row[0] if row else None
        except sqlite3.OperationalError:
            last_override_at = None
    finally:
        conn.close()

    out = []
    for t in base_tags:
        ov = ovr.get(t["address"])
        out.append({
            "address":          t["address"],
            "symbol":           t["tag"] if not ov else (ov.get("symbol") or t["tag"]),
            "description":      t["description"] if not ov else (ov.get("description") if ov.get("description") is not None else t["description"]),
            "type":             t["type"] if not ov else (ov.get("signal_type") or t["type"]),
            "base_symbol":      t["tag"],
            "base_description": t["description"],
            "base_type":        t["type"],
            "overridden":       bool(ov),
        })
    return {
        "active_xlsx":      ACTIVE_XLSX,
        "active_mtime":     datetime.fromtimestamp(os.path.getmtime(ACTIVE_XLSX)).strftime("%Y-%m-%dT%H:%M:%S") if os.path.exists(ACTIVE_XLSX) else None,
        "last_override_at": _db_ts_to_iso(last_override_at),
        "count":            len(out),
        "overrides":        sum(1 for x in out if x["overridden"]),
        "items":            out,
    }


@app.patch("/api/tags/{address:path}", dependencies=[Depends(require_config_session)])
async def api_tag_patch(address: str, request: Request):
    body = await request.json()
    symbol = body.get("symbol")
    desc   = body.get("description")
    typ    = body.get("type")
    if typ is not None and typ not in ("INPUT", "OUTPUT"):
        raise HTTPException(400, "type debe ser INPUT u OUTPUT")

    # Validar que el address exista en el xlsx (si no, el override sería huérfano).
    try:
        base_tags = load_tags()
    except Exception as e:
        raise HTTPException(500, f"Error leyendo xlsx: {e}")
    addrs = {t["address"] for t in base_tags}
    if address not in addrs:
        raise HTTPException(404, f"Address {address!r} no existe en el xlsx activo.")

    conn = _db_connect_unrowed()
    try:
        conn.execute("""
            INSERT INTO tag_overrides(address, symbol, description, signal_type, updated_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(address) DO UPDATE SET
                symbol      = COALESCE(excluded.symbol,      tag_overrides.symbol),
                description = COALESCE(excluded.description, tag_overrides.description),
                signal_type = COALESCE(excluded.signal_type, tag_overrides.signal_type),
                updated_at  = excluded.updated_at
        """, (address, symbol, desc, typ, _now_ts()))
        conn.commit()
    finally:
        conn.close()

    _refresh_tags_cache()
    reload_event.set()
    return {"ok": True, "address": address}


@app.delete("/api/tags/{address:path}/override", dependencies=[Depends(require_config_session)])
def api_tag_override_delete(address: str):
    conn = _db_connect_unrowed()
    try:
        cur = conn.execute("DELETE FROM tag_overrides WHERE address = ?", (address,))
        conn.commit()
        deleted = cur.rowcount
    finally:
        conn.close()

    _refresh_tags_cache()
    reload_event.set()
    return {"ok": True, "address": address, "deleted": deleted}


# --- Upload en dos fases: preview (diff) + confirm (swap real) ---

_PENDING_DIR = Path(BACKUPS_DIR) / "_pending"
_PENDING_RE = re.compile(r"^pending_[A-Za-z0-9]{12}\.xlsx$")


def _cleanup_pending(except_token: Optional[str] = None):
    """Borra todos los pending salvo (opcionalmente) uno. Mantenemos la
    carpeta liviana; los pending son efímeros y siempre se regeneran."""
    if not _PENDING_DIR.is_dir():
        return
    for p in _PENDING_DIR.iterdir():
        if not p.is_file():
            continue
        if except_token and p.name == f"pending_{except_token}.xlsx":
            continue
        try: p.unlink()
        except Exception: pass


def _pending_path(token: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9]{12}", token or ""):
        raise HTTPException(400, "Token de pending inválido.")
    return _PENDING_DIR / f"pending_{token}.xlsx"


def _tag_index(tags: list) -> dict:
    return {t["address"]: t for t in tags}


def _compute_diff(old_tags: list, new_tags: list, override_addrs: set) -> dict:
    """Compara dos listas de tags por address. Detecta también colisiones
    dentro de la nueva planilla (duplicate address, duplicate Flag HR).
    """
    errors: list[str] = []
    warnings: list[str] = []

    # Colisiones internas en la planilla nueva.
    seen_addr = {}
    seen_flag = {}
    for t in new_tags:
        a = t["address"]
        if a in seen_addr:
            errors.append(f"Address duplicado en la planilla: {a}")
        seen_addr[a] = t
        flag_key = (t["mw_word"], t["mw_bit"])
        if flag_key in seen_flag and seen_flag[flag_key] != a:
            errors.append(
                f"Flag HR duplicada: %M{t['mw_word']}.{t['mw_bit']} "
                f"usada por {seen_flag[flag_key]} y {a}"
            )
        seen_flag.setdefault(flag_key, a)

    old_idx = _tag_index(old_tags)
    new_idx = _tag_index(new_tags)

    added, removed, modified, unchanged = [], [], [], 0

    for addr, n in new_idx.items():
        if addr not in old_idx:
            added.append({
                "address": addr, "symbol": n["tag"],
                "description": n["description"], "type": n["type"],
            })
            continue
        o = old_idx[addr]
        fields = []
        if o["tag"]         != n["tag"]:         fields.append("symbol")
        if o["description"] != n["description"]: fields.append("description")
        if o["type"]        != n["type"]:        fields.append("type")
        if o["mw_word"] != n["mw_word"] or o["mw_bit"] != n["mw_bit"]:
            fields.append("flag_hr")
        if not fields:
            unchanged += 1
        else:
            modified.append({
                "address": addr,
                "fields":  fields,
                "old": {
                    "symbol": o["tag"], "description": o["description"],
                    "type": o["type"],
                    "flag_hr": f"%M{o['mw_word']}.{o['mw_bit']}",
                },
                "new": {
                    "symbol": n["tag"], "description": n["description"],
                    "type": n["type"],
                    "flag_hr": f"%M{n['mw_word']}.{n['mw_bit']}",
                },
            })

    for addr, o in old_idx.items():
        if addr not in new_idx:
            removed.append({
                "address": addr, "symbol": o["tag"],
                "description": o["description"], "type": o["type"],
            })

    # Overrides huérfanos: addresses con override que ya no están en la nueva.
    orphan_overrides = sorted(a for a in override_addrs if a not in new_idx)
    if orphan_overrides:
        warnings.append(
            f"{len(orphan_overrides)} override(s) quedan huérfanos "
            f"(sus addresses no están en la nueva planilla). "
            f"Se pueden borrar después desde la lista de tags."
        )

    # Aviso si la cantidad de signals cambia mucho (heurística simple).
    if old_tags and abs(len(new_tags) - len(old_tags)) > max(20, 0.2 * len(old_tags)):
        warnings.append(
            f"La cantidad de tags pasa de {len(old_tags)} a {len(new_tags)} "
            f"— revisá que sea esperado."
        )

    # Aviso si la nueva planilla no tiene INPUTs o no tiene OUTPUTs.
    n_in  = sum(1 for t in new_tags if t["type"] == "INPUT")
    n_out = sum(1 for t in new_tags if t["type"] == "OUTPUT")
    if n_in == 0:
        errors.append("La nueva planilla no tiene ningún tag INPUT.")
    if n_out == 0:
        errors.append("La nueva planilla no tiene ningún tag OUTPUT.")

    return {
        "summary": {
            "old_count":  len(old_tags),
            "new_count":  len(new_tags),
            "unchanged":  unchanged,
            "added":      len(added),
            "removed":    len(removed),
            "modified":   len(modified),
            "inputs":     n_in,
            "outputs":    n_out,
            "orphan_overrides": len(orphan_overrides),
        },
        "added":            added,
        "removed":          removed,
        "modified":         modified,
        "orphan_overrides": orphan_overrides,
        "warnings":         warnings,
        "errors":           errors,
    }


@app.post("/api/tags/preview", dependencies=[Depends(require_config_session)])
async def api_tags_preview(file: UploadFile = File(...)):
    """Valida y compara contra el xlsx activo SIN reemplazarlo.
    Devuelve un diff + un token para confirmar el reemplazo con
    /api/tags/upload/confirm.
    """
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(400, "Se esperaba un archivo .xlsx")

    _PENDING_DIR.mkdir(parents=True, exist_ok=True)
    _cleanup_pending()
    token = secrets.token_hex(6)        # 12 chars hex
    pending = _pending_path(token)
    with open(pending, "wb") as f:
        shutil.copyfileobj(file.file, f)

    try:
        validate_xlsx(str(pending))
    except ValueError as e:
        try: pending.unlink()
        except Exception: pass
        raise HTTPException(400, f"Planilla inválida: {e}")

    # Cargar ambas listas y comparar.
    try:
        new_tags = load_tags(xlsx_path=str(pending))
    except Exception as e:
        try: pending.unlink()
        except Exception: pass
        raise HTTPException(400, f"Error parseando planilla: {e}")

    try:
        old_tags = load_tags()
    except Exception:
        old_tags = []

    conn = _db_connect_unrowed()
    try:
        override_addrs = set(fetch_overrides(conn).keys())
    finally:
        conn.close()

    diff = _compute_diff(old_tags, new_tags, override_addrs)

    # Si hay errores estructurales, se mantiene el pending pero el confirm
    # va a rechazar — la UI debería mostrar los errores y no ofrecer confirm.
    return {
        "ok":          not diff["errors"],
        "pending_id":  token,
        "filename":    file.filename,
        **diff,
    }


@app.post("/api/tags/upload/confirm", dependencies=[Depends(require_config_session)])
async def api_tags_upload_confirm(request: Request):
    body = await request.json()
    token = (body.get("pending_id") or "").strip()
    pending = _pending_path(token)
    if not pending.exists():
        raise HTTPException(404, "Pending no encontrado o expirado. Volvé a subir.")

    # Re-validar (defensa en profundidad: alguien podría borrar/corromper el archivo
    # entre preview y confirm, o cambiar el activo de fondo).
    try:
        validate_xlsx(str(pending))
        new_tags = load_tags(xlsx_path=str(pending))
    except Exception as e:
        try: pending.unlink()
        except Exception: pass
        raise HTTPException(400, f"El pending dejó de ser válido: {e}")

    try:
        old_tags = load_tags()
    except Exception:
        old_tags = []

    conn = _db_connect_unrowed()
    try:
        override_addrs = set(fetch_overrides(conn).keys())
    finally:
        conn.close()

    diff = _compute_diff(old_tags, new_tags, override_addrs)
    if diff["errors"]:
        raise HTTPException(400, "El pending tiene errores: " + "; ".join(diff["errors"]))

    backed_up = _backup_active_xlsx("pre-upload")
    shutil.move(str(pending), ACTIVE_XLSX)
    _cleanup_pending()

    _refresh_tags_cache()
    reload_event.set()
    return {"ok": True, "backup": backed_up, "summary": diff["summary"]}


@app.delete("/api/tags/preview/{token}", dependencies=[Depends(require_config_session)])
def api_tags_preview_cancel(token: str):
    p = _pending_path(token)
    if p.exists():
        try: p.unlink()
        except Exception: pass
    return {"ok": True}


@app.get("/api/tags/backups", dependencies=[Depends(require_config_session)])
def api_tags_backups():
    items = _list_backups()
    active = None
    if os.path.exists(ACTIVE_XLSX):
        try:
            n_valid = validate_xlsx(ACTIVE_XLSX)
        except Exception:
            n_valid = None
        st = os.stat(ACTIVE_XLSX)
        active = {
            "name":  ACTIVE_XLSX,
            "size":  st.st_size,
            "mtime": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%dT%H:%M:%S"),
            "tags":  n_valid,
        }
    return {"active": active, "items": items}


@app.post("/api/tags/rollback", dependencies=[Depends(require_config_session)])
async def api_tags_rollback(request: Request):
    body = await request.json()
    name = (body.get("backup") or "").strip()
    if not name or "/" in name or "\\" in name or ".." in name:
        raise HTTPException(400, "Nombre de backup inválido.")
    src = Path(BACKUPS_DIR) / name
    if not src.exists():
        raise HTTPException(404, f"Backup no encontrado: {name}")
    try:
        validate_xlsx(str(src))
    except ValueError as e:
        raise HTTPException(400, f"El backup no es una planilla válida: {e}")

    backed_up = _backup_active_xlsx("pre-rollback")
    shutil.copy2(str(src), ACTIVE_XLSX)

    _refresh_tags_cache()
    reload_event.set()
    return {"ok": True, "restored": name, "backup": backed_up}


@app.get("/api/tags/download", dependencies=[Depends(require_config_session)])
def api_tags_download():
    if not os.path.exists(ACTIVE_XLSX):
        raise HTTPException(404, "No hay xlsx activo.")
    return FileResponse(
        ACTIVE_XLSX,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=ACTIVE_XLSX,
    )


@app.get("/api/tags/download/{name}", dependencies=[Depends(require_config_session)])
def api_tags_download_backup(name: str):
    if "/" in name or "\\" in name or ".." in name:
        raise HTTPException(400, "Nombre inválido.")
    src = Path(BACKUPS_DIR) / name
    if not src.exists():
        raise HTTPException(404, "Backup no encontrado.")
    return FileResponse(
        str(src),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=name,
    )


# ---------------------------------------------------------------------------
# ADMIN — auto-update desde GitHub (sin tocar la consola)
# ---------------------------------------------------------------------------
#
# El truco: en lugar de pedirle a systemd que reinicie el servicio (que
# requeriría configuración previa de sudoers), la app se reemplaza a sí
# misma con os.execv() después de pullear. El proceso actual se transforma
# en una invocación fresca de Python, que re-importa todo el código del
# disco — incluyendo lo que acaba de bajar de git. Cero intervención manual.

_REPO_DIR = Path(__file__).resolve().parent

# URL del repo remoto. Algunos hostings (Render, Railway) hacen un clone que
# no preserva el remote 'origin', así que lo configuramos lazy si falta.
_REPO_URL = os.environ.get("REPO_URL", "https://github.com/Joaquin493/datalogger.git")


def _ensure_origin_remote():
    """Si no existe el remote 'origin', lo agrega apuntando a _REPO_URL.
    Idempotente — si ya está, no hace nada."""
    try:
        r = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=str(_REPO_DIR), capture_output=True, text=True, timeout=5,
        )
        if r.returncode == 0 and r.stdout.strip():
            return
        subprocess.run(
            ["git", "remote", "add", "origin", _REPO_URL],
            cwd=str(_REPO_DIR), capture_output=True, timeout=5,
        )
    except Exception:
        pass


def _unshallow_if_needed():
    """En hostings con shallow clone (Render, Railway), el repo solo tiene
    el último commit. Sin historial no podemos chequear ancestros (rollback
    floor) ni listar versiones. Detectamos y traemos todo el historial.
    Operación one-shot — después de la primera vez el repo deja de ser shallow.
    """
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--is-shallow-repository"],
            cwd=str(_REPO_DIR), capture_output=True, text=True, timeout=5,
        )
        if not (r.returncode == 0 and r.stdout.strip() == "true"):
            return
        subprocess.run(
            ["git", "fetch", "--unshallow", "origin", "main"],
            cwd=str(_REPO_DIR), capture_output=True, timeout=120,
        )
    except Exception:
        pass


# Piso de rollback: SHA del primer commit que incluye la feature de auto-update.
# Volver a un commit anterior a este dejaría al operador sin acceso a la UI de
# actualización, atrapándolo en una versión vieja sin posibilidad de volver
# salvo SSH. El gate aplica a /api/admin/rollback y se refleja en el historial.
_ROLLBACK_FLOOR_SHA = "c140d347824f71e217957565b1b481295f7ace4b"


def _is_rollback_allowed(target_full_sha: str) -> bool:
    """True si target tiene el piso como ancestro (o ES el piso)."""
    try:
        r = subprocess.run(
            ["git", "merge-base", "--is-ancestor", _ROLLBACK_FLOOR_SHA, target_full_sha],
            cwd=str(_REPO_DIR), capture_output=True, timeout=10,
        )
        return r.returncode == 0
    except Exception:
        # Si falla el chequeo, ser conservadores y NO permitir.
        return False


def _git(*args, check=True) -> subprocess.CompletedProcess:
    """Corre git en el directorio del repo y devuelve el CompletedProcess."""
    return subprocess.run(
        ["git", *args],
        cwd=str(_REPO_DIR),
        capture_output=True,
        text=True,
        check=check,
        timeout=60,
    )


def _git_safe(*args) -> Optional[str]:
    """Variante que devuelve stdout o None si falla. Para info no crítica."""
    try:
        return _git(*args).stdout.strip()
    except Exception:
        return None


def _parse_commit_log(text: str) -> list[dict]:
    """Parsea salida de `git log --format='%h|%ci|%s'`."""
    out = []
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("|", 2)
        if len(parts) != 3:
            continue
        out.append({"sha": parts[0], "date": parts[1], "subject": parts[2]})
    return out


@app.get("/api/admin/version", dependencies=[Depends(require_config_session)])
def api_admin_version():
    """Devuelve commit actual + commits pendientes en origin/main.

    Hace `git fetch` para asegurar que origin/main esté actualizado. No
    modifica nada de la working tree.
    """
    if not (_REPO_DIR / ".git").exists():
        raise HTTPException(500, "El directorio del proyecto no es un repo git.")

    # Asegurar que existe el remote 'origin' (Render/Railway no lo preservan).
    _ensure_origin_remote()
    # Si el repo es shallow (clone de 1 sola profundidad), traer todo el
    # historial. Sin esto, el chequeo de ancestro contra el rollback floor
    # falla porque el floor SHA no está en el repo local.
    _unshallow_if_needed()

    # Fetch (puede fallar si no hay red — devolvemos info parcial en ese caso).
    fetch_error = None
    try:
        _git("fetch", "--quiet", "origin", "main")
    except Exception as e:
        msg = str(e)
        if hasattr(e, "stderr") and e.stderr:
            msg = e.stderr.strip()
        fetch_error = msg

    current_log = _git_safe("log", "-1", "--format=%h|%ci|%s") or ""
    current = _parse_commit_log(current_log)
    current_info = current[0] if current else None

    # Branch actual (para mostrar si no es main).
    branch = _git_safe("rev-parse", "--abbrev-ref", "HEAD")

    # ¿Hay cambios locales sin commitear en archivos trackeados?
    dirty_raw = _git_safe("status", "--porcelain", "--untracked-files=no") or ""
    dirty_files = [
        line[3:].strip() for line in dirty_raw.splitlines() if line.strip()
    ]
    dirty = bool(dirty_files)

    behind = 0
    pending = []
    if not fetch_error:
        # Commits que están en origin/main pero no acá (los que faltan aplicar).
        try:
            behind_str = _git("rev-list", "--count", "HEAD..origin/main").stdout.strip()
            behind = int(behind_str or "0")
        except Exception:
            behind = 0
        if behind > 0:
            pending_log = _git_safe("log", "--format=%h|%ci|%s", "HEAD..origin/main") or ""
            pending = _parse_commit_log(pending_log)

    # Detectar si requirements.txt cambió entre HEAD y origin/main.
    deps_changed = False
    if behind > 0:
        try:
            diff_files = _git("diff", "--name-only", "HEAD..origin/main").stdout
            deps_changed = "requirements.txt" in diff_files.splitlines()
        except Exception:
            pass

    return {
        "current":       current_info,
        "branch":        branch,
        "behind":        behind,
        "pending":       pending,
        "deps_changed":  deps_changed,
        "dirty":         dirty,
        "dirty_files":   dirty_files,
        "fetch_error":   fetch_error,
    }


def _restart_self_after_delay(delay_s: float = 1.5):
    """Re-exec del proceso actual con los mismos argumentos. Lo invoca un
    BackgroundTask para que primero termine la response HTTP."""
    def _go():
        time.sleep(delay_s)
        print("[AUTO_UPDATE] Re-exec del proceso para tomar codigo nuevo...", flush=True)
        # sys.orig_argv preserva los args exactos con que se invoco Python
        # (incluyendo "-m uvicorn ..."). Disponible desde 3.10.
        argv = getattr(sys, "orig_argv", None) or sys.argv
        try:
            os.execv(sys.executable, argv)
        except Exception as e:
            print(f"[AUTO_UPDATE] Re-exec fallo: {e}", flush=True)
            # Si execv falla, salimos con 1 para que systemd nos levante
            # (asumiendo Restart=on-failure o always).
            os._exit(1)
    threading.Thread(target=_go, daemon=True).start()


@app.post("/api/admin/update", dependencies=[Depends(require_config_session)])
def api_admin_update(background_tasks: BackgroundTasks):
    """Pullea, instala deps si cambiaron, y se reinicia.

    El restart es por os.execv — no requiere systemd ni sudoers. La response
    se manda antes del restart; la UI debería polear /healthz para detectar
    cuándo vuelve.
    """
    if not (_REPO_DIR / ".git").exists():
        raise HTTPException(500, "El directorio del proyecto no es un repo git.")

    _ensure_origin_remote()

    # 1. Seguridad: no aplicar si hay cambios locales sin commitear.
    if _git_safe("status", "--porcelain", "--untracked-files=no"):
        raise HTTPException(409, "Hay cambios locales sin commitear. Resolver primero.")

    # 2. Fetch + capturar SHA viejo.
    try:
        _git("fetch", "origin", "main")
    except subprocess.CalledProcessError as e:
        raise HTTPException(502, f"git fetch fallo: {e.stderr.strip()}")
    except subprocess.TimeoutExpired:
        raise HTTPException(504, "git fetch timeout (¿hay salida a internet?).")

    old_sha = _git_safe("rev-parse", "HEAD") or "?"
    remote_sha = _git_safe("rev-parse", "origin/main") or "?"
    if old_sha == remote_sha:
        return {"ok": True, "updated": False, "message": "Ya esta al dia.", "sha": old_sha}

    # 3. Detectar si requirements cambió antes de pullear.
    deps_changed = False
    try:
        diff_files = _git("diff", "--name-only", "HEAD..origin/main").stdout
        deps_changed = "requirements.txt" in diff_files.splitlines()
    except Exception:
        pass

    # 4. Pull (solo fast-forward — sin merges automáticos).
    try:
        pull_out = _git("pull", "--ff-only", "origin", "main")
    except subprocess.CalledProcessError as e:
        raise HTTPException(409, f"git pull fallo (¿conflicto?): {e.stderr.strip() or e.stdout.strip()}")

    new_sha = _git_safe("rev-parse", "HEAD") or "?"

    # 5. pip install si requirements cambio.
    pip_log = None
    if deps_changed:
        pip_args = [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"]
        # Si NO estamos en venv, instalar en --user para no necesitar root.
        if sys.prefix == sys.base_prefix:
            pip_args.append("--user")
        try:
            pip_result = subprocess.run(
                pip_args,
                cwd=str(_REPO_DIR),
                capture_output=True,
                text=True,
                timeout=300,
                check=True,
            )
            pip_log = (pip_result.stdout[-2000:] + pip_result.stderr[-2000:]).strip()
        except subprocess.CalledProcessError as e:
            raise HTTPException(
                500,
                f"pip install fallo. El codigo se actualizo a {new_sha[:7]} pero las deps "
                f"viejas siguen cargadas. Reiniciar manualmente despues de resolver. "
                f"Error: {(e.stderr or e.stdout or '').strip()[-500:]}"
            )

    # 6. Guardar evento en system_events para auditoria.
    try:
        conn = _db_connect_unrowed()
        try:
            conn.execute(
                "INSERT INTO system_events(event_type, description, timestamp) VALUES (?, ?, ?)",
                ("AUTO_UPDATE",
                 f"Pull {old_sha[:7]} -> {new_sha[:7]}"
                 + (" + pip install" if deps_changed else ""),
                 _now_ts()),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass

    # 7. Programar restart por re-exec (despues de mandar la response).
    background_tasks.add_task(_restart_self_after_delay)

    return {
        "ok":            True,
        "updated":       True,
        "old_sha":       old_sha,
        "new_sha":       new_sha,
        "deps_changed":  deps_changed,
        "restart_in_s":  1.5,
    }


@app.get("/api/admin/history", dependencies=[Depends(require_config_session)])
def api_admin_history(limit: int = Query(30, ge=1, le=200)):
    """Devuelve los últimos N commits de origin/main (timeline unificado
    al que se puede ir adelante o atrás).

    No hace fetch — refleja lo que está en el repo local. Para datos
    frescos, primero apretá "Buscar actualizaciones".
    """
    if not (_REPO_DIR / ".git").exists():
        raise HTTPException(500, "El directorio del proyecto no es un repo git.")

    _ensure_origin_remote()
    _unshallow_if_needed()

    current_sha = _git_safe("rev-parse", "HEAD") or ""
    # Usamos el SHA largo para comparar, pero mostramos el corto.
    log_out = _git_safe(
        "log", f"-{limit}", "--format=%H|%h|%ci|%an|%s", "origin/main"
    ) or ""

    items = []
    for line in log_out.splitlines():
        parts = line.split("|", 4)
        if len(parts) != 5:
            continue
        full_sha, short_sha, date, author, subject = parts
        # Filtramos en el backend: solo devolvemos commits a los que se puede
        # rollback. Los anteriores al piso quedarían atrapando al operador
        # sin UI de update, no tiene sentido mostrarlos.
        if not _is_rollback_allowed(full_sha):
            continue
        items.append({
            "sha":         short_sha,
            "full_sha":    full_sha,
            "date":        date,
            "author":      author,
            "subject":     subject,
            "is_current":  full_sha == current_sha,
        })

    return {
        "current_sha": current_sha[:7] if current_sha else None,
        "floor_sha":   _ROLLBACK_FLOOR_SHA[:7],
        "items":       items,
    }


# Parser para las líneas del log. Formato emitido por modbus_logger:
#   2026-05-12 14:30:15.123 | INFO     | plc_logger.main | mensaje
_LOG_LINE_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})"
    r"\s\|\s(\w+)\s*\|\s([^|]+)\s\|\s(.*)$"
)

_LOG_PATH = _REPO_DIR / "logs" / "logger.log"


@app.get("/api/admin/logs", dependencies=[Depends(require_config_session)])
def api_admin_logs(
    lines: int = Query(200, ge=10, le=2000),
    level: str = Query("all"),
):
    """Devuelve las últimas N líneas del log de la app.

    Lee solo el archivo activo (logger.log). Los rotados (.1, .2, ...) no se
    incluyen — sirven para tail-f, no para histórico profundo. Cada línea se
    parsea para extraer timestamp/nivel/logger/mensaje. Las que no matchean
    (continuaciones de traceback, etc.) van con level=None y se renderizan
    visualmente como continuación de la línea anterior.
    """
    if not _LOG_PATH.exists():
        return {"lines": [], "file": str(_LOG_PATH), "exists": False, "size": 0}

    # Leemos solo los últimos ~lines*500 bytes para no traer el archivo entero
    # (logger.log llega hasta 1MB antes de rotar).
    with open(_LOG_PATH, "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        read_size = min(size, max(lines * 500, 16384))
        f.seek(max(0, size - read_size))
        data = f.read().decode("utf-8", errors="replace")

    # Si tomamos del medio de una línea, descartamos esa primera parcial.
    raw_lines = data.splitlines()
    if size > read_size and raw_lines:
        raw_lines = raw_lines[1:]

    raw_lines = raw_lines[-lines:]

    parsed = []
    for raw in raw_lines:
        m = _LOG_LINE_RE.match(raw)
        if m:
            lvl = m.group(2).strip()
            lgr = m.group(3).strip()
            # Promovemos INFO de plc_logger.events a un "nivel" virtual EVENT,
            # asi se visualizan distinto en la UI y se pueden filtrar solos.
            if lvl == "INFO" and lgr.endswith(".events"):
                lvl = "EVENT"
            parsed.append({
                "ts":     m.group(1),
                "level":  lvl,
                "logger": lgr,
                "msg":    m.group(4),
            })
        else:
            # Continuación de una línea (típico en tracebacks).
            parsed.append({"ts": None, "level": None, "logger": None, "msg": raw})

    # Filtro de nivel.
    # EVENT no tiene "floor" — es un filtro especial que muestra SOLO los
    # eventos de cambio de tag. Los floors numéricos son la jerarquía estándar.
    level_up = level.upper()
    if level_up == "EVENT":
        keep = []
        last_shown = False
        for line in parsed:
            if line["level"] is None:
                if last_shown:
                    keep.append(line)
            elif line["level"] == "EVENT":
                keep.append(line)
                last_shown = True
            else:
                last_shown = False
        parsed = keep
    else:
        LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "EVENT": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
        if level_up in LEVEL_ORDER:
            floor = LEVEL_ORDER[level_up]
            keep = []
            last_shown = False
            for line in parsed:
                if line["level"] is None:
                    if last_shown:
                        keep.append(line)
                elif LEVEL_ORDER.get(line["level"], 0) >= floor:
                    keep.append(line)
                    last_shown = True
                else:
                    last_shown = False
            parsed = keep

    return {"lines": parsed, "file": str(_LOG_PATH), "exists": True, "size": size}


@app.post("/api/admin/rollback", dependencies=[Depends(require_config_session)])
async def api_admin_rollback(request: Request, background_tasks: BackgroundTasks):
    """Hace `git reset --hard <sha>` para volver a un commit anterior.

    Solo acepta SHAs que estén en el historial de origin/main (no permite
    saltar a commits arbitrarios). Si requirements.txt difiere, corre
    pip install. Se reinicia con os.execv igual que el update.
    """
    body = await request.json()
    sha = (body.get("sha") or "").strip()
    if not re.fullmatch(r"[A-Fa-f0-9]{7,40}", sha):
        raise HTTPException(400, "SHA inválido.")

    # No aplicar si hay cambios locales sin commitear.
    if _git_safe("status", "--porcelain", "--untracked-files=no"):
        raise HTTPException(409, "Hay cambios locales sin commitear. Resolver primero.")

    # Resolver el SHA a su forma larga (acepta abreviado).
    try:
        full_sha = _git("rev-parse", "--verify", sha + "^{commit}").stdout.strip()
    except subprocess.CalledProcessError:
        raise HTTPException(404, f"El SHA {sha} no existe en el repo local.")

    # Validar que el SHA esté en el historial de origin/main (no aceptamos
    # commits sueltos / branches random).
    check = subprocess.run(
        ["git", "merge-base", "--is-ancestor", full_sha, "origin/main"],
        cwd=str(_REPO_DIR),
        capture_output=True,
        timeout=10,
    )
    if check.returncode != 0:
        raise HTTPException(400, "El SHA no está en el historial de origin/main.")

    # Gate: no permitir rollback a commits anteriores a la introducción de
    # la feature de auto-update. Si volviéramos antes de eso, perderíamos
    # la UI para volver hacia adelante, dejando al operador atrapado.
    if not _is_rollback_allowed(full_sha):
        raise HTTPException(
            400,
            f"No se puede volver a esa versión: es anterior a la feature de "
            f"actualización ({_ROLLBACK_FLOOR_SHA[:7]}). Volver allí dejaría "
            f"al sistema sin manera de actualizarse desde la UI."
        )

    old_sha = _git_safe("rev-parse", "HEAD") or "?"
    if old_sha == full_sha:
        return {"ok": True, "rolled_back": False, "message": "Ya estás en esa versión.", "sha": full_sha[:7]}

    # ¿requirements.txt difiere entre el actual y el target?
    deps_changed = False
    try:
        diff_files = _git("diff", "--name-only", old_sha, full_sha).stdout
        deps_changed = "requirements.txt" in diff_files.splitlines()
    except Exception:
        pass

    # Reset --hard al SHA pedido.
    try:
        _git("reset", "--hard", full_sha)
    except subprocess.CalledProcessError as e:
        raise HTTPException(500, f"git reset fallo: {e.stderr.strip() or e.stdout.strip()}")

    # pip install si requirements cambió.
    if deps_changed:
        pip_args = [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"]
        if sys.prefix == sys.base_prefix:
            pip_args.append("--user")
        try:
            subprocess.run(
                pip_args, cwd=str(_REPO_DIR),
                capture_output=True, text=True, timeout=300, check=True,
            )
        except subprocess.CalledProcessError as e:
            raise HTTPException(
                500,
                f"pip install fallo despues del rollback a {full_sha[:7]}. "
                f"Resolver manualmente. Error: {(e.stderr or e.stdout or '').strip()[-500:]}"
            )

    # Auditoría.
    try:
        conn = _db_connect_unrowed()
        try:
            conn.execute(
                "INSERT INTO system_events(event_type, description, timestamp) VALUES (?, ?, ?)",
                ("ROLLBACK",
                 f"Rollback {old_sha[:7]} -> {full_sha[:7]}"
                 + (" + pip install" if deps_changed else ""),
                 _now_ts()),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass

    background_tasks.add_task(_restart_self_after_delay)

    return {
        "ok":            True,
        "rolled_back":   True,
        "old_sha":       old_sha,
        "new_sha":       full_sha,
        "deps_changed":  deps_changed,
        "restart_in_s":  1.5,
    }
