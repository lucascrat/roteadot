import os
import base64
import secrets
import logging
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import sqlite3

import httpx
from fastapi import FastAPI, Request, Form, Cookie, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "data/roteador.db")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "M3uPro@2026!")
SESSION_TIMEOUT = int(os.getenv("SESSION_TIMEOUT_MINUTES", "10"))
MAX_LISTS = 10

admin_sessions: dict[str, datetime] = {}

# ---------- Database ----------

def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS m3u_lists (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            name          TEXT    NOT NULL,
            source_url    TEXT,
            content       TEXT,
            status        TEXT    DEFAULT 'available',
            session_ip    TEXT,
            session_token TEXT,
            last_accessed TIMESTAMP,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS access_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ip_address TEXT,
            list_id    INTEGER,
            list_name  TEXT,
            action     TEXT,
            timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    # Migration: add session_token column to existing databases
    try:
        conn.execute("ALTER TABLE m3u_lists ADD COLUMN session_token TEXT")
        conn.commit()
    except Exception:
        pass
    conn.commit()
    conn.close()
    logger.info("Database initialised")


def cleanup_expired_sessions():
    conn = get_db()
    threshold = datetime.now() - timedelta(minutes=SESSION_TIMEOUT)
    conn.execute("""
        UPDATE m3u_lists
        SET status='available', session_ip=NULL, session_token=NULL, last_accessed=NULL
        WHERE status='in_use' AND last_accessed < ?
    """, (threshold,))
    released = conn.total_changes
    conn.commit()
    conn.close()
    if released:
        logger.info(f"Released {released} expired session(s)")


# ---------- App lifecycle ----------

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler.add_job(cleanup_expired_sessions, "interval", minutes=1)
    scheduler.start()
    logger.info("Scheduler started — checking every 1 min, timeout %d min", SESSION_TIMEOUT)
    yield
    scheduler.shutdown()


app = FastAPI(title="M3U Router", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

# ---------- Helpers ----------

def get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host or "unknown"


def is_admin(admin_token: str | None) -> bool:
    if not admin_token or admin_token not in admin_sessions:
        return False
    if datetime.now() - admin_sessions[admin_token] > timedelta(hours=24):
        del admin_sessions[admin_token]
        return False
    return True


def encode_url(url: str) -> str:
    return base64.urlsafe_b64encode(url.encode()).decode().rstrip("=")


def decode_url(encoded: str) -> str:
    padding = 4 - len(encoded) % 4
    if padding != 4:
        encoded += "=" * padding
    return base64.urlsafe_b64decode(encoded.encode()).decode()


def rewrite_m3u_urls(content: str, base_url: str, token: str) -> str:
    """Replace every stream URL in the M3U with a redirect through our /s/ endpoint."""
    lines = content.splitlines()
    result = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            encoded = encode_url(stripped)
            result.append(f"{base_url}/s/{token}/{encoded}")
        else:
            result.append(line)
    return "\n".join(result)


async def fetch_raw_m3u(row) -> str:
    row = dict(row)
    if row.get("source_url"):
        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                resp = await client.get(row["source_url"])
                resp.raise_for_status()
                return resp.text
        except Exception as exc:
            logger.error(f"Failed to fetch M3U URL: {exc}")
            return "#EXTM3U\n#EXTINF:-1,Erro ao buscar lista remota\nhttp://0.0.0.0\n"
    return row.get("content") or "#EXTM3U\n"


def get_base_url(request: Request) -> str:
    scheme = "https" if request.headers.get("x-forwarded-proto") == "https" else "http"
    host = request.headers.get("host", "localhost")
    return f"{scheme}://{host}"


# ---------- Public: playlist ----------

@app.get("/playlist.m3u", response_class=PlainTextResponse)
async def serve_playlist(request: Request):
    ip = get_client_ip(request)
    conn = get_db()

    # Re-use existing session
    row = conn.execute(
        "SELECT * FROM m3u_lists WHERE status='in_use' AND session_ip=?", (ip,)
    ).fetchone()

    if row:
        conn.execute(
            "UPDATE m3u_lists SET last_accessed=? WHERE id=?",
            (datetime.now(), row["id"]),
        )
        conn.commit()
        token = row["session_token"]
        raw = await fetch_raw_m3u(row)
        content = rewrite_m3u_urls(raw, get_base_url(request), token)
        conn.close()
        return PlainTextResponse(content, media_type="audio/x-mpegurl")

    # Assign a free list
    available = conn.execute(
        "SELECT * FROM m3u_lists WHERE status='available' ORDER BY RANDOM() LIMIT 1"
    ).fetchone()

    if not available:
        conn.close()
        busy = "#EXTM3U\n#EXTINF:-1,Todas as listas estao em uso. Tente novamente em alguns minutos.\nhttp://0.0.0.0\n"
        return PlainTextResponse(busy, media_type="audio/x-mpegurl", status_code=503)

    token = secrets.token_urlsafe(16)
    conn.execute("""
        UPDATE m3u_lists
        SET status='in_use', session_ip=?, session_token=?, last_accessed=?
        WHERE id=?
    """, (ip, token, datetime.now(), available["id"]))
    conn.execute(
        "INSERT INTO access_log (ip_address, list_id, list_name, action) VALUES (?,?,?,'assigned')",
        (ip, available["id"], available["name"]),
    )
    conn.commit()
    raw = await fetch_raw_m3u(available)
    content = rewrite_m3u_urls(raw, get_base_url(request), token)
    conn.close()
    logger.info(f"List '{available['name']}' assigned to {ip} (token {token[:8]}…)")
    return PlainTextResponse(content, media_type="audio/x-mpegurl")


# ---------- Public: stream redirect (activity tracker) ----------

@app.get("/s/{token}/{encoded}")
async def stream_redirect(token: str, encoded: str, request: Request):
    try:
        original_url = decode_url(encoded)
    except Exception:
        raise HTTPException(status_code=400, detail="URL inválida")

    conn = get_db()
    conn.execute(
        "UPDATE m3u_lists SET last_accessed=? WHERE session_token=? AND status='in_use'",
        (datetime.now(), token),
    )
    conn.commit()
    conn.close()

    return RedirectResponse(original_url, status_code=302)


# ---------- Admin: auth ----------

@app.get("/admin/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = ""):
    return templates.TemplateResponse("login.html", {"request": request, "error": error})


@app.post("/admin/login")
async def do_login(request: Request, password: str = Form(...)):
    if password == ADMIN_PASSWORD:
        token = secrets.token_hex(32)
        admin_sessions[token] = datetime.now()
        resp = RedirectResponse("/admin", status_code=302)
        resp.set_cookie("admin_token", token, httponly=True, samesite="lax", max_age=86400)
        return resp
    return templates.TemplateResponse(
        "login.html", {"request": request, "error": "Senha incorreta"}, status_code=401
    )


@app.get("/admin/logout")
async def logout(admin_token: str = Cookie(default=None)):
    if admin_token and admin_token in admin_sessions:
        del admin_sessions[admin_token]
    resp = RedirectResponse("/admin/login", status_code=302)
    resp.delete_cookie("admin_token")
    return resp


# ---------- Admin: dashboard ----------

@app.get("/admin", response_class=HTMLResponse)
async def dashboard(request: Request, admin_token: str = Cookie(default=None)):
    if not is_admin(admin_token):
        return RedirectResponse("/admin/login")

    conn = get_db()
    lists = conn.execute("SELECT * FROM m3u_lists ORDER BY id").fetchall()
    logs = conn.execute(
        "SELECT * FROM access_log ORDER BY timestamp DESC LIMIT 30"
    ).fetchall()
    conn.close()

    total = len(lists)
    available = sum(1 for l in lists if l["status"] == "available")
    in_use = total - available

    playlist_url = f"{get_base_url(request)}/playlist.m3u"

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "lists": lists,
        "logs": logs,
        "total": total,
        "available": available,
        "in_use": in_use,
        "max_lists": MAX_LISTS,
        "timeout": SESSION_TIMEOUT,
        "playlist_url": playlist_url,
        "now": datetime.now(),
    })


# ---------- Admin: list management ----------

@app.post("/admin/lists/add")
async def add_list(
    request: Request,
    name: str = Form(...),
    source_url: str = Form(default=""),
    content: str = Form(default=""),
    admin_token: str = Cookie(default=None),
):
    if not is_admin(admin_token):
        return RedirectResponse("/admin/login", status_code=302)

    if not name.strip():
        return RedirectResponse("/admin?msg=Nome+obrigatorio", status_code=302)

    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM m3u_lists").fetchone()[0]
    if count >= MAX_LISTS:
        conn.close()
        return RedirectResponse(f"/admin?msg=Limite+de+{MAX_LISTS}+listas+atingido", status_code=302)

    url = source_url.strip() or None
    body = content.strip() or None

    if not url and not body:
        conn.close()
        return RedirectResponse("/admin?msg=Informe+URL+ou+conteudo", status_code=302)

    conn.execute(
        "INSERT INTO m3u_lists (name, source_url, content) VALUES (?,?,?)",
        (name.strip(), url, body),
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/admin?msg=Lista+adicionada", status_code=302)


@app.post("/admin/lists/{list_id}/release")
async def release_list(list_id: int, admin_token: str = Cookie(default=None)):
    if not is_admin(admin_token):
        return RedirectResponse("/admin/login", status_code=302)
    conn = get_db()
    conn.execute(
        "UPDATE m3u_lists SET status='available', session_ip=NULL, session_token=NULL, last_accessed=NULL WHERE id=?",
        (list_id,),
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/admin?msg=Lista+liberada", status_code=302)


@app.post("/admin/lists/{list_id}/delete")
async def delete_list(list_id: int, admin_token: str = Cookie(default=None)):
    if not is_admin(admin_token):
        return RedirectResponse("/admin/login", status_code=302)
    conn = get_db()
    conn.execute("DELETE FROM m3u_lists WHERE id=?", (list_id,))
    conn.commit()
    conn.close()
    return RedirectResponse("/admin?msg=Lista+removida", status_code=302)


@app.post("/admin/release-all")
async def release_all(admin_token: str = Cookie(default=None)):
    if not is_admin(admin_token):
        return RedirectResponse("/admin/login", status_code=302)
    conn = get_db()
    conn.execute(
        "UPDATE m3u_lists SET status='available', session_ip=NULL, session_token=NULL, last_accessed=NULL WHERE status='in_use'"
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/admin?msg=Todas+as+sessoes+liberadas", status_code=302)
