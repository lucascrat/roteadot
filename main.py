import os
import base64
import secrets
import logging
from datetime import datetime, timedelta
from contextlib import asynccontextmanager, contextmanager
import sqlite3

import httpx
import psycopg2
import psycopg2.pool
import psycopg2.extras
from fastapi import FastAPI, Request, Form, Cookie, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL  = os.getenv("DATABASE_URL", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "M3uPro@2026!")
SESSION_TIMEOUT = int(os.getenv("SESSION_TIMEOUT_MINUTES", "10"))

if not DATABASE_URL:
    raise RuntimeError("❌ Variável de ambiente DATABASE_URL não definida. Configure no Coolify em Environment Variables.")
MAX_LISTS = 10

admin_sessions: dict[str, datetime] = {}

# ---------- Database ----------

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        _pool = psycopg2.pool.ThreadedConnectionPool(1, 10, DATABASE_URL)
        logger.info("PostgreSQL connection pool created")
    return _pool


@contextmanager
def get_db():
    conn = get_pool().getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        get_pool().putconn(conn)


def fetchall(conn, query: str, params=()) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params)
        return [dict(r) for r in cur.fetchall()]


def fetchone(conn, query: str, params=()) -> dict | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else None


def execute(conn, query: str, params=()):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.rowcount


def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS m3u_lists (
                    id            SERIAL PRIMARY KEY,
                    name          TEXT        NOT NULL,
                    source_url    TEXT,
                    content       TEXT,
                    status        TEXT        DEFAULT 'available',
                    session_ip    TEXT,
                    session_token TEXT,
                    last_accessed TIMESTAMP,
                    created_at    TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS access_log (
                    id         SERIAL PRIMARY KEY,
                    ip_address TEXT,
                    list_id    INTEGER,
                    list_name  TEXT,
                    action     TEXT,
                    timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
    logger.info("Database initialised")


def cleanup_expired_sessions():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE m3u_lists
                SET status='available', session_ip=NULL, session_token=NULL, last_accessed=NULL
                WHERE status='in_use' AND last_accessed < %s
            """, (datetime.now() - timedelta(minutes=SESSION_TIMEOUT),))
            released = cur.rowcount
    if released:
        logger.info(f"Released {released} expired session(s)")


# ---------- App lifecycle ----------

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app):
    init_db()
    scheduler.add_job(cleanup_expired_sessions, "interval", minutes=1)
    scheduler.start()
    logger.info("Scheduler started — timeout %d min", SESSION_TIMEOUT)
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
    lines = content.splitlines()
    result = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            result.append(f"{base_url}/s/{token}/{encode_url(stripped)}")
        else:
            result.append(line)
    return "\n".join(result)


async def fetch_raw_m3u(row: dict) -> str:
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
    return f"{scheme}://{request.headers.get('host', 'localhost')}"


# ---------- Public: playlist ----------

@app.get("/playlist.m3u", response_class=PlainTextResponse)
async def serve_playlist(request: Request):
    ip = get_client_ip(request)

    with get_db() as conn:
        row = fetchone(conn,
            "SELECT * FROM m3u_lists WHERE status='in_use' AND session_ip=%s", (ip,))

        if row:
            execute(conn,
                "UPDATE m3u_lists SET last_accessed=%s WHERE id=%s",
                (datetime.now(), row["id"]))
            raw = await fetch_raw_m3u(row)
            return PlainTextResponse(raw, media_type="audio/x-mpegurl")

        available = fetchone(conn,
            "SELECT * FROM m3u_lists WHERE status='available' ORDER BY RANDOM() LIMIT 1")

        if not available:
            busy = "#EXTM3U\n#EXTINF:-1,Todas as listas estao em uso. Tente novamente.\nhttp://0.0.0.0\n"
            return PlainTextResponse(busy, media_type="audio/x-mpegurl", status_code=503)

        execute(conn, """
            UPDATE m3u_lists
            SET status='in_use', session_ip=%s, last_accessed=%s
            WHERE id=%s
        """, (ip, datetime.now(), available["id"]))
        execute(conn,
            "INSERT INTO access_log (ip_address, list_id, list_name, action) VALUES (%s,%s,%s,'assigned')",
            (ip, available["id"], available["name"]))

        raw = await fetch_raw_m3u(available)
        logger.info(f"List '{available['name']}' assigned to {ip}")
        return PlainTextResponse(raw, media_type="audio/x-mpegurl")


# ---------- Public: stream redirect ----------

@app.get("/s/{token}/{encoded}")
async def stream_redirect(token: str, encoded: str):
    try:
        original_url = decode_url(encoded)
    except Exception:
        raise HTTPException(status_code=400, detail="URL inválida")

    with get_db() as conn:
        execute(conn,
            "UPDATE m3u_lists SET last_accessed=%s WHERE session_token=%s AND status='in_use'",
            (datetime.now(), token))

    return RedirectResponse(original_url, status_code=302)


# ---------- Admin: auth ----------

@app.get("/admin/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": ""})


@app.post("/admin/login")
async def do_login(request: Request, password: str = Form(...)):
    if password == ADMIN_PASSWORD:
        token = secrets.token_hex(32)
        admin_sessions[token] = datetime.now()
        resp = RedirectResponse("/admin", status_code=302)
        resp.set_cookie("admin_token", token, httponly=True, samesite="lax", max_age=86400)
        return resp
    return templates.TemplateResponse(
        "login.html", {"request": request, "error": "Senha incorreta"}, status_code=401)


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

    with get_db() as conn:
        lists = fetchall(conn, "SELECT * FROM m3u_lists ORDER BY id")
        logs  = fetchall(conn, "SELECT * FROM access_log ORDER BY timestamp DESC LIMIT 30")

    total     = len(lists)
    available = sum(1 for l in lists if l["status"] == "available")
    in_use    = total - available

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "lists": lists,
        "logs": logs,
        "total": total,
        "available": available,
        "in_use": in_use,
        "max_lists": MAX_LISTS,
        "timeout": SESSION_TIMEOUT,
        "playlist_url": f"{get_base_url(request)}/playlist.m3u",
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

    url  = source_url.strip() or None
    body = content.strip() or None
    if not url and not body:
        return RedirectResponse("/admin?msg=Informe+URL+ou+conteudo", status_code=302)

    with get_db() as conn:
        count = fetchone(conn, "SELECT COUNT(*) AS c FROM m3u_lists")["c"]
        if count >= MAX_LISTS:
            return RedirectResponse(f"/admin?msg=Limite+de+{MAX_LISTS}+listas+atingido", status_code=302)
        execute(conn,
            "INSERT INTO m3u_lists (name, source_url, content) VALUES (%s,%s,%s)",
            (name.strip(), url, body))

    return RedirectResponse("/admin?msg=Lista+adicionada", status_code=302)


@app.post("/admin/lists/{list_id}/release")
async def release_list(list_id: int, admin_token: str = Cookie(default=None)):
    if not is_admin(admin_token):
        return RedirectResponse("/admin/login", status_code=302)
    with get_db() as conn:
        execute(conn,
            "UPDATE m3u_lists SET status='available', session_ip=NULL, session_token=NULL, last_accessed=NULL WHERE id=%s",
            (list_id,))
    return RedirectResponse("/admin?msg=Lista+liberada", status_code=302)


@app.post("/admin/lists/{list_id}/delete")
async def delete_list(list_id: int, admin_token: str = Cookie(default=None)):
    if not is_admin(admin_token):
        return RedirectResponse("/admin/login", status_code=302)
    with get_db() as conn:
        execute(conn, "DELETE FROM m3u_lists WHERE id=%s", (list_id,))
    return RedirectResponse("/admin?msg=Lista+removida", status_code=302)


@app.post("/admin/release-all")
async def release_all(admin_token: str = Cookie(default=None)):
    if not is_admin(admin_token):
        return RedirectResponse("/admin/login", status_code=302)
    with get_db() as conn:
        execute(conn,
            "UPDATE m3u_lists SET status='available', session_ip=NULL, session_token=NULL, last_accessed=NULL WHERE status='in_use'")
    return RedirectResponse("/admin?msg=Todas+as+sessoes+liberadas", status_code=302)
