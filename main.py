import os
import gzip
import secrets
import logging
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import sqlite3

import httpx
from fastapi import FastAPI, Request, Form, Cookie, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "data/roteador.db")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "M3uPro@2026!")
SESSION_TIMEOUT = int(os.getenv("SESSION_TIMEOUT_MINUTES", "10"))
CLEANUP_INTERVAL_MINUTES = int(os.getenv("CLEANUP_INTERVAL_MINUTES", "1"))
ACTIVE_CHECK_SECONDS = int(os.getenv("ACTIVE_CHECK_SECONDS", "45"))
PROVIDER_API_BASE = os.getenv(
    "PROVIDER_API_BASE",
    "http://gfbegin.top:8880/player_api.php?username={username}&password={password}",
)
CACHE_TTL_HOURS = int(os.getenv("CACHE_TTL_HOURS", "6"))
CACHE_REFRESH_MINUTES = int(os.getenv("CACHE_REFRESH_MINUTES", "60"))
PROVIDER_BASE = os.getenv(
    "PROVIDER_BASE",
    "http://gfbegin.top:8880/get.php?username={username}&password={password}&type=m3u_plus&output=m3u8",
)
MAX_LISTS = 10


def build_provider_url(username: str, password: str) -> str:
    from urllib.parse import quote
    return PROVIDER_BASE.format(username=quote(username, safe=""), password=quote(password, safe=""))

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
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT    NOT NULL,
            source_url   TEXT,
            content      TEXT,
            status       TEXT    DEFAULT 'available',
            session_ip   TEXT,
            last_accessed TIMESTAMP,
            cached_content TEXT,
            cached_at    TIMESTAMP,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    # Migrations for existing DBs
    existing = {r[1] for r in conn.execute("PRAGMA table_info(m3u_lists)").fetchall()}
    if "cached_content" not in existing:
        conn.execute("ALTER TABLE m3u_lists ADD COLUMN cached_content TEXT")
    if "cached_at" not in existing:
        conn.execute("ALTER TABLE m3u_lists ADD COLUMN cached_at TIMESTAMP")
    if "username" not in existing:
        conn.execute("ALTER TABLE m3u_lists ADD COLUMN username TEXT")
    if "password" not in existing:
        conn.execute("ALTER TABLE m3u_lists ADD COLUMN password TEXT")
    if "cached_gzip" not in existing:
        conn.execute("ALTER TABLE m3u_lists ADD COLUMN cached_gzip BLOB")
    # Libera espaco do cache antigo em texto
    conn.execute("UPDATE m3u_lists SET cached_content=NULL WHERE cached_content IS NOT NULL")
    conn.commit()
    conn.close()
    logger.info("Database initialised")


async def check_active_connections():
    """Pergunta ao provedor Xtream quantas conexoes estao ativas por credencial.

    - active_cons > 0: usuario ainda assistindo -> atualiza last_accessed
    - active_cons == 0: ninguem conectado -> libera a lista imediatamente

    Isso torna a deteccao de "logado/deslogado" quase imediata,
    independente do timeout fixo.
    """
    from urllib.parse import quote
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, username, password, session_ip, last_accessed "
        "FROM m3u_lists WHERE status='in_use' AND username IS NOT NULL AND password IS NOT NULL"
    ).fetchall()
    conn.close()

    if not rows:
        return

    async with httpx.AsyncClient(timeout=10, follow_redirects=True, headers=IPTV_HEADERS) as client:
        for row in rows:
            url = PROVIDER_API_BASE.format(
                username=quote(row["username"], safe=""),
                password=quote(row["password"], safe=""),
            )
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
                user_info = data.get("user_info", {}) if isinstance(data, dict) else {}
                active_raw = user_info.get("active_cons", 0)
                try:
                    active = int(active_raw)
                except (TypeError, ValueError):
                    active = 0
            except Exception as exc:
                logger.warning(f"player_api falhou lista id={row['id']}: {exc} — mantendo sessao")
                continue

            c = get_db()
            if active > 0:
                c.execute(
                    "UPDATE m3u_lists SET last_accessed=? WHERE id=? AND status='in_use'",
                    (datetime.now(), row["id"]),
                )
                c.commit()
                c.close()
                logger.info(f"Lista id={row['id']} ativa no provedor (active_cons={active})")
            else:
                # Confirma que esta sem conexao ha pelo menos uns segundos
                # para evitar corrida entre player abrindo stream e API.
                last = row["last_accessed"]
                try:
                    ts = last if isinstance(last, datetime) else datetime.fromisoformat(str(last))
                    idle_sec = (datetime.now() - ts).total_seconds()
                except Exception:
                    idle_sec = 9999
                if idle_sec < 60:
                    c.close()
                    logger.info(f"Lista id={row['id']} sem conexao mas recem-atribuida ({idle_sec:.0f}s) — aguardando")
                    continue
                c.execute(
                    "UPDATE m3u_lists SET status='available', session_ip=NULL, last_accessed=NULL "
                    "WHERE id=? AND status='in_use'",
                    (row["id"],),
                )
                c.execute(
                    "INSERT INTO access_log (ip_address, list_id, list_name, action) "
                    "VALUES (?,?,?,'released_no_conn')",
                    (row["session_ip"], row["id"], row["name"]),
                )
                c.commit()
                c.close()
                logger.info(f"Lista id={row['id']} liberada (active_cons=0, idle={idle_sec:.0f}s)")


def cleanup_expired_sessions():
    conn = get_db()
    threshold = datetime.now() - timedelta(minutes=SESSION_TIMEOUT)
    conn.execute("""
        UPDATE m3u_lists
        SET status='available', session_ip=NULL, last_accessed=NULL
        WHERE status='in_use' AND last_accessed < ?
    """, (threshold,))
    released = conn.total_changes
    conn.commit()
    conn.close()
    if released:
        logger.info(f"Released {released} expired session(s)")


def _gz(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"), compresslevel=6)


def _gunz(blob: bytes) -> str:
    return gzip.decompress(blob).decode("utf-8")


def rewrite_credentials(template: str, from_user: str, from_pass: str, to_user: str, to_pass: str) -> str:
    """Substitui credenciais Xtream dentro do conteudo M3U.

    Cobre tanto o formato de querystring (?username=...&password=...)
    quanto o formato de path (/live/USER/PASS/123.ts).
    """
    out = template
    # Querystring
    out = out.replace(f"username={from_user}", f"username={to_user}")
    out = out.replace(f"password={from_pass}", f"password={to_pass}")
    # Path-style: /USER/PASS/  (precisa de dois replaces posicionais)
    out = out.replace(f"/{from_user}/{from_pass}/", f"/{to_user}/{to_pass}/")
    return out


async def refresh_all_caches():
    """Busca UMA lista e gera o cache das demais por substituicao de credenciais.

    Todas as listas apontam para o mesmo provedor variando apenas
    usuario/senha — fazer um unico fetch e substituir e muito mais rapido
    e reduz carga no provedor.
    """
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, source_url, username, password, cached_content "
        "FROM m3u_lists WHERE source_url IS NOT NULL AND source_url != ''"
    ).fetchall()
    conn.close()

    if not rows:
        return

    # Escolhe uma lista com usuario/senha como "template"
    template_row = next((r for r in rows if r["username"] and r["password"]), None)

    if template_row is None:
        # Sem credenciais salvas — busca cada lista individualmente (fallback)
        for row in rows:
            try:
                async with httpx.AsyncClient(timeout=45, follow_redirects=True, headers=IPTV_HEADERS) as client:
                    resp = await client.get(row["source_url"])
                    resp.raise_for_status()
                    content = resp.text
                if _is_valid_m3u(content):
                    blob = _gz(content)
                    c = get_db()
                    c.execute(
                        "UPDATE m3u_lists SET cached_gzip=?, cached_at=? WHERE id=?",
                        (blob, datetime.now(), row["id"]),
                    )
                    c.commit()
                    c.close()
                    logger.info(f"Cache lista id={row['id']} atualizado ({len(content)} -> {len(blob)} bytes)")
            except Exception as exc:
                logger.error(f"Erro no refresh da lista id={row['id']}: {exc}")
        return

    # Busca o template uma unica vez
    try:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True, headers=IPTV_HEADERS) as client:
            resp = await client.get(template_row["source_url"])
            resp.raise_for_status()
            template_content = resp.text
    except Exception as exc:
        logger.error(f"Erro ao buscar template (lista id={template_row['id']}): {exc}")
        return

    if not _is_valid_m3u(template_content):
        logger.warning("Template retornou vazio — cache antigo preservado")
        return

    now = datetime.now()
    tpl_user = template_row["username"]
    tpl_pass = template_row["password"]

    for row in rows:
        if row["id"] == template_row["id"]:
            derived = template_content
        elif row["username"] and row["password"]:
            derived = rewrite_credentials(
                template_content, tpl_user, tpl_pass, row["username"], row["password"]
            )
        else:
            # Sem credenciais — pula (mantem cache antigo se houver)
            logger.warning(f"Lista id={row['id']} sem usuario/senha — cache nao derivado")
            continue

        blob = _gz(derived)
        c = get_db()
        c.execute(
            "UPDATE m3u_lists SET cached_gzip=?, cached_at=? WHERE id=?",
            (blob, now, row["id"]),
        )
        c.commit()
        c.close()
    logger.info(
        f"Cache refrescado via template (lista id={template_row['id']}, {len(template_content)} -> "
        f"~{len(_gz(template_content))} bytes gzip) para {len(rows)} lista(s)"
    )


# ---------- App lifecycle ----------

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler.add_job(cleanup_expired_sessions, "interval", minutes=CLEANUP_INTERVAL_MINUTES)
    scheduler.add_job(check_active_connections, "interval", seconds=ACTIVE_CHECK_SECONDS, max_instances=1)
    scheduler.add_job(refresh_all_caches, "interval", minutes=CACHE_REFRESH_MINUTES)
    scheduler.add_job(refresh_all_caches, "date", run_date=datetime.now() + timedelta(seconds=15))
    scheduler.start()
    logger.info("Scheduler started")
    yield
    scheduler.shutdown()


app = FastAPI(title="M3U Router", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=6)
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
    # expire admin session after 24h
    if datetime.now() - admin_sessions[admin_token] > timedelta(hours=24):
        del admin_sessions[admin_token]
        return False
    return True


IPTV_HEADERS = {
    "User-Agent": "VLC/3.0.20 LibVLC/3.0.20",
    "Accept": "*/*",
    "Connection": "keep-alive",
}

def _is_valid_m3u(text: str) -> bool:
    if not text or not text.strip():
        return False
    # Must have at least one real entry beyond the header
    return "#EXTINF" in text and len(text) > 100


PLAYLIST_HEADERS = {
    "Cache-Control": "public, max-age=300",
    "Content-Disposition": 'inline; filename="playlist.m3u"',
}


async def fetch_m3u_gzip(row, conn=None) -> bytes:
    """Retorna o conteudo M3U ja gzipado, pronto para servir."""
    row = dict(row)
    if not row.get("source_url"):
        manual = row.get("content") or "#EXTM3U\n"
        return _gz(manual)

    cached_blob = row.get("cached_gzip")
    cached_at = row.get("cached_at")
    if cached_blob and cached_at:
        try:
            ts = cached_at if isinstance(cached_at, datetime) else datetime.fromisoformat(str(cached_at))
            if datetime.now() - ts < timedelta(hours=CACHE_TTL_HOURS):
                logger.info(f"Cache hit lista id={row['id']} ({len(cached_blob)} bytes gzip)")
                return cached_blob
        except Exception:
            pass

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=IPTV_HEADERS) as client:
            resp = await client.get(row["source_url"])
            resp.raise_for_status()
            content = resp.text
            logger.info(f"Buscou {len(content)} bytes de {row['source_url']}")
            if _is_valid_m3u(content):
                blob = _gz(content)
                if conn is not None:
                    conn.execute(
                        "UPDATE m3u_lists SET cached_gzip=?, cached_at=? WHERE id=?",
                        (blob, datetime.now(), row["id"]),
                    )
                    conn.commit()
                return blob
            if cached_blob:
                logger.warning(f"Provedor vazio; servindo cache antigo da lista id={row['id']}")
                return cached_blob
            return _gz("#EXTM3U\n#EXTINF:-1,Lista vazia do provedor\nhttp://0.0.0.0\n")
    except Exception as exc:
        logger.error(f"Erro ao buscar M3U ({row.get('source_url')}): {exc}")
        if cached_blob:
            logger.warning(f"Erro no fetch; servindo cache antigo da lista id={row['id']}")
            return cached_blob
        return _gz(f"#EXTM3U\n#EXTINF:-1,Erro: {exc}\nhttp://0.0.0.0\n")


def playlist_response(blob: bytes, accept_encoding: str, status_code: int = 200) -> Response:
    headers = {**PLAYLIST_HEADERS}
    if "gzip" in (accept_encoding or "").lower():
        headers["Content-Encoding"] = "gzip"
        return Response(
            content=blob,
            media_type="audio/x-mpegurl",
            headers=headers,
            status_code=status_code,
        )
    # Cliente nao aceita gzip — descompacta
    return Response(
        content=gzip.decompress(blob),
        media_type="audio/x-mpegurl",
        headers=headers,
        status_code=status_code,
    )


# ---------- Public endpoint ----------


@app.get("/playlist.m3u", response_class=PlainTextResponse)
async def serve_playlist(request: Request):
    import time
    t0 = time.perf_counter()
    ip = get_client_ip(request)
    conn = get_db()

    def _log_timing(tag: str, size: int):
        ms = (time.perf_counter() - t0) * 1000
        logger.info(f"/playlist.m3u [{tag}] ip={ip} bytes={size} tempo={ms:.0f}ms")

    # Transacao exclusiva: serializa concorrencia para evitar atribuir
    # a mesma lista a dois IPs diferentes ao mesmo tempo.
    conn.execute("BEGIN IMMEDIATE")

    # Re-use existing session
    row = conn.execute(
        "SELECT * FROM m3u_lists WHERE status='in_use' AND session_ip=?", (ip,)
    ).fetchone()

    accept_enc = request.headers.get("accept-encoding", "")

    if row:
        conn.execute(
            "UPDATE m3u_lists SET last_accessed=? WHERE id=?",
            (datetime.now(), row["id"]),
        )
        conn.commit()
        blob = await fetch_m3u_gzip(row, conn)
        conn.close()
        _log_timing("reuse", len(blob))
        return playlist_response(blob, accept_enc)

    # Atribuicao atomica: UPDATE com WHERE status='available' garante
    # que apenas uma requisicao consiga marcar a lista como in_use.
    available = conn.execute(
        "SELECT * FROM m3u_lists WHERE status='available' ORDER BY RANDOM() LIMIT 1"
    ).fetchone()

    if not available:
        conn.rollback()
        conn.close()
        busy = _gz("#EXTM3U\n#EXTINF:-1,Todas as listas estao em uso. Tente novamente em alguns minutos.\nhttp://0.0.0.0\n")
        return playlist_response(busy, accept_enc, status_code=503)

    cur = conn.execute("""
        UPDATE m3u_lists
        SET status='in_use', session_ip=?, last_accessed=?
        WHERE id=? AND status='available'
    """, (ip, datetime.now(), available["id"]))

    if cur.rowcount == 0:
        conn.rollback()
        conn.close()
        logger.warning(f"Corrida detectada ao atribuir lista id={available['id']} para {ip}")
        retry = _gz("#EXTM3U\n#EXTINF:-1,Tente novamente em alguns segundos.\nhttp://0.0.0.0\n")
        return playlist_response(retry, accept_enc, status_code=503)
    conn.execute(
        "INSERT INTO access_log (ip_address, list_id, list_name, action) VALUES (?,?,?,'assigned')",
        (ip, available["id"], available["name"]),
    )
    conn.commit()
    blob = await fetch_m3u_gzip(available)
    conn.close()
    logger.info(f"List '{available['name']}' assigned to {ip}")
    _log_timing("assign", len(blob))
    return playlist_response(blob, accept_enc)


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
    raw_lists = conn.execute("SELECT * FROM m3u_lists ORDER BY id").fetchall()
    logs = conn.execute(
        "SELECT * FROM access_log ORDER BY timestamp DESC LIMIT 30"
    ).fetchall()
    conn.close()

    now_dt = datetime.now()
    lists = []
    for l in raw_lists:
        d = dict(l)
        cached_at = d.get("cached_at")
        age_min = None
        size_kb = None
        if cached_at:
            try:
                ts = cached_at if isinstance(cached_at, datetime) else datetime.fromisoformat(str(cached_at))
                age_min = int((now_dt - ts).total_seconds() // 60)
            except Exception:
                pass
        if d.get("cached_gzip"):
            size_kb = len(d["cached_gzip"]) // 1024
        d["cache_age_min"] = age_min
        d["cache_size_kb"] = size_kb
        lists.append(d)

    total = len(lists)
    available = sum(1 for l in lists if l["status"] == "available")
    in_use = total - available

    host = request.headers.get("host", "seu-dominio.com")
    scheme = "https" if request.headers.get("x-forwarded-proto") == "https" else "http"
    playlist_url = f"{scheme}://{host}/playlist.m3u"

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
    username: str = Form(default=""),
    password: str = Form(default=""),
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

    u = username.strip()
    p = password.strip()
    url = source_url.strip() or None
    body = content.strip() or None

    if u and p:
        url = build_provider_url(u, p)

    if not url and not body:
        conn.close()
        return RedirectResponse("/admin?msg=Informe+usuario+e+senha+ou+URL", status_code=302)

    conn.execute(
        "INSERT INTO m3u_lists (name, source_url, content, username, password) VALUES (?,?,?,?,?)",
        (name.strip(), url, body, u or None, p or None),
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
        "UPDATE m3u_lists SET status='available', session_ip=NULL, last_accessed=NULL WHERE id=?",
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
        "UPDATE m3u_lists SET status='available', session_ip=NULL, last_accessed=NULL WHERE status='in_use'"
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/admin?msg=Todas+as+sessoes+liberadas", status_code=302)
