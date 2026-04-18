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
ACTIVE_CHECK_SECONDS = int(os.getenv("ACTIVE_CHECK_SECONDS", "30"))
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


def build_provider_url(username: str, password: str) -> str:
    from urllib.parse import quote
    return PROVIDER_BASE.format(username=quote(username, safe=""), password=quote(password, safe=""))

admin_sessions: dict[str, datetime] = {}

# ---------- Database ----------

def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
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
    if "last_active_cons" not in existing:
        conn.execute("ALTER TABLE m3u_lists ADD COLUMN last_active_cons INTEGER")
    if "zero_streak" not in existing:
        conn.execute("ALTER TABLE m3u_lists ADD COLUMN zero_streak INTEGER DEFAULT 0")
    if "last_probe_at" not in existing:
        conn.execute("ALTER TABLE m3u_lists ADD COLUMN last_probe_at TIMESTAMP")
    # Libera espaco do cache antigo em texto
    conn.execute("UPDATE m3u_lists SET cached_content=NULL WHERE cached_content IS NOT NULL")
    # Indices
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lists_status ON m3u_lists(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lists_session_ip ON m3u_lists(session_ip)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_timestamp ON access_log(timestamp)")
    conn.commit()
    conn.close()
    logger.info("Database initialised")


def prune_access_log():
    conn = get_db()
    cur = conn.execute(
        "DELETE FROM access_log WHERE timestamp < datetime('now', '-30 days')"
    )
    removed = cur.rowcount
    conn.commit()
    conn.close()
    if removed:
        logger.info(f"Podados {removed} registros antigos de access_log")


ZERO_STREAK_TO_RELEASE = int(os.getenv("ZERO_STREAK_TO_RELEASE", "2"))


async def probe_provider(username: str, password: str) -> dict:
    """Chama player_api.php e retorna dict com 'ok', 'active', 'raw'."""
    from urllib.parse import quote
    url = PROVIDER_API_BASE.format(
        username=quote(username, safe=""),
        password=quote(password, safe=""),
    )
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True, headers=IPTV_HEADERS) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        return {"ok": False, "error": str(exc), "active": None, "raw": None, "url": url}

    user_info = data.get("user_info", {}) if isinstance(data, dict) else {}
    active_raw = user_info.get("active_cons", 0)
    try:
        active = int(active_raw)
    except (TypeError, ValueError):
        active = 0
    return {"ok": True, "active": active, "raw": user_info, "url": url}


PROBE_CONCURRENCY = int(os.getenv("PROBE_CONCURRENCY", "5"))


async def check_active_connections():
    """Consulta player_api.php em paralelo e aplica transicoes.

    - Lista 'available' com active_cons > 0 vira 'in_use' (session_ip=NULL)
      para ser reivindicada pelo primeiro IP que chamar /playlist.m3u.
    - Lista 'in_use' so libera apos ZERO_STREAK_TO_RELEASE checagens
      consecutivas com active_cons=0.
    """
    import asyncio
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, status, username, password, session_ip, last_accessed, zero_streak "
        "FROM m3u_lists WHERE username IS NOT NULL AND password IS NOT NULL"
    ).fetchall()
    conn.close()

    if not rows:
        return

    sem = asyncio.Semaphore(PROBE_CONCURRENCY)

    async def probe_with_sem(r):
        async with sem:
            return r, await probe_provider(r["username"], r["password"])

    results = await asyncio.gather(*(probe_with_sem(r) for r in rows))

    now = datetime.now()
    conn = get_db()
    try:
        for row, result in results:
            if row["status"] == "available":
                if result["ok"] and (result["active"] or 0) > 0:
                    cur = conn.execute(
                        "UPDATE m3u_lists SET status='in_use', session_ip=NULL, "
                        "last_accessed=?, last_active_cons=?, zero_streak=0, last_probe_at=? "
                        "WHERE id=? AND status='available'",
                        (now, result["active"], now, row["id"]),
                    )
                    if cur.rowcount:
                        conn.execute(
                            "INSERT INTO access_log (ip_address, list_id, list_name, action) "
                            "VALUES (?,?,?,'auto_assigned')",
                            (None, row["id"], row["name"]),
                        )
                        logger.info(f"Lista id={row['id']} auto-atribuida (active_cons={result['active']})")
                elif result["ok"]:
                    conn.execute(
                        "UPDATE m3u_lists SET last_active_cons=?, last_probe_at=? WHERE id=?",
                        (result["active"], now, row["id"]),
                    )
                continue

            # status == 'in_use'
            if not result["ok"]:
                conn.execute(
                    "UPDATE m3u_lists SET last_probe_at=? WHERE id=?",
                    (now, row["id"]),
                )
                logger.warning(f"player_api falhou lista id={row['id']}: {result.get('error')} — mantendo sessao")
                continue

            active = result["active"]
            if active > 0:
                conn.execute(
                    "UPDATE m3u_lists SET last_accessed=?, last_active_cons=?, "
                    "zero_streak=0, last_probe_at=? WHERE id=? AND status='in_use'",
                    (now, active, now, row["id"]),
                )
                continue

            # active == 0
            last = row["last_accessed"]
            try:
                ts = last if isinstance(last, datetime) else datetime.fromisoformat(str(last))
                idle_sec = (now - ts).total_seconds()
            except Exception:
                idle_sec = 9999
            if idle_sec < 60:
                conn.execute(
                    "UPDATE m3u_lists SET last_active_cons=0, last_probe_at=? WHERE id=?",
                    (now, row["id"]),
                )
                continue

            streak = (row["zero_streak"] or 0) + 1
            if streak < ZERO_STREAK_TO_RELEASE:
                conn.execute(
                    "UPDATE m3u_lists SET zero_streak=?, last_active_cons=0, last_probe_at=? WHERE id=?",
                    (streak, now, row["id"]),
                )
                continue

            conn.execute(
                "UPDATE m3u_lists SET status='available', session_ip=NULL, last_accessed=NULL, "
                "zero_streak=0, last_active_cons=0, last_probe_at=? "
                "WHERE id=? AND status='in_use'",
                (now, row["id"]),
            )
            conn.execute(
                "INSERT INTO access_log (ip_address, list_id, list_name, action) "
                "VALUES (?,?,?,'released_no_conn')",
                (row["session_ip"], row["id"], row["name"]),
            )
            logger.info(f"Lista id={row['id']} liberada ({streak} checagens zeradas, idle={idle_sec:.0f}s)")
        conn.commit()
    finally:
        conn.close()


def cleanup_expired_sessions():
    conn = get_db()
    threshold = datetime.now() - timedelta(minutes=SESSION_TIMEOUT)
    cur = conn.execute("""
        UPDATE m3u_lists
        SET status='available', session_ip=NULL, last_accessed=NULL
        WHERE status='in_use' AND last_accessed < ?
    """, (threshold,))
    released = cur.rowcount
    conn.commit()
    conn.close()
    if released:
        logger.info(f"Released {released} expired session(s)")


def _gz(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"), compresslevel=6)


def _gunz(blob: bytes) -> str:
    return gzip.decompress(blob).decode("utf-8")


def _cred_variants(value: str) -> list[str]:
    """Retorna formas que o provedor pode usar no M3U (crua e url-encoded)."""
    from urllib.parse import quote, unquote
    variants = {value, quote(value, safe=""), unquote(value)}
    # Mais longas primeiro para evitar substituicao parcial
    return sorted((v for v in variants if v), key=len, reverse=True)


def rewrite_credentials(template: str, from_user: str, from_pass: str, to_user: str, to_pass: str) -> str:
    """Substitui credenciais Xtream no M3U.

    Cobre querystring (?username=...&password=...) e path (/USER/PASS/),
    em formas crua e url-encoded, com placeholders para evitar que a
    substituicao de username pegue pedacos da password.
    """
    import re
    from urllib.parse import quote
    out = template
    U_PH = "\x00U_PLACEHOLDER\x00"
    P_PH = "\x00P_PLACEHOLDER\x00"

    for u in _cred_variants(from_user):
        out = re.sub(rf"username={re.escape(u)}\b", f"username={U_PH}", out)
    for p in _cred_variants(from_pass):
        out = re.sub(rf"password={re.escape(p)}\b", f"password={P_PH}", out)

    # Path style: /USER/PASS/
    for u in _cred_variants(from_user):
        for p in _cred_variants(from_pass):
            out = out.replace(f"/{u}/{p}/", f"/{U_PH}/{P_PH}/")

    out = out.replace(U_PH, quote(to_user, safe=""))
    out = out.replace(P_PH, quote(to_pass, safe=""))
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
    scheduler.add_job(prune_access_log, "interval", hours=24)
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

    import time
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=IPTV_HEADERS) as client:
            resp = await client.get(row["source_url"])
            resp.raise_for_status()
            content = resp.text
            fetch_ms = (time.perf_counter() - t0) * 1000
            logger.info(f"Buscou {len(content)} bytes de {row['source_url']} em {fetch_ms:.0f}ms")
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


FORCE_GZIP = os.getenv("FORCE_GZIP", "1") == "1"


def playlist_response(blob: bytes, accept_encoding: str, user_agent: str = "", status_code: int = 200) -> Response:
    headers = {**PLAYLIST_HEADERS}
    accepts_gzip = "gzip" in (accept_encoding or "").lower()
    # Varios players (Ibo, Smarters, VLC) suportam gzip mesmo sem
    # anunciar Accept-Encoding. Servimos gzip por padrao pois reduz
    # a resposta em ~10x — muito mais rapido em smart TVs.
    if accepts_gzip or FORCE_GZIP:
        headers["Content-Encoding"] = "gzip"
        headers["Content-Length"] = str(len(blob))
        return Response(
            content=blob,
            media_type="audio/x-mpegurl",
            headers=headers,
            status_code=status_code,
        )
    raw = gzip.decompress(blob)
    headers["Content-Length"] = str(len(raw))
    return Response(
        content=raw,
        media_type="audio/x-mpegurl",
        headers=headers,
        status_code=status_code,
    )


# ---------- Public endpoint ----------


@app.get("/health")
async def health():
    try:
        conn = get_db()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return {"ok": True}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/playlist.m3u", response_class=PlainTextResponse)
async def serve_playlist(request: Request):
    import time
    t0 = time.perf_counter()
    ip = get_client_ip(request)
    ua = request.headers.get("user-agent", "")
    accept_enc = request.headers.get("accept-encoding", "")

    def _log_timing(tag: str, size: int, fetch_ms: float | None = None):
        ms = (time.perf_counter() - t0) * 1000
        extra = f" fetch={fetch_ms:.0f}ms" if fetch_ms is not None else ""
        logger.info(f"/playlist.m3u [{tag}] ip={ip} ua={ua[:40]!r} bytes={size} total={ms:.0f}ms{extra}")

    conn = get_db()
    row = None
    tag = None
    try:
        # Transacao curta: so atribuicao, sem fetch HTTP dentro.
        conn.execute("BEGIN IMMEDIATE")

        # 1) Reuso por IP
        row = conn.execute(
            "SELECT * FROM m3u_lists WHERE status='in_use' AND session_ip=?", (ip,)
        ).fetchone()

        if row:
            conn.execute(
                "UPDATE m3u_lists SET last_accessed=? WHERE id=?",
                (datetime.now(), row["id"]),
            )
            conn.commit()
            tag = "reuse"
        else:
            # 2) Reivindica lista auto-atribuida (session_ip IS NULL)
            orphan = conn.execute(
                "SELECT * FROM m3u_lists WHERE status='in_use' AND session_ip IS NULL LIMIT 1"
            ).fetchone()
            if orphan:
                cur = conn.execute(
                    "UPDATE m3u_lists SET session_ip=?, last_accessed=? "
                    "WHERE id=? AND status='in_use' AND session_ip IS NULL",
                    (ip, datetime.now(), orphan["id"]),
                )
                if cur.rowcount:
                    conn.execute(
                        "INSERT INTO access_log (ip_address, list_id, list_name, action) VALUES (?,?,?,'claimed')",
                        (ip, orphan["id"], orphan["name"]),
                    )
                    conn.commit()
                    row = orphan
                    tag = "claim"

            if row is None:
                # 3) Atribuicao atomica de lista available
                available = conn.execute(
                    "SELECT * FROM m3u_lists WHERE status='available' ORDER BY RANDOM() LIMIT 1"
                ).fetchone()
                if not available:
                    conn.rollback()
                    busy = _gz("#EXTM3U\n#EXTINF:-1,Todas as listas estao em uso. Tente novamente em alguns minutos.\nhttp://0.0.0.0\n")
                    return playlist_response(busy, accept_enc, status_code=503)
                cur = conn.execute("""
                    UPDATE m3u_lists
                    SET status='in_use', session_ip=?, last_accessed=?, zero_streak=0, last_active_cons=NULL
                    WHERE id=? AND status='available'
                """, (ip, datetime.now(), available["id"]))
                if cur.rowcount == 0:
                    conn.rollback()
                    logger.warning(f"Corrida ao atribuir lista id={available['id']} para {ip}")
                    retry = _gz("#EXTM3U\n#EXTINF:-1,Tente novamente em alguns segundos.\nhttp://0.0.0.0\n")
                    return playlist_response(retry, accept_enc, status_code=503)
                conn.execute(
                    "INSERT INTO access_log (ip_address, list_id, list_name, action) VALUES (?,?,?,'assigned')",
                    (ip, available["id"], available["name"]),
                )
                conn.commit()
                row = available
                tag = "assign"
                logger.info(f"List '{available['name']}' assigned to {ip}")
    finally:
        conn.close()

    # Fetch FORA da transacao — cache miss nao trava outros IPs.
    fetch_t0 = time.perf_counter()
    conn2 = get_db()
    try:
        blob = await fetch_m3u_gzip(row, conn2)
    finally:
        conn2.close()
    fetch_ms = (time.perf_counter() - fetch_t0) * 1000
    _log_timing(tag, len(blob), fetch_ms)
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

        probe_at = d.get("last_probe_at")
        probe_age_sec = None
        if probe_at:
            try:
                ts = probe_at if isinstance(probe_at, datetime) else datetime.fromisoformat(str(probe_at))
                probe_age_sec = int((now_dt - ts).total_seconds())
            except Exception:
                pass
        d["probe_age_sec"] = probe_age_sec
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


@app.get("/admin/probe")
async def admin_probe(admin_token: str = Cookie(default=None)):
    if not is_admin(admin_token):
        return RedirectResponse("/admin/login", status_code=302)
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, username, password FROM m3u_lists WHERE username IS NOT NULL AND password IS NOT NULL"
    ).fetchall()
    conn.close()
    out = []
    for r in rows:
        result = await probe_provider(r["username"], r["password"])
        out.append({
            "id": r["id"],
            "name": r["name"],
            "ok": result["ok"],
            "active_cons": result.get("active"),
            "error": result.get("error"),
            "url": result.get("url"),
            "user_info": result.get("raw"),
        })
    return out


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
