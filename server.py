"""
Microservicio de scraping para la web USA (Walmart, Target).

Gemelo del scraper español (github.com/seocracks/nutribot-scraper), adaptado a
las cadenas de EE. UU. Resuelve el problema que el cliente PHP no puede:
RedSky (Target) y Walmart rechazan las peticiones por FINGERPRINT TLS (Akamai /
PerimeterX) aunque la IP sea de EE. UU. Scrapling combina el TLS de Chrome
(curl_cffi `impersonate`) con la IP residencial (DataImpulse país-US) → pasan.

Kroger NO pasa por aquí: tiene API oficial y se consume directo desde PHP.

Endpoints:
  POST /scrape        — scrapea el precio de una URL de producto (Target/Walmart)
  POST /scrape-batch  — N URLs en paralelo
  POST /fetch-json    — GET crudo de un endpoint JSON (RedSky search/pdp) con TLS Chrome
  POST /fetch-html    — GET de una página con fallback a browser real (Walmart PDP)
  GET  /health        — healthcheck (sin auth)

Autenticación: header `Authorization: Bearer <SCRAPER_API_KEY>`.
Configuración: variables de entorno (ver README).
"""
import os
import re
import sys
import time
import json
import signal
import base64
import random
import logging
import asyncio
import subprocess
from html import unescape
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Header, status
from pydantic import BaseModel, Field

# ─── Configuración ──────────────────────────────────────────────
API_KEY    = os.environ.get("SCRAPER_API_KEY", "")
PROXY_URL  = os.environ.get("PROXY_URL", "")          # DataImpulse país-US
DEFAULT_FETCHER = os.environ.get("DEFAULT_FETCHER", "fetcher")  # 'fetcher' (rápido) o 'stealthy'
MAX_CONCURRENT  = int(os.environ.get("MAX_CONCURRENT", "2"))
# Target RedSky (la key pública del site; store/zip de referencia = Columbus OH)
TARGET_API_KEY  = os.environ.get("TARGET_API_KEY", "9f36aeafbe60771e321a7cc95a78140772ab3e96")
TARGET_STORE_ID = os.environ.get("TARGET_STORE_ID", "1058")
TARGET_ZIP      = os.environ.get("TARGET_ZIP", "43215")
# Throttling — delay aleatorio entre requests (segundos).
THROTTLE_MIN = float(os.environ.get("THROTTLE_MIN_S", "1.0"))
THROTTLE_MAX = float(os.environ.get("THROTTLE_MAX_S", "3.0"))
LOG_LEVEL  = os.environ.get("LOG_LEVEL", "INFO").upper()

if not API_KEY:
    raise RuntimeError("Falta la variable de entorno SCRAPER_API_KEY")
if not PROXY_URL:
    raise RuntimeError("Falta la variable de entorno PROXY_URL")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scraper-usa")
logging.getLogger("scrapling").setLevel(logging.WARNING)


# ─── Modelos Pydantic ───────────────────────────────────────────
class ScrapeRequest(BaseModel):
    url: str = Field(..., min_length=10)
    fetcher: Optional[str] = Field(None, description="'fetcher' (rápido) o 'stealthy' (browser real)")
    timeout: int = Field(60, ge=5, le=300)


class ScrapeBatchRequest(BaseModel):
    items: List[dict] = Field(..., description="Lista de {id, url}")
    fetcher: Optional[str] = None
    timeout: int = Field(60, ge=5, le=300)


class FetchRequest(BaseModel):
    url: str = Field(..., min_length=10, description="URL JSON/HTML a buscar con TLS de Chrome + proxy US")
    timeout: int = Field(60, ge=5, le=300)


class ScrapeResult(BaseModel):
    id: Optional[int | str] = None
    url: str
    http_code: int
    time_ms: int
    size_bytes: int = 0
    # Precio total que paga el cliente, en CENTAVOS (148 = $1.48)
    precio_centimos: Optional[int] = None
    # Cantidad/formato de venta — ej "1 gal", "12 fl oz", "40 oz"
    unidad_venta: Optional[str] = None
    # Precio comparativo ($/oz, $/lb, $/fl oz) en centavos
    precio_ref_centimos: Optional[int] = None
    precio_ref_unidad: Optional[str] = None
    # Código de barras (UPC/GTIN) — clave del join con USDA en PHP
    upc: Optional[str] = None
    es_ficha: bool = False
    error: Optional[str] = None


# ─── Lógica de scraping ─────────────────────────────────────────
_executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT)


def _precio_a_centimos(s) -> Optional[int]:
    """'1.48' → 148. 3.59 → 359. None si no se puede."""
    if s is None or s == "":
        return None
    try:
        return int(round(float(str(s).replace(",", ".").strip()) * 100))
    except (ValueError, TypeError):
        return None


def _detectar_super(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if "target.com" in host:        return "target"
    if "walmart.com" in host:       return "walmart"
    if "kroger.com" in host:        return "kroger"
    return "desconocido"


# Whitelist de hosts (anti-SSRF): solo endpoints de datos de las cadenas USA.
_FETCH_JSON_HOSTS = {"redsky.target.com", "r2d2.target.com"}
_FETCH_HTML_HOSTS = {"www.target.com", "target.com", "www.walmart.com", "walmart.com"}

# Dominios bloqueados con el browser real (ahorra ancho de banda del proxy US,
# mismo criterio conservador que el scraper ES: solo tracking/ads/widgets).
_BLOCKED_DOMAINS = {
    "googletagmanager.com", "google-analytics.com", "googlesyndication.com",
    "doubleclick.net", "bat.bing.com", "scorecardresearch.com",
    "adsrvr.org", "criteo.com", "quantummetric.com", "tvpixel.com",
    "youtube.com", "ytimg.com", "cloudflareinsights.com",
    "branch.io", "tealiumiq.com",
}


# ─── BLINDAJE (2026-07-22): Camoufox en subproceso con matadero duro ───
# PROBLEMA medido: con ThreadPoolExecutor, un StealthyFetcher.fetch() que se
# cuelga POR ENCIMA de su timeout (Firefox zombi en el lanzamiento o CONNECT del
# proxy, algo que el timeout de navegación NO cubre) deja el HILO atascado para
# siempre — y en Python un hilo no se puede matar. Tras una carga larga los 2
# workers acabaron zombis, reteniendo los 2 slots del semáforo; /health seguía
# OK (es async, no usa el executor) mientras el scrape estaba muerto. La cola de
# Target quedó 15h con 0 éxitos.
#
# FIX: cada scrape con browser real corre en un SUBPROCESO fresco (`python -c`,
# intérprete nuevo → sin fork-hazards con el server async ni acumulación de
# estado). Con start_new_session=True el hijo es líder de grupo, así que si se
# cuelga se mata el GRUPO ENTERO (hijo + Firefox nietos) con os.killpg → el slot
# se libera SIEMPRE dentro de `hard` segundos. El proxy va por stdin (no en argv:
# no se ve en `ps`).
_STEALTHY_WORKER = r'''
import sys, json, base64
cfg = json.loads(sys.stdin.read())
try:
    from scrapling.fetchers import StealthyFetcher
    page = StealthyFetcher.fetch(
        cfg["url"], headless=True, proxy=cfg["proxy"],
        solve_cloudflare=False, wait=2500,
        timeout=cfg["timeout"] * 1000, blocked_domains=cfg.get("blocked"),
    )
    html = page.html_content or ""
    st = getattr(page, "status", 0) or 200
    sys.stdout.write("RESULT:" + json.dumps(
        {"status": st, "html_b64": base64.b64encode(html.encode("utf-8", "ignore")).decode()}))
except Exception as e:
    sys.stdout.write("RESULT:" + json.dumps({"error": type(e).__name__ + ": " + str(e)}))
'''


def _stealthy_fetch_hard(url: str, timeout: int, blocked) -> tuple[int, str]:
    """Camoufox en subproceso con timeout duro + kill de grupo. Nunca cuelga el
    hilo llamante más de `timeout + 12` s."""
    cfg = json.dumps({"url": url, "proxy": PROXY_URL, "timeout": timeout, "blocked": blocked})
    hard = timeout + 12
    proc = subprocess.Popen(
        [sys.executable, "-c", _STEALTHY_WORKER],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, start_new_session=True,
    )
    try:
        out, err = proc.communicate(input=cfg, timeout=hard)
    except subprocess.TimeoutExpired:
        # Colgado → matar TODO el grupo (hijo + Firefox nietos) para no dejar zombis.
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            proc.kill()
        try:
            proc.communicate(timeout=5)
        except Exception:
            pass
        raise TimeoutError(f"stealthy hard-timeout tras {hard}s (subproceso matado)")

    i = (out or "").rfind("RESULT:")
    if i < 0:
        raise RuntimeError("stealthy sin resultado: " + ((err or out or "")[:200]))
    data = json.loads(out[i + 7:])
    if "error" in data:
        raise RuntimeError(data["error"])
    return int(data.get("status") or 200), base64.b64decode(data["html_b64"]).decode("utf-8", "ignore")


def _http_get(url: str, fetcher: str, timeout: int, accept_json: bool = False) -> tuple[int, str]:
    """GET con Scrapling, devuelve (status, body). Proxy US SIEMPRE (Target/Walmart
    rechazan IPs no-US). 'fetcher' = curl_cffi TLS-Chrome (rápido); 'stealthy' =
    Camoufox (para PerimeterX de Walmart)."""
    from scrapling.fetchers import Fetcher

    headers = {
        "Accept": ("application/json,text/plain,*/*" if accept_json
                   else "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"),
        "Accept-Language": "en-US,en;q=0.9",
    }

    if fetcher == "stealthy" and not accept_json:
        super_id = _detectar_super(url)
        blocked = _BLOCKED_DOMAINS if super_id in ("walmart", "target") else None
        # Camoufox aislado en subproceso con matadero duro (ver _stealthy_fetch_hard).
        return _stealthy_fetch_hard(url, timeout, blocked)

    page = Fetcher.get(url, proxy=PROXY_URL, impersonate="chrome", timeout=timeout, headers=headers)
    body = page.body
    text = body.decode("utf-8", errors="ignore") if isinstance(body, bytes) else (body or "")
    return page.status, text


# ─── Extractor: Target (RedSky) ─────────────────────────────────
def _tcin_de_url(url: str) -> Optional[str]:
    """Extrae el tcin de una URL de producto Target (/p/…/A-<tcin> o ?tcin=)."""
    m = re.search(r"/A-(\d+)", url) or re.search(r"[?&]tcin=(\d+)", url)
    return m.group(1) if m else None


def _normalizar_item_target(item: dict, price_block: dict) -> dict:
    """Convierte un item RedSky (mismo shape en search y pdp) al modelo del PHP."""
    desc = ((item.get("product_description") or {}).get("title") or "").strip()
    upc = re.sub(r"\D", "", str(item.get("primary_barcode") or ""))

    retail = price_block.get("current_retail")
    if retail is None:
        retail = price_block.get("current_retail_min")
    precio = _precio_a_centimos(retail)

    # Tamaño: al final del título ("… - 32oz") o en package/handling
    unidad = None
    m = re.search(r"-\s*([\d.\/]+\s*(?:fl\s*oz|oz|lb|lbs|gal|qt|pt|ct|pk|each|ea))\s*$", desc, re.IGNORECASE)
    if m:
        unidad = m.group(1).strip()

    return {
        "precio_centimos": precio,
        "unidad_venta": unidad,
        "precio_ref_centimos": None,
        "precio_ref_unidad": None,
        "upc": upc or None,
        "es_ficha": precio is not None,
    }


def _json_de_respuesta(body: str) -> str:
    """Extrae el JSON de la respuesta de RedSky vía Camoufox. El navegador real
    envuelve el JSON en <html><body><pre>{...}</pre></body></html>; el fetcher
    rápido lo devuelve crudo. Cubre ambos casos."""
    body = (body or "").strip()
    if body.startswith("{") or body.startswith("["):
        return body
    m = re.search(r"<pre[^>]*>(.*?)</pre>", body, re.DOTALL | re.IGNORECASE)
    if m:
        return unescape(m.group(1).strip())
    # Fallback: del primer { al último } (por si el <pre> trae atributos raros).
    i, j = body.find("{"), body.rfind("}")
    if i != -1 and j > i:
        return body[i:j + 1]
    return body


def _redsky_get_json(url: str, timeout: int, intentos: int = 3) -> tuple[Optional[str], str]:
    """GET a RedSky con el navegador real (Camoufox). Akamai sirve un CAPTCHA al
    fetcher rápido (curl_cffi impersonate) pero deja pasar al browser real, que
    devuelve el JSON envuelto en <pre> (verificado 2026-07-19: fetcher→403 captcha,
    stealthy→JSON válido). Se IGNORA el status HTTP (RedSky puede dar 403 con el
    JSON correcto en el body) y se valida que el body extraído sea JSON parseable.

    REINTENTOS: el proxy DataImpulse es rotativo y Akamai bloquea según la IP
    concreta que toque (medido: ~1 de cada 3 intentos pasa). Cada reintento sale
    por una IP distinta → con 3 intentos la tasa de éxito sube a >90 %.

    Devuelve (json_str, error). json_str None si no se pudo extraer JSON."""
    ultimo_err = ""
    for i in range(max(1, intentos)):
        try:
            _, html_body = _http_get(url, "stealthy", timeout, accept_json=False)
        except Exception as e:
            ultimo_err = f"{type(e).__name__}: {e}"
            continue
        js = _json_de_respuesta(html_body)
        try:
            json.loads(js)
            return js, ""
        except Exception:
            ultimo_err = f"captcha/no-JSON ({len(html_body or '')}B)"
            log.info("RedSky intento %d/%d sin JSON (IP bloqueada), reintentando", i + 1, intentos)
    return None, ultimo_err


def _scrape_target(url: str, timeout: int) -> dict:
    """Target: RedSky pdp_client_v1 por tcin (precio de la tienda de referencia)."""
    tcin = _tcin_de_url(url)
    if not tcin:
        return {"http_code": 400, "size_bytes": 0, "es_ficha": False,
                "error": "URL Target sin tcin detectable"}

    pdp = (
        "https://redsky.target.com/redsky_aggregations/v1/web/pdp_client_v1"
        f"?key={TARGET_API_KEY}&tcin={tcin}&is_bot=false&channel=WEB"
        f"&pricing_store_id={TARGET_STORE_ID}&store_id={TARGET_STORE_ID}&zip={TARGET_ZIP}"
    )
    js, err = _redsky_get_json(pdp, timeout)
    if err or js is None:
        return {"http_code": 502, "size_bytes": 0, "es_ficha": False,
                "error": f"RedSky pdp: {err}"}
    data = json.loads(js)

    product = ((data.get("data") or {}).get("product")) or {}
    item = product.get("item") or {}
    price_block = product.get("price") or {}
    r = _normalizar_item_target(item, price_block)
    r["http_code"] = 200
    r["size_bytes"] = len(js)
    return r


# ─── Extractor: Walmart (__NEXT_DATA__) ─────────────────────────
# EXPERIMENTAL: Walmart usa PerimeterX (bot-management agresivo). La vía rápida
# (curl_cffi) suele bastar para el JSON embebido; si devuelve el reto, cae al
# browser real. Pendiente de validar en el VPS con proxy US residencial cuando
# se aborde Walmart (hoy el catálogo USA va con Kroger + Target).
def _scrape_walmart(html: str, status_code: int) -> dict:
    precio_centimos = None
    unidad_venta = None
    upc = None
    es_ficha = False

    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>', html, re.DOTALL)
    if m:
        try:
            data = json.loads(m.group(1))
            product = (((data.get("props") or {}).get("pageProps") or {})
                       .get("initialData") or {}).get("data", {}).get("product") or {}
            if product:
                es_ficha = True
                pmap = product.get("priceInfo") or {}
                cur = (pmap.get("currentPrice") or {}).get("price")
                precio_centimos = _precio_a_centimos(cur)
                upc = re.sub(r"\D", "", str(product.get("upc") or "")) or None
        except Exception:
            pass

    # Fallback regex al JSON-LD si no hubo __NEXT_DATA__ útil
    if precio_centimos is None:
        m = re.search(r'"price"\s*:\s*"?(\d+(?:\.\d{1,2})?)"?', html)
        if m:
            precio_centimos = _precio_a_centimos(m.group(1))
            es_ficha = es_ficha or precio_centimos is not None

    return {
        "http_code": status_code,
        "size_bytes": len(html),
        "precio_centimos": precio_centimos,
        "unidad_venta": unidad_venta,
        "precio_ref_centimos": None,
        "precio_ref_unidad": None,
        "upc": upc,
        "es_ficha": es_ficha,
    }


def _scrape_blocking(url: str, fetcher: str, timeout: int) -> dict:
    """Scrapea una URL síncronamente. Detecta cadena y aplica extractor."""
    t0 = time.time()
    base = {"url": url, "es_ficha": False}

    if THROTTLE_MIN > 0:
        time.sleep(random.uniform(THROTTLE_MIN, max(THROTTLE_MIN, THROTTLE_MAX)))

    try:
        super_id = _detectar_super(url)
        log.info("Scrape %s — super=%s fetcher=%s", url[:80], super_id, fetcher)

        if super_id == "target":
            r = _scrape_target(url, timeout)
        elif super_id == "walmart":
            status_code, html = _http_get(url, fetcher, timeout)
            r = _scrape_walmart(html, status_code)
        else:
            base.update({"http_code": 0, "time_ms": int((time.time() - t0) * 1000),
                         "size_bytes": 0, "error": f"Cadena no soportada: {super_id}"})
            return base

        base.update(r)
        base["time_ms"] = int((time.time() - t0) * 1000)
        return base
    except Exception as e:
        log.warning("Error scrapeando %s: %s", url, e)
        base.update({"http_code": 0, "time_ms": int((time.time() - t0) * 1000),
                     "size_bytes": 0, "error": f"{type(e).__name__}: {e}"})
        return base


def _fetch_json_blocking(url: str, timeout: int) -> tuple[int, str, str]:
    """GET de un endpoint JSON whitelisted de RedSky (search/pdp) con el navegador
    real (Camoufox) — el fetcher rápido recibe CAPTCHA de Akamai. Devuelve
    (status, json_str, error). Es lo que desbloquea el import de Target: el PHP
    arma la URL de RedSky y aquí se obtiene el JSON pasando el bot-management."""
    host = urlparse(url).netloc.lower()
    if host not in _FETCH_JSON_HOSTS:
        return 400, "", f"host no permitido: {host}"
    js, err = _redsky_get_json(url, timeout)
    if err or js is None:
        return 502, "", err
    return 200, js, ""


def _fetch_html_blocking(url: str, timeout: int) -> tuple[int, str, str]:
    """GET de una página whitelisted con fallback a browser real (Walmart PDP)."""
    host = urlparse(url).netloc.lower()
    if host not in _FETCH_HTML_HOSTS:
        return 400, "", f"host no permitido: {host}"
    err_fast = ""
    try:
        status_code, html = _http_get(url, "fetcher", timeout)
        if status_code == 200 and html and "px-captcha" not in html.lower() and "__NEXT_DATA__" in html:
            return status_code, html, ""
    except Exception as e:
        err_fast = f"{type(e).__name__}: {e}"
    try:
        status_code, html = _http_get(url, "stealthy", timeout)
        return status_code, html, ""
    except Exception as e:
        return 0, "", f"{err_fast} | stealthy: {type(e).__name__}: {e}".strip(" |")


# ─── App FastAPI ────────────────────────────────────────────────
app = FastAPI(
    title="Nutribot USA scraper",
    description="Microservicio interno de scraping para la web USA (Walmart, Target)",
    version="1.0.0",
)


def _check_auth(authorization: Optional[str]) -> None:
    if not authorization or authorization != f"Bearer {API_KEY}":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token inválido")


@app.get("/health")
async def health():
    return {"status": "ok", "default_fetcher": DEFAULT_FETCHER, "max_concurrent": MAX_CONCURRENT}


@app.post("/scrape", response_model=ScrapeResult)
async def scrape(req: ScrapeRequest, authorization: Optional[str] = Header(None)):
    _check_auth(authorization)
    fetcher = req.fetcher or DEFAULT_FETCHER
    if fetcher not in ("fetcher", "stealthy"):
        raise HTTPException(400, "fetcher debe ser 'fetcher' o 'stealthy'")
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _scrape_blocking, req.url, fetcher, req.timeout)


@app.post("/scrape-batch", response_model=List[ScrapeResult])
async def scrape_batch(req: ScrapeBatchRequest, authorization: Optional[str] = Header(None)):
    _check_auth(authorization)
    fetcher = req.fetcher or DEFAULT_FETCHER
    if fetcher not in ("fetcher", "stealthy"):
        raise HTTPException(400, "fetcher debe ser 'fetcher' o 'stealthy'")
    loop = asyncio.get_event_loop()

    async def _do_one(item):
        url = item.get("url", "")
        if not url:
            return {"id": item.get("id"), "url": "", "http_code": 0, "time_ms": 0,
                    "error": "URL vacía", "es_ficha": False}
        result = await loop.run_in_executor(_executor, _scrape_blocking, url, fetcher, req.timeout)
        result["id"] = item.get("id")
        return result

    return await asyncio.gather(*[_do_one(it) for it in req.items])


@app.post("/debug-egress")
async def debug_egress(authorization: Optional[str] = Header(None)):
    """Diagnóstico: IP de salida CON proxy y SIN proxy, para confirmar que el
    proxy US está bien configurado en el contenedor. No expone la password."""
    _check_auth(authorization)
    from scrapling.fetchers import Fetcher

    def _probe():
        out = {
            "proxy_configured": bool(PROXY_URL),
            "proxy_host": urlparse(PROXY_URL).hostname if PROXY_URL else None,
            "proxy_user": (urlparse(PROXY_URL).username or "")[:12] if PROXY_URL else None,
        }
        for etiqueta, usar_proxy in (("via_proxy", True), ("direct", False)):
            try:
                p = Fetcher.get(
                    "https://api.ipify.org?format=json",
                    proxy=(PROXY_URL if usar_proxy else None),
                    impersonate="chrome", timeout=30,
                )
                body = p.body.decode("utf-8", "ignore") if isinstance(p.body, bytes) else (p.body or "")
                out[etiqueta] = {"status": getattr(p, "status", 0), "body": body[:120]}
            except Exception as e:
                out[etiqueta] = {"error": f"{type(e).__name__}: {e}"}
        return out

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _probe)


@app.post("/debug-fetch")
async def debug_fetch(req: FetchRequest, authorization: Optional[str] = Header(None)):
    """Diagnóstico: busca una URL de Target con AMBOS motores (fetcher rápido y
    stealthy/Camoufox) y devuelve el status + preview de cada uno, sin ocultar
    el error tras un 502. Solo target.com."""
    _check_auth(authorization)
    if "target.com" not in urlparse(req.url).netloc.lower():
        raise HTTPException(400, "solo target.com")

    def _probe():
        res = {}
        for fet in ("fetcher", "stealthy"):
            try:
                st, body = _http_get(req.url, fet, req.timeout, accept_json=(fet == "fetcher"))
                res[fet] = {"status": st, "len": len(body or ""), "preview": (body or "")[:200]}
            except Exception as e:
                res[fet] = {"error": f"{type(e).__name__}: {e}"}
        return res

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _probe)


@app.post("/fetch-json")
async def fetch_json(req: FetchRequest, authorization: Optional[str] = Header(None)):
    """Devuelve el body crudo de un endpoint JSON de RedSky (search o pdp), buscado
    con el TLS de Chrome + proxy US. El PHP (target-client.php) arma la URL y parsea."""
    _check_auth(authorization)
    loop = asyncio.get_event_loop()
    status_code, body, err = await loop.run_in_executor(_executor, _fetch_json_blocking, req.url, req.timeout)
    if status_code == 200 and body and not err:
        return {"status": status_code, "size_bytes": len(body), "body": body}
    raise HTTPException(status.HTTP_502_BAD_GATEWAY,
                        f"No se pudo obtener el JSON (http {status_code}): {err or 'sin contenido'}")


@app.post("/fetch-html")
async def fetch_html(req: FetchRequest, authorization: Optional[str] = Header(None)):
    """Devuelve el HTML de una página de producto (Walmart) con fallback a browser real."""
    _check_auth(authorization)
    loop = asyncio.get_event_loop()
    status_code, html, err = await loop.run_in_executor(_executor, _fetch_html_blocking, req.url, req.timeout)
    if status_code == 200 and html and not err:
        return {"status": status_code, "size_bytes": len(html), "html": html}
    raise HTTPException(status.HTTP_502_BAD_GATEWAY,
                        f"No se pudo obtener el HTML (http {status_code}): {err or 'sin contenido'}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=5001, log_level=LOG_LEVEL.lower())
