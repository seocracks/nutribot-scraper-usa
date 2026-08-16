"""
Microservicio de scraping para la web USA (Walmart, Target).

Gemelo del scraper español (github.com/seocracks/nutribot-scraper), adaptado a
las cadenas de EE. UU. Resuelve el problema que el cliente PHP no puede:
RedSky (Target) y Walmart rechazan las peticiones por FINGERPRINT TLS (Akamai /
PerimeterX) aunque la IP sea de EE. UU. Scrapling combina el TLS de Chrome
(curl_cffi `impersonate`) con la IP residencial (DataImpulse país-US) → pasan.

Kroger NO pasa por aquí: tiene API oficial y se consume directo desde PHP.

Endpoints:
  POST /scrape          — scrapea una URL de producto (Target/Walmart). Walmart
                          devuelve además `detalle` (ficha completa: marca,
                          breadcrumbs, ingredientes, especificaciones…)
  POST /scrape-batch    — N URLs en paralelo
  POST /walmart-search  — término → ~55-62 productos CON precio por página.
                          La vía barata (2-3 KB/producto en el cable) para
                          descubrimiento y refresco masivo de precios
  POST /fetch-json      — GET crudo de un endpoint JSON (RedSky search/pdp) con TLS Chrome
  POST /fetch-html      — GET de una página con fallback a browser real (Walmart PDP)
  GET  /health          — healthcheck (sin auth)

Autenticación: header `Authorization: Bearer <SCRAPER_API_KEY>`.
Configuración: variables de entorno (ver README).
"""
import os
import re
import gzip
import time
import json
import random
import logging
import asyncio
from html import unescape
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote_plus, urlparse

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


class WalmartSearchRequest(BaseModel):
    termino: str = Field(..., min_length=1, max_length=120)
    pagina: int = Field(1, ge=1, le=25)
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
    # Bytes ~facturables en el cable (gzip aprox., suma de TODOS los intentos).
    # El proxy DataImpulse cobra por GB: es el contador de coste. Solo Walmart.
    wire_bytes: int = 0
    # Ficha COMPLETA de Walmart (nombre, marca, upc, breadcrumbs, ingredientes,
    # especificaciones, foto, vendedor…) para walmartDetalle() del PHP. None en Target.
    detalle: Optional[dict] = None


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


def _http_get(url: str, fetcher: str, timeout: int, accept_json: bool = False) -> tuple[int, str]:
    """GET con Scrapling, devuelve (status, body). Proxy US SIEMPRE (Target/Walmart
    rechazan IPs no-US). 'fetcher' = curl_cffi TLS-Chrome (rápido); 'stealthy' =
    Camoufox (para PerimeterX de Walmart)."""
    from scrapling.fetchers import Fetcher, StealthyFetcher

    headers = {
        "Accept": ("application/json,text/plain,*/*" if accept_json
                   else "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"),
        "Accept-Language": "en-US,en;q=0.9",
    }

    if fetcher == "stealthy" and not accept_json:
        super_id = _detectar_super(url)
        blocked = _BLOCKED_DOMAINS if super_id in ("walmart", "target") else None
        page = StealthyFetcher.fetch(
            url,
            headless=True,
            proxy=PROXY_URL,
            solve_cloudflare=False,   # Target/Walmart usan Akamai/PerimeterX, no Cloudflare
            wait=2500,
            timeout=timeout * 1000,
            blocked_domains=blocked,
        )
        return getattr(page, "status", 0) or 200, page.html_content or ""

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
# VALIDADO 2026-08-16 (antes EXPERIMENTAL): prueba de ~50 peticiones + validación
# de 150 productos aleatorios del catálogo, desde IP-ES con DataImpulse país-US y
# TLS de Chrome (curl_cffi): 0 bloqueos PerimeterX en 159 peticiones, 100 % de
# fichas OK (descontando 404 legítimos = producto retirado), UPC correcto 97 %.
# El fetcher rápido BASTA; el browser real (stealthy) queda como escape manual.
#
# Las rutas de FICHA cuelgan de props.pageProps.initialData.data del
# <script id="__NEXT_DATA__">; las de BÚSQUEDA, de …initialData.searchResult
# (con currentPrice null y el precio como string "$6.96" — son shapes distintos).

_WALMART_RE_NEXT = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>', re.DOTALL)
_WALMART_REINTENTOS = 3


def _wire_aprox(html: str) -> int:
    """Bytes ~facturables por el proxy para este body (DataImpulse cobra por GB).
    Walmart sirve gzip/brotli y curl lo descomprime antes de llegar aquí, así que
    se re-comprime en local para estimar lo que viajó por el cable. Medido
    2026-08-16: PDP de 493 KB en plano → 112,7 KB gzip-6 vs 107-112 KB reales."""
    try:
        return len(gzip.compress((html or "").encode("utf-8", "ignore"), 6))
    except Exception:
        return int(len(html or "") * 0.25)


def _walmart_es_bloqueo(status_code: int, html: str) -> bool:
    """Bloqueo PerimeterX: status del challenge o marcadores del muro en el HTML.
    ⚠ NO usar 'captcha' ni 'blocked' a secas como señal: aparecen dentro del
    bundle JS de páginas perfectamente VÁLIDAS (falso positivo medido al montar
    la prueba del 2026-08-16)."""
    if status_code in (403, 412, 429):
        return True
    h = (html or "").lower()
    return "px-captcha" in h or "robot or human" in h


def _walmart_next_root(html: str) -> Optional[dict]:
    """JSON completo del <script id="__NEXT_DATA__">, o None si falta/no parsea."""
    m = _WALMART_RE_NEXT.search(html or "")
    if not m:
        return None
    try:
        data = json.loads(m.group(1))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _walmart_get(url: str, timeout: int, intentos: int = _WALMART_REINTENTOS) -> tuple[int, str, str, int]:
    """GET a walmart.com con el fetcher rápido y REINTENTOS obligatorios.

    Por qué reintentos (mismo patrón que _redsky_get_json en Target):
      · el proxy DataImpulse da `SSLError / curl(35) TLS connect error` esporádico
        (~1 de cada 15-20 peticiones): es transitorio, el reintento sale por otra
        IP y funciona;
      · un bloqueo PerimeterX puntual también se resuelve rotando de IP.
    Un 404 NO se reintenta: es producto retirado del catálogo (dato, no fallo).
    Éxito = HTTP 200 con __NEXT_DATA__ presente (el caller valida el contenido).

    Devuelve (status, html, error, wire_bytes). error != '' solo si TODOS los
    intentos fallaron; wire_bytes acumula todos los intentos (coste real)."""
    ultimo_status, ultimo_html, ultimo_err, wire = 0, "", "", 0
    for i in range(1, max(1, intentos) + 1):
        if i > 1:
            time.sleep(random.uniform(1.0, 2.0))
        try:
            status_code, html = _http_get(url, "fetcher", timeout)
        except Exception as e:
            ultimo_status, ultimo_html = 0, ""
            ultimo_err = f"{type(e).__name__}: {e}"
            log.info("Walmart intento %d/%d error de red (%s), reintentando", i, intentos, type(e).__name__)
            continue
        wire += _wire_aprox(html)
        if status_code == 404:
            return 404, html, "", wire
        if _walmart_es_bloqueo(status_code, html):
            ultimo_status, ultimo_html = status_code, html
            ultimo_err = f"bloqueo PerimeterX (HTTP {status_code})"
            log.info("Walmart intento %d/%d bloqueado por PerimeterX, rotando IP", i, intentos)
            continue
        if status_code == 200 and _WALMART_RE_NEXT.search(html or ""):
            return status_code, html, "", wire
        ultimo_status, ultimo_html = status_code, html
        ultimo_err = f"HTTP {status_code} sin __NEXT_DATA__"
    return ultimo_status, ultimo_html, ultimo_err, wire


def _walmart_extraer_pdp(data_node: dict) -> tuple[dict, dict]:
    """Ficha del nodo initialData.data del PDP → (campos ScrapeResult, detalle).

    Rutas VERIFICADAS 2026-08-16 contra peticiones reales:
      product.name / brand / usItemId / upc (12 dígitos, ya normalizado)
      product.priceInfo.currentPrice.price      → precio (float, ej. 3.58)
      product.priceInfo.unitPrice.priceString   → "2.8 ¢/fl oz"
      product.priceInfo.wasPrice.price          → precio anterior (puede ser null)
      product.availabilityStatus                → IN_STOCK / OUT_OF_STOCK
      product.sellerName                        → "Walmart.com" o marketplace
      product.category.path[].name              → breadcrumbs (filtro solo-alimentos)
      product.imageInfo.thumbnailUrl            → foto
      idml.ingredients.ingredients.value        → ingredientes (string)
      idml.specifications[] {name,value}        → "Net content statement" → tamaño"""
    prod = data_node.get("product") or {}
    idml = data_node.get("idml") or {}
    pi   = prod.get("priceInfo") or {}
    cur  = pi.get("currentPrice") if isinstance(pi.get("currentPrice"), dict) else None
    unit = pi.get("unitPrice") if isinstance(pi.get("unitPrice"), dict) else None
    was  = pi.get("wasPrice") if isinstance(pi.get("wasPrice"), dict) else None

    precio = _precio_a_centimos((cur or {}).get("price"))

    breadcrumbs = []
    for c in ((prod.get("category") or {}).get("path") or []):
        nombre_miga = str((c or {}).get("name") or "").strip()
        if nombre_miga:
            breadcrumbs.append(nombre_miga)

    ingredientes = ""
    ing = idml.get("ingredients")
    if isinstance(ing, dict):
        ingredientes = str((ing.get("ingredients") or {}).get("value") or "").strip()

    especificaciones = []
    tamano = ""
    for s in (idml.get("specifications") or []):
        if not isinstance(s, dict) or s.get("name") is None:
            continue
        spec = {"name": str(s.get("name")), "value": str(s.get("value") or "")}
        especificaciones.append(spec)
        n = spec["name"].lower()
        if not tamano and ("net content" in n or "net weight" in n):
            tamano = spec["value"]

    # TODAS las fotos del producto (6-12 sin etiquetar, imageInfo.allImages[].url):
    # las necesita el OCR de etiquetas nutricionales del fork
    # (ocrImagenesEtiquetaWalmart), que antes las sacaba del endpoint `product`
    # de BlueCart a 1 crédito por producto.
    fotos = []
    for a in ((prod.get("imageInfo") or {}).get("allImages") or []):
        u = str((a or {}).get("url") or "").strip()
        if u and u not in fotos:
            fotos.append(u)

    detalle = {
        "us_item_id":               str(prod.get("usItemId") or ""),
        "nombre":                   unescape(str(prod.get("name") or "")).strip(),
        "marca":                    unescape(str(prod.get("brand") or "")).strip(),
        "upc":                      re.sub(r"\D", "", str(prod.get("upc") or "")),
        "precio_centimos":          precio,
        "precio_ref":               str((unit or {}).get("priceString") or ""),
        "precio_anterior_centimos": _precio_a_centimos((was or {}).get("price")),
        "disponibilidad":           str(prod.get("availabilityStatus") or ""),
        "vendedor":                 str(prod.get("sellerName") or ""),
        "breadcrumbs":              breadcrumbs,
        "foto":                     str((prod.get("imageInfo") or {}).get("thumbnailUrl") or ""),
        "fotos":                    fotos,
        "ingredientes":             ingredientes,
        "tamano":                   tamano,
        "especificaciones":         especificaciones,
    }
    campos = {
        "precio_centimos":     precio,
        "unidad_venta":        tamano or None,
        "precio_ref_centimos": _precio_a_centimos((unit or {}).get("price")),
        "precio_ref_unidad":   detalle["precio_ref"] or None,
        "upc":                 detalle["upc"] or None,
        # Señal FIABLE de éxito: __NEXT_DATA__ parsea Y hay product Y currentPrice
        # no es null. (Con precio null la página puede ser válida — OUT_OF_STOCK.)
        "es_ficha":            bool(prod) and precio is not None,
    }
    return campos, detalle


def _walmart_resultado(status_code: int, html: str, err: str, wire: int) -> dict:
    """Clasifica la respuesta de _walmart_get como ScrapeResult (dict)."""
    base = {"http_code": status_code, "size_bytes": len(html or ""),
            "wire_bytes": wire, "es_ficha": False}
    if err:
        base["error"] = err
        return base
    if status_code == 404:
        # Producto RETIRADO del catálogo: dato correcto, no fallo. El caller debe
        # marcarlo no disponible y NO reintentarlo.
        base["error"] = "404 producto retirado"
        return base
    root = _walmart_next_root(html)
    data_node = ((((root or {}).get("props") or {}).get("pageProps") or {})
                 .get("initialData") or {}).get("data")
    if not isinstance(data_node, dict) or not (data_node.get("product") or {}):
        base["error"] = "pagina sin ficha de producto (redirigida a categoria/busqueda?)"
        return base
    campos, detalle = _walmart_extraer_pdp(data_node)
    base.update(campos)
    base["detalle"] = detalle
    if not base["es_ficha"]:
        base["error"] = f"currentPrice null (disponibilidad={detalle['disponibilidad'] or '?'})"
    return base


# ─── Búsqueda Walmart (/walmart-search) ─────────────────────────
def _walmart_buscar_stacks(nodo, profundidad: int = 0):
    """Encuentra la clave `itemStacks` RECURSIVAMENTE. Walmart cambia el nivel de
    anidamiento cada pocos meses (hoy: props.pageProps.initialData.searchResult.
    itemStacks, verificado 2026-08-16) — no atarse a la ruta exacta."""
    if profundidad > 12:
        return None
    if isinstance(nodo, dict):
        v = nodo.get("itemStacks")
        if isinstance(v, list):
            return v
        for val in nodo.values():
            r = _walmart_buscar_stacks(val, profundidad + 1)
            if r is not None:
                return r
    elif isinstance(nodo, list):
        for val in nodo:
            r = _walmart_buscar_stacks(val, profundidad + 1)
            if r is not None:
                return r
    return None


def _walmart_precio_busqueda(pi: dict) -> Optional[int]:
    """Precio de un item de BÚSQUEDA → céntimos. Aquí llega como STRING '$6.96'
    en priceInfo.linePrice (currentPrice es null en búsqueda; solo el PDP lo trae
    como float). 'From $…'/'2 options' = rango de variantes, no un precio → None."""
    for clave in ("linePrice", "itemPrice"):
        s = str(pi.get(clave) or "").strip()
        if not s:
            continue
        if re.search(r"\b(from|options)\b", s, re.IGNORECASE):
            return None
        m = re.search(r"\$\s*([\d,]+(?:\.\d{1,2})?)", s)
        if m:
            try:
                return int(round(float(m.group(1).replace(",", "")) * 100))
            except ValueError:
                return None
    return None


def _walmart_item_busqueda(it: dict) -> Optional[dict]:
    """Normaliza un item de itemStacks[].items[]. None si no es un producto usable."""
    us_id = str(it.get("usItemId") or "").strip()
    nombre = unescape(str(it.get("name") or "")).strip()
    if not us_id or not nombre:
        return None
    pi = it.get("priceInfo") if isinstance(it.get("priceInfo"), dict) else {}
    canonical = str(it.get("canonicalUrl") or "").split("?")[0]
    imagen = it.get("imageInfo") if isinstance(it.get("imageInfo"), dict) else {}
    disp = it.get("availabilityStatusV2") if isinstance(it.get("availabilityStatusV2"), dict) else {}
    return {
        "us_item_id":      us_id,
        "nombre":          nombre,
        # ⚠ brand llega null casi siempre en búsqueda (igual que en BlueCart):
        # la marca fiable la da el PDP (/scrape → detalle).
        "marca":           unescape(str(it.get("brand") or "")).strip(),
        "precio_centimos": _walmart_precio_busqueda(pi),
        "precio_unidad":   str(pi.get("unitPrice") or "").strip(),   # "21.8 ¢/oz"
        "foto":            str(imagen.get("thumbnailUrl") or it.get("image") or ""),
        "enlace":          ("https://www.walmart.com" + canonical) if canonical.startswith("/")
                           else f"https://www.walmart.com/ip/{us_id}",
        "disponibilidad":  str(disp.get("value") or ""),
        "vendedor":        str(it.get("sellerName") or ""),
        "patrocinado":     bool(it.get("isSponsoredFlag")),
    }


def _walmart_search_blocking(termino: str, pagina: int, timeout: int) -> dict:
    """Página de búsqueda → lista normalizada. Es la vía BARATA: ~55-62 productos
    con precio por ~120-150 KB en el cable ≈ 2-3 KB/producto, ~47× menos que pedir
    ficha a ficha. Para descubrimiento y refresco masivo de precios; la ficha
    completa (/scrape) se reserva para altas y para completar UPC/ingredientes.

    ⚠ SSR DEGRADADO (medido 2026-08-16): de vez en cuando Walmart sirve la página
    con itemStacks completo pero SIN los precios hidratados — la misma búsqueda
    dio 0/62 items con precio en una pasada y 62/62 en la siguiente. Si pasa, se
    reintenta la página ENTERA (otra IP suele traer el SSR bueno). Sin este guard
    un refresco de precios se saltaría el lote completo en silencio."""
    if THROTTLE_MIN > 0:
        time.sleep(random.uniform(THROTTLE_MIN, max(THROTTLE_MIN, THROTTLE_MAX)))
    url = "https://www.walmart.com/search?q=" + quote_plus(termino)
    if pagina > 1:
        url += f"&page={pagina}"
    log.info("Walmart search %r p%d", termino, pagina)

    wire_total = 0
    resultado = {"status": 0, "size_bytes": 0, "wire_bytes": 0,
                 "total": 0, "items": [], "error": "sin intentos"}
    for intento in range(1, _WALMART_REINTENTOS + 1):
        status_code, html, err, wire = _walmart_get(url, timeout)
        wire_total += wire
        if err or not html:
            # _walmart_get YA reintentó el transporte (TLS/PX): no insistir más.
            resultado = {"status": status_code, "size_bytes": 0, "wire_bytes": wire_total,
                         "total": 0, "items": [], "error": err or "sin contenido"}
            break
        stacks = _walmart_buscar_stacks(_walmart_next_root(html))
        if stacks is None:
            resultado = {"status": status_code, "size_bytes": len(html), "wire_bytes": wire_total,
                         "total": 0, "items": [],
                         "error": "sin itemStacks en __NEXT_DATA__ (cambio de layout?)"}
            break
        items = []
        for stack in stacks:
            for it in ((stack or {}).get("items") or []):
                if isinstance(it, dict):
                    norm = _walmart_item_busqueda(it)
                    if norm:
                        items.append(norm)
        resultado = {"status": status_code, "size_bytes": len(html), "wire_bytes": wire_total,
                     "total": len(items), "items": items, "error": None}
        con_precio = sum(1 for x in items if x["precio_centimos"] is not None)
        if items and con_precio == 0 and intento < _WALMART_REINTENTOS:
            log.info("Walmart search %r p%d: %d items con 0 precios (SSR degradado), reintentando",
                     termino, pagina, len(items))
            time.sleep(random.uniform(1.0, 2.0))
            continue
        break
    return resultado


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
            if fetcher == "stealthy":
                # Escape manual: browser real, por si PerimeterX endureciera algún
                # día. La vía validada es el fetcher rápido con reintentos.
                status_code, html = _http_get(url, "stealthy", timeout)
                r = _walmart_resultado(status_code, html, "", _wire_aprox(html))
            else:
                status_code, html, err, wire = _walmart_get(url, timeout)
                r = _walmart_resultado(status_code, html, err, wire)
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
    if "walmart" in host:
        # Vía validada 2026-08-16: fetcher rápido con reintentos (rotación de IP).
        # Un 404 sale directo (producto retirado; el browser real no lo cambiaría).
        status_code, html, err_fast, _ = _walmart_get(url, timeout)
        if not err_fast and html and status_code in (200, 404):
            return status_code, html, ""
    else:
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


@app.post("/walmart-search")
async def walmart_search(req: WalmartSearchRequest, authorization: Optional[str] = Header(None)):
    """Búsqueda de Walmart: término → ~55-62 productos con precio por página.
    La vía BARATA (2-3 KB/producto en el cable ≈ 47× menos que ficha a ficha)
    para descubrimiento de catálogo y refresco masivo de precios. La ficha
    completa (/scrape) queda para altas y para completar UPC/ingredientes."""
    _check_auth(authorization)
    loop = asyncio.get_event_loop()
    r = await loop.run_in_executor(_executor, _walmart_search_blocking,
                                   req.termino, req.pagina, req.timeout)
    if r.get("error"):
        raise HTTPException(status.HTTP_502_BAD_GATEWAY,
                            f"walmart-search '{req.termino}' p{req.pagina}: {r['error']}")
    return r


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
