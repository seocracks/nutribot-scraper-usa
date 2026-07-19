# Nutribot USA — Microservicio de scraping (Walmart, Target)

Gemelo del scraper español (`github.com/seocracks/nutribot-scraper`), adaptado a
las cadenas de EE. UU. Servicio HTTP en Python (FastAPI + Scrapling) que se
despliega en el VPS (EasyPanel) y lo consume la app PHP del fork USA.

## Por qué existe (y por qué el PHP no basta)

**Kroger** tiene API oficial → se consume directo desde PHP, **no pasa por aquí**.

**Target (RedSky)** y **Walmart** rechazan las peticiones por **fingerprint TLS**
(Akamai / PerimeterX), devolviendo 403 aunque la IP sea de EE. UU. — verificado
el 2026-07-19: `curl`/PHP con proxy US → 403. Scrapling resuelve exactamente eso:
`Fetcher.get(impersonate="chrome")` presenta el TLS de Chrome y, combinado con la
IP residencial de DataImpulse (país-US), pasa el bot-management. Es la misma
técnica que en el scraper ES desbloquea Carrefour tras Cloudflare.

## Arquitectura (idéntica al ES)

- Detección de cadena por dominio (`target.com` / `walmart.com`).
- Dos fetchers: `fetcher` (curl_cffi, TLS-Chrome, rápido — RedSky JSON) y
  `stealthy` (Camoufox, browser real — reservado para PerimeterX de Walmart).
- Proxy US **siempre** (a diferencia del ES, donde solo Carrefour lo usaba: aquí
  ambas cadenas rechazan IPs no-US).
- `blocked_domains` con el browser real para ahorrar ancho de banda del proxy.
- Throttling con delay aleatorio. Auth Bearer. Whitelist de hosts (anti-SSRF).

## Variables de entorno

| Var | Obligatoria | Default | Descripción |
|---|---|---|---|
| `SCRAPER_API_KEY` | sí | — | Token Bearer que el cliente PHP envía |
| `PROXY_URL` | sí | — | Proxy DataImpulse **con targeting país-US** |
| `DEFAULT_FETCHER` | no | `fetcher` | `fetcher` (rápido) o `stealthy` (browser real) |
| `MAX_CONCURRENT` | no | `2` | Máx. scrapes en paralelo (Camoufox = mucha RAM) |
| `TARGET_API_KEY` | no | (la pública) | Key de RedSky; si Target la rota, actualizar |
| `TARGET_STORE_ID` | no | `1058` | Tienda de referencia (Columbus Central, OH) |
| `TARGET_ZIP` | no | `43215` | ZIP de referencia (Columbus, OH) |
| `LOG_LEVEL` | no | `INFO` | `DEBUG` / `INFO` / `WARNING` |

## Endpoints

Todos requieren `Authorization: Bearer <SCRAPER_API_KEY>`, excepto `/health`.

- **`GET /health`** — `{"status":"ok"}`. Healthcheck de EasyPanel.
- **`POST /fetch-json`** `{url, timeout}` — GET crudo de un endpoint RedSky
  (search o pdp) con TLS-Chrome + proxy US. Devuelve `{status, size_bytes, body}`.
  **Es lo que desbloquea el import de Target**: `target-client.php` arma la URL de
  RedSky y este servicio la busca; el PHP parsea (ya tiene `targetNormalizarProducto`).
- **`POST /scrape`** `{url, fetcher?, timeout}` — precio normalizado de una URL de
  producto. Target (por tcin → RedSky pdp) y Walmart (`__NEXT_DATA__`). Devuelve
  `ScrapeResult` con `precio_centimos`, `unidad_venta`, `upc`, `es_ficha`.
- **`POST /scrape-batch`** `{items:[{id,url}], fetcher?, timeout}` — N en paralelo.
- **`POST /fetch-html`** `{url, timeout}` — HTML de una página (Walmart PDP) con
  fallback a browser real. Para cuando se aborde Walmart.

## Estado

- **Target vía `/fetch-json`**: listo. Falta validarlo EN EL VPS con proxy US real
  (desde IP europea RedSky da 403 incluso con proxy en curl; la hipótesis es que el
  TLS de Chrome de Scrapling + IP US lo pasa, igual que Carrefour en el ES).
- **Walmart**: extractor `__NEXT_DATA__` escrito pero **EXPERIMENTAL** — PerimeterX
  es agresivo; se validará cuando el proyecto aborde Walmart (hoy el catálogo va con
  Kroger + Target). El programa de afiliados de Walmart es la vía preferente.

## Probar localmente

```bash
python -m venv venv && source venv/bin/activate   # o venv/Scripts/activate en Windows
pip install -r requirements.txt
python -m playwright install chromium

export SCRAPER_API_KEY="test-token-1234"
export PROXY_URL="http://usuario__cr.us:pass@gw.dataimpulse.com:823"
python server.py

# En otra terminal:
curl http://localhost:5001/health
curl -X POST http://localhost:5001/fetch-json \
  -H "Authorization: Bearer test-token-1234" -H "Content-Type: application/json" \
  -d '{"url":"https://redsky.target.com/redsky_aggregations/v1/web/plp_search_v2?key=9f36aeafbe60771e321a7cc95a78140772ab3e96&channel=WEB&keyword=milk&count=4&offset=0&page=%2Fs%2Fmilk&pricing_store_id=1058&store_ids=1058&zip=43215&visitor_id=0123456789ABCDEF0123456789ABCDEF&platform=desktop"}'
```

## Despliegue en EasyPanel — paso a paso

Mismo patrón que el scraper ES. Requiere el código en un repo Git accesible.

**1. Subir el repo a GitHub** (desde este directorio, el commit ya está hecho):
```bash
git remote add origin git@github.com:seocracks/nutribot-scraper-usa.git   # crea el repo antes en github.com/new (privado)
git push -u origin main
```

**2. Crear el servicio en EasyPanel:**
- Proyecto → **+ Service → App**.
- **Source**: GitHub → repo `nutribot-scraper-usa`, rama `main`.
- **Build**: Dockerfile (EasyPanel lo autodetecta; no toques el comando).
- **Environment** (pestaña Environment, una var por línea). ⚠️ **Secretos reales:
  se ponen SOLO aquí en EasyPanel, NUNCA en el repo.**
  ```
  SCRAPER_API_KEY=<el valor de SCRAPER_USA_KEY del config.php del fork>
  PROXY_URL=http://USUARIO__cr.us:PASSWORD@gw.dataimpulse.com:823   # tu proxy DataImpulse país-US
  DEFAULT_FETCHER=fetcher
  MAX_CONCURRENT=2
  ```
  (TARGET_STORE_ID/TARGET_ZIP ya traen el default de Columbus OH; solo ponlos si
  cambias de tienda de referencia.)
- **Ports / Proxy**: puerto interno **5001**. Asigna un dominio, p. ej.
  `scraper-usa.tudominio.com`.
- **Deploy**. El healthcheck de `/health` debe pasar a verde.

**3. DNS — NUBE GRIS (DNS-only), nunca naranja.** Igual que `scraper.nutribot.es`:
Camoufox + los timeouts largos romperían tras Cloudflare (524 + Bot Fight).

**4. Conectar el fork PHP**: en `nutribot-usa/config/config.php` pon la URL:
```php
define('SCRAPER_USA_URL', 'https://scraper-usa.tudominio.com');
```
(`SCRAPER_USA_KEY` ya coincide con `SCRAPER_API_KEY` de arriba.)

**5. Probar de punta a punta** (`target-client.php` ya enruta por el scraper):
```bash
# healthcheck público
curl https://scraper-usa.tudominio.com/health
# import de Target real (desde el fork, dry-run):
php cli/importar-target.php --term=milk --cat=milk --max=12
```
Si devuelve productos con precio → Target desbloqueado. Entonces:
`php cli/importar-target.php --max=96 --apply` y el ciclo
`enriquecer-usda → corregir-outliers → clasificar → cron-indexar`.
