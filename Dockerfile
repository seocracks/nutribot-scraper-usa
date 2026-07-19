# Imagen base de Python — instalamos Scrapling encima
# (la imagen oficial de Scrapling es muy pesada; preferimos build limpio)
FROM python:3.11-slim

# Variables de runtime que Camoufox/Playwright esperan
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/app/.browsers

WORKDIR /app

# Dependencias del sistema necesarias para Chromium/Camoufox
RUN apt-get update && apt-get install -y --no-install-recommends \
        wget gnupg ca-certificates fonts-liberation tzdata \
        libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
        libdrm2 libdbus-1-3 libxkbcommon0 libxcomposite1 libxdamage1 \
        libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 \
        libasound2 libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias Python
COPY requirements.txt .
RUN pip install -r requirements.txt

# Descargar browsers (Chromium para Playwright que usa Scrapling)
RUN python -m playwright install chromium --with-deps 2>/dev/null || python -m playwright install chromium

# Copiar código
COPY server.py ./

# Puerto del microservicio
EXPOSE 5001

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5001/health', timeout=3)" || exit 1

# Arranque con uvicorn (1 worker — Camoufox consume mucha RAM)
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "5001", "--workers", "1"]
