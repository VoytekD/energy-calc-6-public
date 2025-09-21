# ---- Base runtime ----
FROM python:3.11-slim

# Logi bez buforowania + strefa czasu
ENV PYTHONUNBUFFERED=1 \
    TZ=Europe/Warsaw

# Katalog roboczy
WORKDIR /app

# Tzdata (strefa czasu) – bez zbędnych zależności
RUN apt-get update && apt-get install -y --no-install-recommends tzdata && \
    rm -rf /var/lib/apt/lists/*

# --- Pliki projektu ---
# Jeśli używasz pyproject.toml (zalecane), kopiujemy go przed instalacją zależności
COPY pyproject.toml /app/pyproject.toml

# Kod źródłowy (układ src/energy_calc/…)
COPY src/ /app/src/

# Entrypoint – konwersja CRLF->LF (na wszelki wypadek) i prawa wykonania
COPY ./docker/entrypoint.sh /app/entrypoint.sh
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Python powinien widzieć moduły z /app/src
ENV PYTHONPATH="/app/src:${PYTHONPATH}"

# --- Zależności Pythona ---
# psycopg3 (binary) >= 3.2 – potrzebne do notifies(timeout=…)
# potem instalacja projektu z pyproject.toml (jeśli definiuje dependencies)
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir "psycopg[binary]>=3.2,<4" && \
    pip install --no-cache-dir .

# --- Start kontenera ---
ENTRYPOINT ["/app/entrypoint.sh"]
