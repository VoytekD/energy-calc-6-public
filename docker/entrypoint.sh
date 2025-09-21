#!/usr/bin/env bash
set -euo pipefail

export PYTHONUNBUFFERED=1
export TZ="${TZ:-Europe/Warsaw}"

echo "[entrypoint] starting energy-calc-6 worker..."
echo "[entrypoint] TZ=${TZ}"
echo "[entrypoint] PERIODIC_TICK_SEC=${PERIODIC_TICK_SEC:-<UNSET>}"
echo "[entrypoint] NOTIFY_CHANNELS=${NOTIFY_CHANNELS:-<UNSET>}"
echo "[entrypoint] DEBOUNCE_SECONDS=${DEBOUNCE_SECONDS:-<UNSET>}"
echo "[entrypoint] RUN_ONCE=${RUN_ONCE:-<UNSET>}"

# --- wymagane env: tylko tick, bez fallbacków ---
if [[ -z "${PERIODIC_TICK_SEC:-}" ]]; then
  echo "FATAL: missing env PERIODIC_TICK_SEC" >&2
  exit 64
fi
if ! [[ "${PERIODIC_TICK_SEC}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "FATAL: PERIODIC_TICK_SEC must be numeric seconds, got: '${PERIODIC_TICK_SEC}'" >&2
  exit 64
fi

# --- wymagane env DB (PG* lub DB_*) ---
db_ok=0
if [[ -n "${PGHOST:-}" && -n "${PGDATABASE:-}" && -n "${PGUSER:-}" && -n "${PGPASSWORD:-}" ]]; then db_ok=1; fi
if [[ -n "${DB_HOST:-}" && -n "${DB_NAME:-}" && -n "${DB_USER:-}" && -n "${DB_PASSWORD:-}" ]]; then db_ok=1; fi
if [[ "${db_ok}" -ne 1 ]]; then
  echo "FATAL: missing DB env. Provide either PGHOST/PGDATABASE/PGUSER/PGPASSWORD or DB_HOST/DB_NAME/DB_USER/DB_PASSWORD." >&2
  exit 64
fi

# --- PYTHONPATH: dołóż /app/src jeśli nie ma ---
export PYTHONPATH="${PYTHONPATH:-}"
case ":${PYTHONPATH}:" in
  *":/app/src:"*) : ;;  # już jest
  *) export PYTHONPATH="/app/src:${PYTHONPATH}";;
esac
echo "[entrypoint] PYTHONPATH=${PYTHONPATH}"

# --- preflight import: pokaż pełny traceback, jeśli coś nie gra ---
python - <<'PY'
import os, sys, traceback
print(f"[preflight] python={sys.version.split()[0]} cwd={os.getcwd()}")
print(f"[preflight] sys.path[0:3]={sys.path[0:3]}")
try:
    import energy_calc.main as m
    print("[preflight] import energy_calc.main OK")
    # sprawdź, czy ma funkcję main
    assert hasattr(m, "main"), "energy_calc.main missing function main()"
except Exception:
    print("[preflight] import FAILED — traceback below:", flush=True)
    traceback.print_exc()
    sys.exit(42)
PY

echo "[preflight] ok"

# --- uruchom właściwy proces (wywołanie main()) ---
# jeśli chcesz być niezależny od strażnika __name__ == '__main__':
exec python -c "from energy_calc.main import main; main()"
# alternatywa (gdy masz w pliku guard): exec python -m energy_calc.main
