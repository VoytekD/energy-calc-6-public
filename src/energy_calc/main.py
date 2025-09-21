# src/energy_calc/main.py
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import List, Optional

import psycopg  # psycopg3

from .pipeline import full_rebuild

# --- logowanie ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger(__name__)


@dataclass
class Config:
    # harmonogram / logika
    notify_channels: List[str]
    tick_seconds: float
    debounce_seconds: float
    log_level: str
    # połączenie DB (używa io_db.connect_db(cfg))
    db_host: str
    db_port: str
    db_name: str
    db_user: str
    db_password: str


def _env_required(name: str) -> str:
    val = os.getenv(name)
    if val is None or str(val).strip() == "":
        raise RuntimeError(f"Missing required env: {name}")
    return val


def _load_db_env() -> tuple[str, str, str, str, str]:
    """
    Ładuje zmienne DB z ENV:
      - preferuje PG* jeśli są, albo DB_* (tak jak waliduje entrypoint.sh)
      - port: jeśli nie podany, używa '5432'
    """
    host = os.getenv("PGHOST") or os.getenv("DB_HOST")
    db   = os.getenv("PGDATABASE") or os.getenv("DB_NAME")
    user = os.getenv("PGUSER") or os.getenv("DB_USER")
    pwd  = os.getenv("PGPASSWORD") or os.getenv("DB_PASSWORD")
    port = os.getenv("PGPORT") or os.getenv("DB_PORT") or "5432"

    missing = [n for n, v in [("host", host), ("db", db), ("user", user), ("pwd", pwd)] if not v]
    if missing:
        raise RuntimeError(
            "Missing DB env. Provide either PGHOST/PGDATABASE/PGUSER/PGPASSWORD "
            "or DB_HOST/DB_NAME/DB_USER/DB_PASSWORD."
        )
    return host, port, db, user, pwd


def _dsn_from_cfg(cfg: Config) -> str:
    return (
        f"host={cfg.db_host} "
        f"port={cfg.db_port} "
        f"dbname={cfg.db_name} "
        f"user={cfg.db_user} "
        f"password={cfg.db_password}"
    )


def _connect_listen(dsn: str) -> psycopg.Connection:
    conn = psycopg.connect(dsn)
    conn.autocommit = True  # wymagane przy LISTEN/NOTIFY
    return conn


def _listen_on(conn: psycopg.Connection, channels: List[str]) -> None:
    with conn.cursor() as cur:
        for ch in channels:
            cur.execute(f"LISTEN {ch};")
            log.info("Listening on channel: %s", ch)


def _rebuild(cfg: Config) -> None:
    log.info("Rebuild started…")
    full_rebuild(cfg)  # io_db.connect_db korzysta z cfg.db_*
    log.info("Rebuild finished.")


def main():
    # --- konfiguracja tylko z ENV (tick bez fallbacków) ---
    notify_channels = [c.strip() for c in os.getenv("NOTIFY_CHANNELS", "ch_energy_rebuild").split(",") if c.strip()]

    tick_env = _env_required("PERIODIC_TICK_SEC")  # brak -> wyjątek
    try:
        tick_s = float(tick_env)
    except Exception:
        raise RuntimeError(f"PERIODIC_TICK_SEC must be numeric seconds, got: {tick_env!r}")

    # debounce: jeśli ma być też „bez fallbacków”, zastąp poniższą linię na _env_required("DEBOUNCE_SECONDS")
    debounce_s = float(os.getenv("DEBOUNCE_SECONDS", "2.0"))

    # DB env (PG* lub DB_*)
    db_host, db_port, db_name, db_user, db_password = _load_db_env()

    cfg = Config(
        notify_channels=notify_channels,
        tick_seconds=tick_s,
        debounce_seconds=debounce_s,
        log_level=LOG_LEVEL,
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        db_user=db_user,
        db_password=db_password,
    )

    # nagłówek
    pyver = f"{os.sys.version_info.major}.{os.sys.version_info.minor}.{os.sys.version_info.micro}"
    log.info("[entrypoint] starting energy-calc-6 worker… host=%s python=%s pid=%s",
             os.uname().nodename, pyver, os.getpid())
    log.info("Config: notify_channels=%s tick=%ss debounce=%ss log_level=%s",
             ",".join(notify_channels), int(tick_s), debounce_s, LOG_LEVEL)

    # połączenie do LISTEN/NOTIFY
    listen_conn: Optional[psycopg.Connection] = None
    try:
        dsn = _dsn_from_cfg(cfg)
        listen_conn = _connect_listen(dsn)
        _listen_on(listen_conn, notify_channels)
    except Exception as e:
        log.exception("Cannot set up LISTEN/NOTIFY (will run on tick only): %s", e)
        listen_conn = None

    # 1) pierwszy przebieg na starcie (jeśli padnie – zostajemy w pętli i będziemy próbować dalej)
    try:
        log.info("Initial full rebuild…")
        _rebuild(cfg)
    except Exception as e:
        log.exception("Error in initial rebuild (will keep running): %s", e)

    # 2) pętla: tick + triggery (natychmiastowy rebuild po debounce)
    next_tick = time.monotonic() + cfg.tick_seconds

    while True:
        try:
            now = time.monotonic()
            time_to_tick = max(0.0, next_tick - now)

            if listen_conn is not None:
                # Krótki timeout: max do debounce, żeby nie blokować pętli
                wait_timeout = min(time_to_tick, cfg.debounce_seconds)
                got_any = False

                # Czekaj na PIERWSZY notify (wyjdź od razu po jednym)
                for n in listen_conn.notifies(timeout=wait_timeout):  # psycopg >= 3.2
                    got_any = True
                    try:
                        payload = json.loads(n.payload) if n.payload else {}
                    except Exception:
                        payload = {"raw": n.payload}
                    log.info("Trigger from DB: %s", payload if payload else "(no payload)")
                    break  # nie czekaj reszty timeoutu

                if got_any:
                    # Debounce: dociągnij kaskadę powiadomień jeszcze przez cfg.debounce_seconds
                    deadline = time.monotonic() + cfg.debounce_seconds
                    while True:
                        rem = max(0.0, deadline - time.monotonic())
                        if rem == 0.0:
                            break
                        got_more = False
                        for _ in listen_conn.notifies(timeout=rem):
                            got_more = True
                            # można logować: log.debug("batched more triggers")
                            break
                        if not got_more:
                            break

                    # NATYCHMIAST po debounce – przebuduj i kontynuuj pętlę
                    log.info("Rebuild due to: trigger (immediate after debounce)…")
                    try:
                        _rebuild(cfg)
                    except Exception as e:
                        log.exception("Fatal error in rebuild: %s", e)
                    # restart zegara ticku
                    next_tick = time.monotonic() + cfg.tick_seconds
                    continue  # nowa iteracja (nie sprawdzaj już ticka teraz)

                # brak triggerów w oknie wait_timeout – sprawdź tick poniżej
            else:
                # Brak LISTEN – krótkie uśpienie, żeby nie zająć CPU
                time.sleep(min(time_to_tick, 1.0))

            # Tick okresowy?
            now = time.monotonic()
            if now >= next_tick:
                log.info("Rebuild due to: tick…")
                try:
                    _rebuild(cfg)
                except Exception as e:
                    log.exception("Fatal error in rebuild: %s", e)
                next_tick = time.monotonic() + cfg.tick_seconds

        except KeyboardInterrupt:
            log.info("Interrupted. Bye.")
            break
        except Exception as e:
            log.exception("Loop error: %s", e)
            time.sleep(0.5)


if __name__ == "__main__":
    main()
