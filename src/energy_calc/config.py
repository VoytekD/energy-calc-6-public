from __future__ import annotations
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class RunConfig:
    # DB
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_name: str = os.getenv("DB_NAME", "energia")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "")

    # Listen / scheduling (jeśli użyjesz pętli LISTEN/NOTIFY w main)
    notify_channels: tuple[str, ...] = tuple(
        c.strip() for c in os.getenv("NOTIFY_CHANNELS", "ch_energy_rebuild").split(",") if c.strip()
    )
    periodic_tick_sec: int = int(os.getenv("PERIODIC_TICK_SEC", "300"))
    notify_debounce_sec: float = float(os.getenv("NOTIFY_DEBOUNCE_SEC", "2"))

    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
