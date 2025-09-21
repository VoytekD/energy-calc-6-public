from __future__ import annotations

import logging
import time
import os
import pandas as pd
import psycopg

from .config import RunConfig

LOG = logging.getLogger(__name__)


def connect_db(cfg: RunConfig) -> psycopg.Connection:
    t0 = time.perf_counter()
    conn = psycopg.connect(
        host=cfg.db_host,
        port=cfg.db_port,
        dbname=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
        autocommit=True,
        # UWAGA: brak row_factory=dict_row — pandas.read_sql_query wymaga krotek z kursora
    )
    LOG.info("DB connection established in %.1f ms", (time.perf_counter() - t0) * 1000)
    return conn


def load_delta_brutto(conn: psycopg.Connection) -> pd.DataFrame:
    q = """
        SELECT
          ts_utc,
          delta_brutto::float8 AS delta_brutto,
          price_pln_mwh::float8 AS price_pln_mwh
        FROM output.delta_brutto
        ORDER BY ts_utc
    """
    df = pd.read_sql_query(q, conn)
    LOG.info(
        "Loaded output.delta_brutto: rows=%d cols=%d price[min=%.2f, max=%.2f] nulls=%d",
        len(df), len(df.columns),
        (df["price_pln_mwh"].min() if len(df) else float("nan")),
        (df["price_pln_mwh"].max() if len(df) else float("nan")),
        int(df["price_pln_mwh"].isna().sum()) if len(df) else 0,
    )
    return df


# ---------- BOOTSTRAP OUTPUT OBJECTS (idempotent) ----------

def _run_sql_file(conn: psycopg.Connection, path: str) -> None:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Brak pliku SQL: {path}")
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    if not sql.strip():
        return
    with conn.cursor() as cur:
        cur.execute(sql)
    LOG.info("Executed SQL file: %s", path)


def ensure_output_objects(conn: psycopg.Connection, sql_dir: str = "/app/sql") -> None:
    """
    Idempotentny bootstrap obiektów w schemacie `output` na podstawie Twoich plików:
      - 01_tables.sql        (tabele detail)
      - 02_view_summary.sql  (widok dzienny)
    Wywoływany przed TRUNCATE, aby po wipe DB odtworzyć brakujące obiekty.
    """
    tables_sql = os.path.join(sql_dir, "01_tables.sql")
    view_sql   = os.path.join(sql_dir, "02_view_summary.sql")

    # Najpierw tabele (zależność widoku)
    _run_sql_file(conn, tables_sql)
    _run_sql_file(conn, view_sql)


def truncate_details_v2(conn: psycopg.Connection, schema: str = "output") -> None:
    # Upewnij się, że obiekty istnieją (po wipe DB)
    ensure_output_objects(conn, sql_dir="/app/sql")

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {schema}.energy_oze_detail")
        cur.execute(f"TRUNCATE {schema}.energy_arbi_detail")
        cur.execute(f"TRUNCATE {schema}.energy_broker_detail")
    LOG.info("Truncated %s.energy_*_detail tables", schema)


def copy_details_v2(
    conn: psycopg.Connection,
    df_broker: pd.DataFrame,
    df_oze: pd.DataFrame,
    df_arbi: pd.DataFrame,
    schema: str = "output",
) -> None:
    from .map_detail import BROKER_COLS, OZE_COLS, ARBI_COLS, sanitize_types

    tb = sanitize_types(df_broker, BROKER_COLS)
    to = sanitize_types(df_oze,    OZE_COLS)
    ta = sanitize_types(df_arbi,   ARBI_COLS)

    def _copy(df, fq, cols):
        if df.empty:
            return
        with conn.cursor() as cur:
            with cur.copy(f"COPY {fq} ({','.join(cols)}) FROM STDIN WITH (FORMAT CSV)") as cp:
                df.to_csv(cp, index=False, header=False)

    _copy(tb, f"{schema}.energy_broker_detail", BROKER_COLS)
    _copy(to, f"{schema}.energy_oze_detail",    OZE_COLS)
    _copy(ta, f"{schema}.energy_arbi_detail",   ARBI_COLS)

    LOG.info("COPY v2 done | broker=%d, oze=%d, arbi=%d", len(tb), len(to), len(ta))
