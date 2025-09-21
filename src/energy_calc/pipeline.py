from __future__ import annotations
import logging

from .config import RunConfig
from .io_db import connect_db as _open_conn, load_delta_brutto, truncate_details_v2, copy_details_v2
from .params.loader import load_params
from .engines import oze as oze_engine
from .engines import arbi as arbi_engine
from .engines import broker as broker_engine

log = logging.getLogger(__name__)


def full_rebuild(cfg: RunConfig) -> None:
    with _open_conn(cfg) as conn:
        log.info("Loading params…")
        params = load_params(conn)

        log.info("Loading delta_brutto…")
        df = load_delta_brutto(conn)

        log.info("Computing OZE…")
        df_oze  = oze_engine.compute_oze_detail(df, params.oze)

        log.info("Computing ARBI…")
        df_arbi = arbi_engine.compute_arbi_detail(
            df, params.arbi, params.arbi_price_low, params.arbi_price_high
        )

        log.info("Broker merge…")
        df_broker = broker_engine.compute_broker_detail(df, params, df_oze, df_arbi)

        log.info("Saving detail tables…")
        truncate_details_v2(conn, schema="output")
        copy_details_v2(conn, df_broker, df_oze, df_arbi, schema="output")

        log.info(
            "Done | OZE[e_ch=%.3f,e_dis=%.3f] ARBI[e_ch=%.3f,e_dis=%.3f,net=%.2f PLN]",
            float(df_oze["e_ch_mwh"].sum()),
            float(df_oze["e_dis_mwh"].sum()),
            float(df_arbi["e_ch_mwh"].sum()),
            float(df_arbi["e_dis_mwh"].sum()),
            float(df_arbi["net_value_pln"].sum()),
        )
