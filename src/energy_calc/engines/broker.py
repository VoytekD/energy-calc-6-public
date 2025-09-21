from __future__ import annotations
import logging
import numpy as np
import pandas as pd
from ..models import Params

log = logging.getLogger(__name__).getChild("broker")


def compute_broker_detail(
    df_base: pd.DataFrame,   # ts_utc, delta_brutto, price_pln_mwh (nieużywane tu poza czasem)
    params: Params,
    df_oze: pd.DataFrame,    # wynik compute_oze_detail
    df_arbi: pd.DataFrame,   # wynik compute_arbi_detail
) -> pd.DataFrame:
    """
    Alokacja mocy z priorytetem OZE + limit mocy umownej.
    Zwraca kolumny dla output.energy_broker_detail.
    """
    cols = [
        "ts_start","ts_end","step_hours",
        "req_ch_oze_mw","req_dis_oze_mw","req_ch_arbi_mw","req_dis_arbi_mw",
        "cap_ch_mw","cap_dis_mw","cap_contract_mw",
        "alloc_ch_oze_mw","alloc_dis_oze_mw","alloc_ch_arbi_mw","alloc_dis_arbi_mw",
        "note",
    ]
    if df_oze.empty or df_arbi.empty:
        return pd.DataFrame(columns=cols)

    o = df_oze[["ts_start","ts_end","step_hours","p_ch_mw","p_dis_mw"]].rename(
        columns={"p_ch_mw":"req_ch_oze_mw","p_dis_mw":"req_dis_oze_mw"}
    )
    a = df_arbi[["ts_start","ts_end","step_hours","p_ch_mw","p_dis_mw"]].rename(
        columns={"p_ch_mw":"req_ch_arbi_mw","p_dis_mw":"req_dis_arbi_mw"}
    )
    m = o.merge(a, on=["ts_start","ts_end","step_hours"], how="inner").copy()

    cap_ch_mw = params.oze.c_rate_ch_mw + params.arbi.c_rate_ch_mw
    cap_dis_mw = params.oze.c_rate_dis_mw + params.arbi.c_rate_dis_mw
    contract = params.moc_umowna_mw if params.moc_umowna_mw and params.moc_umowna_mw > 0 else None

    req_ch_oze = m["req_ch_oze_mw"].to_numpy()
    req_ch_arbi = m["req_ch_arbi_mw"].to_numpy()
    req_dis_oze = m["req_dis_oze_mw"].to_numpy()
    req_dis_arbi = m["req_dis_arbi_mw"].to_numpy()

    cap_ch = np.full(len(m), cap_ch_mw, dtype=float)
    cap_dis = np.full(len(m), cap_dis_mw, dtype=float)
    if contract is not None:
        cap_ch = np.minimum(cap_ch, contract)
        cap_dis = np.minimum(cap_dis, contract)

    alloc_ch_oze = np.minimum(req_ch_oze, cap_ch)
    remaining_ch = np.maximum(0.0, cap_ch - alloc_ch_oze)
    alloc_ch_arbi = np.minimum(req_ch_arbi, remaining_ch)

    alloc_dis_oze = np.minimum(req_dis_oze, cap_dis)
    remaining_dis = np.maximum(0.0, cap_dis - alloc_dis_oze)
    alloc_dis_arbi = np.minimum(req_dis_arbi, remaining_dis)

    out = m.copy()
    out["cap_ch_mw"] = cap_ch_mw
    out["cap_dis_mw"] = cap_dis_mw
    out["cap_contract_mw"] = contract
    out["alloc_ch_oze_mw"] = alloc_ch_oze
    out["alloc_dis_oze_mw"] = alloc_dis_oze
    out["alloc_ch_arbi_mw"] = alloc_ch_arbi
    out["alloc_dis_arbi_mw"] = alloc_dis_arbi
    out["note"] = None

    log.info(
        "BROKER detail | rows=%d | cap[ch=%.3f,dis=%.3f], contract=%s",
        len(out), cap_ch_mw, cap_dis_mw, f"{contract:.3f}" if contract else "None"
    )
    return out[cols]
