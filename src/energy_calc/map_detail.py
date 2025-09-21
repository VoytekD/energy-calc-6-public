from __future__ import annotations
import pandas as pd

# Kolejność kolumn musi odpowiadać 01_tables.sql
BROKER_COLS = [
    "ts_start","ts_end","step_hours",
    "req_ch_oze_mw","req_dis_oze_mw","req_ch_arbi_mw","req_dis_arbi_mw",
    "cap_ch_mw","cap_dis_mw","cap_contract_mw",
    "alloc_ch_oze_mw","alloc_dis_oze_mw","alloc_ch_arbi_mw","alloc_dis_arbi_mw",
    "note",
]

OZE_COLS = [
    "ts_start","ts_end","step_hours",
    "soc_start_mwh","soc_end_mwh",
    "p_ch_mw","p_dis_mw","e_ch_mwh","e_dis_mwh",
    "loss_conv_mwh","loss_idle_mwh","loss_total_mwh",
    "spill_surplus_mwh","unmet_deficit_mwh",
    "soc_gap_to_min_start_mwh","soc_gap_to_min_end_mwh",
    "time_below_min_h","hit_part_cap_max","hit_part_cap_min",
]

ARBI_COLS = [
    "ts_start","ts_end","step_hours",
    "soc_start_mwh","soc_end_mwh",
    "p_ch_mw","p_dis_mw","e_ch_mwh","e_dis_mwh",
    "loss_conv_mwh","loss_idle_mwh","loss_total_mwh",
    "price_pln_mwh","cost_pln","revenue_pln","net_value_pln",
    "soc_gap_to_min_start_mwh","soc_gap_to_min_end_mwh",
    "time_below_min_h","hit_part_cap_max","hit_part_cap_min",
]


def sanitize_types(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    for c in df.columns:
        if c.startswith("ts_"):
            df[c] = pd.to_datetime(df[c])
        elif df[c].dtype == "bool":
            pass
        elif df[c].dtype.kind in "ifu":
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            pass
    return df
