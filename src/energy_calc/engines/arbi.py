from __future__ import annotations
import logging
from typing import Optional
import pandas as pd
from ..models import TrackParams

log = logging.getLogger(__name__).getChild("arbi")


def compute_arbi_detail(
    df: pd.DataFrame,
    tp: TrackParams,
    price_low_pln_mwh: Optional[float],
    price_high_pln_mwh: Optional[float],
) -> pd.DataFrame:
    """
    Arbitraż cenowy: price <= low → ładuj; price >= high → rozładowuj.
    Zwraca kolumny dla output.energy_arbi_detail (z finansami).
    """
    cols = [
        "ts_start","ts_end","step_hours",
        "soc_start_mwh","soc_end_mwh",
        "p_ch_mw","p_dis_mw","e_ch_mwh","e_dis_mwh",
        "loss_conv_mwh","loss_idle_mwh","loss_total_mwh",
        "price_pln_mwh","cost_pln","revenue_pln","net_value_pln",
        "soc_gap_to_min_start_mwh","soc_gap_to_min_end_mwh",
        "time_below_min_h","hit_part_cap_max","hit_part_cap_min",
    ]
    if df.empty:
        return pd.DataFrame(columns=cols)

    ts = pd.to_datetime(df["ts_utc"])
    step = (ts.shift(-1) - ts).dt.total_seconds().div(3600.0)
    default_step = step.dropna().median() if step.dropna().size else 1.0
    step = step.fillna(default_step).clip(lower=1e-9)

    emax = float(tp.emax_mwh)
    soc_min = float(tp.soc_min_mwh)
    soc_max = float(tp.soc_max_mwh)
    soc = float(tp.soc_init_mwh)

    c_ch = float(tp.c_rate_ch_mw)
    c_dis = float(tp.c_rate_dis_mw)
    eta_ch = float(tp.eta_ch)
    eta_dis = float(tp.eta_dis)
    self_dis = float(tp.self_discharge_per_h)

    low = price_low_pln_mwh
    high = price_high_pln_mwh

    rows = []
    for i, r in df.iterrows():
        ts_start = ts.iloc[i]
        dt_h = float(step.iloc[i])
        ts_end = ts_start + pd.Timedelta(hours=dt_h)

        price = None if pd.isna(r["price_pln_mwh"]) else float(r["price_pln_mwh"])
        e_cap_ch = c_ch * dt_h
        e_cap_dis = c_dis * dt_h

        loss_idle = 0.0
        if self_dis > 0.0 and soc > soc_min:
            leak = min(self_dis * emax * dt_h, soc - soc_min)
            soc -= leak
            loss_idle += leak

        soc_start = soc
        gap_to_min_start = max(0.0, soc_min - soc_start)
        hit_max = False
        hit_min = False

        e_ch = e_dis = loss_conv = 0.0
        cost = revenue = 0.0

        if price is not None and low is not None and high is not None:
            if price <= low:
                can_store = soc_max - soc
                if can_store > 1e-12:
                    e_store_max = min(can_store, e_cap_ch)
                    e_in = e_store_max / max(eta_ch, 1e-12)
                    stored = e_in * eta_ch
                    e_ch = stored
                    soc += stored
                    loss_conv += max(0.0, e_in - stored)
                    cost += e_in * price
                    if e_store_max >= can_store - 1e-12:
                        hit_max = True
                else:
                    hit_max = True
            elif price >= high:
                can_supply = soc - soc_min
                if can_supply > 1e-12:
                    e_take_max = min(can_supply, e_cap_dis)
                    e_out = e_take_max * eta_dis
                    take = e_out / max(eta_dis, 1e-12)
                    e_dis = e_out
                    soc -= take
                    loss_conv += max(0.0, take - e_out)
                    revenue += e_out * price
                    if e_take_max >= can_supply - 1e-12:
                        hit_min = True
                else:
                    hit_min = True

        soc = min(max(soc, soc_min), soc_max)
        gap_to_min_end = max(0.0, soc_min - soc)
        time_below = 0.0
        if soc_start < soc_min or soc < soc_min or (gap_to_min_start > 0 and gap_to_min_end > 0):
            time_below = dt_h if (soc_start < soc_min and soc < soc_min) else dt_h * 0.5

        rows.append({
            "ts_start": ts_start, "ts_end": ts_end, "step_hours": dt_h,
            "soc_start_mwh": round(soc_start, 6), "soc_end_mwh": round(soc, 6),
            "p_ch_mw": round(e_ch / dt_h, 6), "p_dis_mw": round(e_dis / dt_h, 6),
            "e_ch_mwh": round(e_ch, 6), "e_dis_mwh": round(e_dis, 6),
            "loss_conv_mwh": round(loss_conv, 6), "loss_idle_mwh": round(loss_idle, 6),
            "loss_total_mwh": round(loss_conv + loss_idle, 6),
            "price_pln_mwh": None if price is None else float(price),
            "cost_pln": round(cost, 2), "revenue_pln": round(revenue, 2),
            "net_value_pln": round(revenue - cost, 2),
            "soc_gap_to_min_start_mwh": round(gap_to_min_start, 6),
            "soc_gap_to_min_end_mwh": round(gap_to_min_end, 6),
            "time_below_min_h": round(time_below, 6),
            "hit_part_cap_max": bool(hit_max), "hit_part_cap_min": bool(hit_min),
        })

    out = pd.DataFrame(rows)
    log.info(
        "ARBI detail | rows=%d | e_ch=%.3f e_dis=%.3f loss(conv=%.3f idle=%.3f) | net=%.2f PLN",
        len(out), float(out["e_ch_mwh"].sum()), float(out["e_dis_mwh"].sum()),
        float(out["loss_conv_mwh"].sum()), float(out["loss_idle_mwh"].sum()),
        float(out["net_value_pln"].sum())
    )
    return out
