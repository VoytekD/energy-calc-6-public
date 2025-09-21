# src/energy_calc/params/loader.py
from __future__ import annotations

import logging
from typing import Any, Dict, Tuple

from ..models import Params, BessParams, TrackParams

log = logging.getLogger(__name__)

# Kolumny/metadata pomijane przy scalaniu płaskich kolumn
_META_COLS = {
    "id", "pk", "uid",
    "inserted_at", "updated_at",
    "created_at", "timestamp", "ts", "ts_utc", "ts_local"
}


def _list_params_tables(conn, schema: str = "params") -> list[str]:
    """Zwraca listę tabel (BASE TABLE) w danym schemacie."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (schema,))
        return [r[0] for r in cur.fetchall() or []]


def _pg_fetch_latest_merged(conn, schema: str, table: str) -> Dict[str, Any]:
    """
    Pobiera najnowszy wiersz z <schema>.<table> (po dostępnych kolumnach czasu/id),
    zwraca scalony dict: payload JSONB + płaskie kolumny (bez metadanych).
    """
    order_bys = [
        "inserted_at DESC", "updated_at DESC", "created_at DESC",
        "timestamp DESC", "ts_utc DESC", "ts_local DESC", "ts DESC", "id DESC"
    ]
    for ob in order_bys:
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {schema}.{table} ORDER BY {ob} LIMIT 1")
                row = cur.fetchone()
                if not row:
                    continue
                cols = [d.name for d in cur.description]
                as_dict = dict(zip(cols, row))
                merged: Dict[str, Any] = {}

                # 1) payload JSONB (jeśli jest)
                if "payload" in as_dict and as_dict["payload"] is not None:
                    try:
                        merged.update(dict(as_dict["payload"]))
                    except Exception:
                        # jeśli payload nie jest jsonem/dict – pomijamy
                        pass

                # 2) płaskie kolumny (bez meta i bez payload)
                for k, v in as_dict.items():
                    lk = k.lower()
                    if lk == "payload" or lk in _META_COLS or v is None:
                        continue
                    if k not in merged:
                        merged[k] = v
                return merged
        except Exception:
            # jeśli dana kolumna sortująca nie istnieje – próbujemy kolejnej
            continue
    return {}


def _norm(v: Any) -> str:
    """Prosta normalizacja do porównania wartości (wykrywanie konfliktów)."""
    try:
        return str(float(v))
    except Exception:
        return str(v)


def _merge_all_params(conn, schema: str = "params") -> Dict[str, Any]:
    """
    Zbiera wartości ze wszystkich tabel w schemacie i scala w jeden dict.
    Jeśli ten sam klucz ma różne wartości w różnych tabelach -> ValueError.
    """
    tables = _list_params_tables(conn, schema)
    log.info("Znaleziono %d tabel w schemacie %s: %s", len(tables), schema, ", ".join(tables) if tables else "-")

    merged: Dict[str, Any] = {}
    sources: Dict[str, Tuple[str, Any]] = {}

    for t in tables:
        d = _pg_fetch_latest_merged(conn, schema, t)
        if not d:
            log.debug("%s.%s: brak danych do scalenia.", schema, t)
            continue
        log.info("%s.%s: wczytano %d kluczy (scalone JSON + kolumny)", schema, t, len(d))
        for k, v in d.items():
            if v is None:
                continue
            if k in merged:
                old_t, old_v = sources[k]
                if _norm(old_v) != _norm(v):
                    raise ValueError(
                        f"Konflikt wartości dla '{k}': {old_v!r} (z {schema}.{old_t}) vs {v!r} (z {schema}.{t}). "
                        f"Ujednolić dane, aby klucz występował z jedną wartością."
                    )
                continue
            merged[k] = v
            sources[k] = (t, v)

    log.info("Scalone klucze łącznie: %d", len(merged))
    return merged


def _num(d: Dict[str, Any], key: str) -> float:
    """Wymaga istnienia liczby pod danym kluczem; rzuca ValueError jeśli brak/nieparsowalne."""
    if key not in d or d[key] is None:
        have = ", ".join(sorted(d.keys()))
        raise ValueError(f"Brak wymaganych parametrów: [{key}]. Dostępne klucze: {have}")
    val = d[key]
    try:
        return float(val)
    except Exception:
        try:
            return float(str(val).replace(",", "."))
        except Exception:
            raise ValueError(f"Wartość pod '{key}' nie jest liczbą: {val!r}")


def load_params(conn) -> Params:
    # 1) Zbierz wartości ze wszystkich tabel params.*
    p = _merge_all_params(conn, schema="params")

    # 2) WYMAGANE KLUCZE (dokładnie takie nazwy jak w bazie)
    #    UWAGA: czasy ładowania/rozładowania podawane są w GODZINACH [h],
    #           nie jako C-rate. Przeliczenia niżej.
    emax               = _num(p, "emax")                     # [MWh]
    t_ch_h             = _num(p, "bess_c_rate_charge")       # [h] czas pełnego ładowania
    t_dis_h            = _num(p, "bess_c_rate_discharge")    # [h] czas pełnego rozładowania
    eta_ch_pct         = _num(p, "bess_charge_eff")          # [%]
    eta_dis_pct        = _num(p, "bess_discharge_eff")       # [%]
    lambda_month_pct   = _num(p, "bess_lambda_month")        # [% / miesiąc] utrata SOC
    p_arbi_pct         = _num(p, "procent_arbitrazu")        # [%]
    price_low          = _num(p, "arbi_price_low")           # [PLN/MWh]
    price_high         = _num(p, "arbi_price_high")          # [PLN/MWh]
    moc_umowna         = _num(p, "klient_moc_umowna")        # [MW]
    soc_start_pct      = _num(p, "bess_soc_start")           # [% całego BESS]
    soc_min_pct        = _num(p, "bess_min_soc")             # [% całego BESS]
    soc_max_pct        = _num(p, "bess_max_soc")             # [% całego BESS]

    if t_ch_h <= 0.0 or t_dis_h <= 0.0:
        raise ValueError("Czasy 'bess_c_rate_charge' i 'bess_c_rate_discharge' muszą być > 0 h.")

    # 3) PRZELICZENIA (wyłącznie dozwolone konwersje jednostek)
    # sprawności w [0..1]
    eta_ch  = eta_ch_pct  / 100.0
    eta_dis = eta_dis_pct / 100.0

    # samorozładowanie: %/miesiąc -> ułamek/h (30 dni * 24 h)
    self_dis_per_h = (lambda_month_pct / 100.0) / (30.0 * 24.0)

    # udział OZE
    share_oze = max(0.0, min(1.0, 1.0 - p_arbi_pct / 100.0))

    # z czasu [h] -> C [h^-1] oraz moc [MW] = E[MWh] / T[h]
    c_ch_h  = 1.0 / t_ch_h
    c_dis_h = 1.0 / t_dis_h
    p_ch_mw  = emax / t_ch_h
    p_dis_mw = emax / t_dis_h

    # Limity SOC całego BESS (MWh)
    bess_soc_min_mwh = (soc_min_pct / 100.0) * emax
    bess_soc_max_mwh = (soc_max_pct / 100.0) * emax

    # Pojemności torów
    emax_oze  = share_oze * emax
    emax_arbi = (1.0 - share_oze) * emax

    # Startowy SOC całego BESS [MWh], podział proporcjonalny do udziału
    soc_total_mwh = (soc_start_pct / 100.0) * emax
    soc_init_oze  = share_oze * soc_total_mwh
    soc_init_arbi = (1.0 - share_oze) * soc_total_mwh

    # 4) Konstrukcja obiektów parametrów
    bess = BessParams(
        emax_mwh=emax,
        c_rate_ch_mw=p_ch_mw,
        c_rate_dis_mw=p_dis_mw,
        eta_ch=eta_ch,
        eta_dis=eta_dis,
        self_discharge_per_h=self_dis_per_h,
        soc_min_mwh=bess_soc_min_mwh,
        soc_max_mwh=bess_soc_max_mwh,
    )

    oze = TrackParams(
        emax_mwh=emax_oze,
        c_rate_ch_mw=bess.c_rate_ch_mw,
        c_rate_dis_mw=bess.c_rate_dis_mw,
        eta_ch=bess.eta_ch,
        eta_dis=bess.eta_dis,
        self_discharge_per_h=bess.self_discharge_per_h,
        soc_min_mwh=0.0,
        soc_max_mwh=emax_oze,
        soc_init_mwh=soc_init_oze,
    )

    arbi = TrackParams(
        emax_mwh=emax_arbi,
        c_rate_ch_mw=bess.c_rate_ch_mw,
        c_rate_dis_mw=bess.c_rate_dis_mw,
        eta_ch=bess.eta_ch,
        eta_dis=bess.eta_dis,
        self_discharge_per_h=bess.self_discharge_per_h,
        soc_min_mwh=0.0,
        soc_max_mwh=emax_arbi,
        soc_init_mwh=soc_init_arbi,
    )

    params = Params(
        bess=bess,
        share_oze=share_oze,
        oze=oze,
        arbi=arbi,
        moc_umowna_mw=moc_umowna,
        arbi_price_low=price_low,
        arbi_price_high=price_high,
    )

    # 5) Log diagnostyczny (z podaniem czasu, c i mocy)
    log.info(
        "Params ready: emax=%.3f, share_oze=%.3f | "
        "OZE[SOC min=%.3f,max=%.3f,init=%.3f] | "
        "ARBI[SOC min=%.3f,max=%.3f,init=%.3f] | "
        "czas[h](ch=%.3f,dis=%.3f) -> c[h^-1](ch=%.3f,dis=%.3f) -> P[MW](ch=%.3f,dis=%.3f) | "
        "eta[ch=%.3f,dis=%.3f] | self_dis/h=%.8f (z %.3f%%/mies.) | "
        "price[low=%.2f,high=%.2f] | moc_umowna=%.3f MW",
        params.emax, params.share_oze,
        params.oze.soc_min_mwh, params.oze.soc_max_mwh, params.oze.soc_init_mwh,
        params.arbi.soc_min_mwh, params.arbi.soc_max_mwh, params.arbi.soc_init_mwh,
        t_ch_h, t_dis_h, c_ch_h, c_dis_h, p_ch_mw, p_dis_mw,
        params.bess.eta_ch, params.bess.eta_dis,
        params.bess.self_discharge_per_h, lambda_month_pct,
        price_low, price_high, moc_umowna
    )

    # 6) Walidacja spójności
    assert params.bess.emax_mwh > 0.0
    assert 0.0 <= params.share_oze <= 1.0
    assert 0.0 <= params.bess.soc_min_mwh < params.bess.soc_max_mwh <= params.emax
    assert 0.0 <= params.oze.soc_init_mwh  <= params.oze.soc_max_mwh
    assert 0.0 <= params.arbi.soc_init_mwh <= params.arbi.soc_max_mwh
    return params
