-- Godzinowy widok magazynu (granulacja jak w *_detail)
-- + SOC [%] względem własnej części i względem całego Emax

CREATE OR REPLACE VIEW output.energy_store_summary AS
WITH p AS (
  SELECT
    emax::numeric                      AS emax_mwh,
    procent_arbitrazu::numeric         AS procent_arbitrazu,        -- w %
    (procent_arbitrazu::numeric)/100.0 AS share_arbi,
    1.0 - (procent_arbitrazu::numeric)/100.0 AS share_oze
  FROM params.form_zmienne
  ORDER BY updated_at DESC
  LIMIT 1
),
j AS (
  SELECT
    o.ts_start,
    o.ts_end,
    EXTRACT(HOUR FROM o.ts_start)::int AS hour,

    -- przepływy OZE (MWh)
    o.e_ch_mwh       AS oze_e_ch_mwh,
    o.e_dis_mwh      AS oze_e_dis_mwh,
    o.loss_total_mwh AS oze_losses_mwh,

    -- przepływy Arbitraż (MWh)
    a.e_ch_mwh       AS arbi_e_ch_mwh,
    a.e_dis_mwh      AS arbi_e_dis_mwh,
    a.loss_total_mwh AS arbi_losses_mwh,

    -- (jeśli masz te pola w arbi_detail, zostaną wystawione 1:1)
    a.cost_pln       AS arbi_cost_pln,
    a.revenue_pln    AS arbi_revenue_pln,
    a.net_value_pln  AS arbi_net_pln,

    -- stany SOC na koniec godziny (MWh)
    o.soc_end_mwh    AS soc_oze_mwh,
    a.soc_end_mwh    AS soc_arbi_mwh
  FROM output.energy_oze_detail  o
  JOIN output.energy_arbi_detail a
    ON a.ts_start = o.ts_start AND a.ts_end = o.ts_end
)
SELECT
  j.*,

  -- parametry / pojemności z 2 miejscami
  ROUND(p.emax_mwh, 2)                        AS emax_mwh,
  p.procent_arbitrazu,                        -- to masz jako integer, OK
  ROUND(p.share_oze, 2)                       AS share_oze,
  ROUND(p.share_arbi, 2)                      AS share_arbi,
  ROUND(p.emax_mwh * p.share_oze,  2)         AS emax_oze_mwh,
  ROUND(p.emax_mwh * p.share_arbi, 2)         AS emax_arbi_mwh,

  -- SOC [%] względem własnej części (2 miejsca)
  ROUND((j.soc_oze_mwh  / (p.emax_mwh * p.share_oze))  * 100, 2) AS soc_oze_pct,
  ROUND((j.soc_arbi_mwh / (p.emax_mwh * p.share_arbi)) * 100, 2) AS soc_arbi_pct,

  -- SOC [%] względem całego Emax (2 miejsca)
  ROUND((j.soc_oze_mwh  / p.emax_mwh) * 100, 2)                  AS soc_oze_total_pct,
  ROUND((j.soc_arbi_mwh / p.emax_mwh) * 100, 2)                  AS soc_arbi_total_pct,
  ROUND(((j.soc_oze_mwh + j.soc_arbi_mwh) / p.emax_mwh) * 100, 2) AS soc_total_pct
FROM j
CROSS JOIN p
ORDER BY j.ts_start;
