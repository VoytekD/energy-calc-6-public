-- Schemat OUTPUT: tabele *detaliczne* dla OZE/ARBI/BROKER

CREATE SCHEMA IF NOT EXISTS output;

-- Tabela: szczegóły OZE (krok czasowy)
CREATE TABLE IF NOT EXISTS output.energy_oze_detail (
    ts_start                     timestamp without time zone NOT NULL,
    ts_end                       timestamp without time zone NOT NULL,
    step_hours                   numeric NOT NULL,

    soc_start_mwh               numeric,
    soc_end_mwh                 numeric,

    p_ch_mw                     numeric,
    p_dis_mw                    numeric,
    e_ch_mwh                    numeric,
    e_dis_mwh                   numeric,

    loss_conv_mwh               numeric,
    loss_idle_mwh               numeric,
    loss_total_mwh              numeric,

    spill_surplus_mwh           numeric,
    unmet_deficit_mwh           numeric,

    soc_gap_to_min_start_mwh    numeric,
    soc_gap_to_min_end_mwh      numeric,
    time_below_min_h            numeric,

    hit_part_cap_max            boolean,
    hit_part_cap_min            boolean
);

-- Tabela: szczegóły ARBI (krok czasowy) – z finansami
CREATE TABLE IF NOT EXISTS output.energy_arbi_detail (
    ts_start                     timestamp without time zone NOT NULL,
    ts_end                       timestamp without time zone NOT NULL,
    step_hours                   numeric NOT NULL,

    soc_start_mwh               numeric,
    soc_end_mwh                 numeric,

    p_ch_mw                     numeric,
    p_dis_mw                    numeric,
    e_ch_mwh                    numeric,
    e_dis_mwh                   numeric,

    loss_conv_mwh               numeric,
    loss_idle_mwh               numeric,
    loss_total_mwh              numeric,

    price_pln_mwh               numeric,
    cost_pln                    numeric,
    revenue_pln                 numeric,
    net_value_pln               numeric,

    soc_gap_to_min_start_mwh    numeric,
    soc_gap_to_min_end_mwh      numeric,
    time_below_min_h            numeric,

    hit_part_cap_max            boolean,
    hit_part_cap_min            boolean
);

-- Tabela: broker (alokacja mocy)
CREATE TABLE IF NOT EXISTS output.energy_broker_detail (
    ts_start                timestamp without time zone NOT NULL,
    ts_end                  timestamp without time zone NOT NULL,
    step_hours              numeric NOT NULL,

    req_ch_oze_mw           numeric,
    req_dis_oze_mw          numeric,
    req_ch_arbi_mw          numeric,
    req_dis_arbi_mw         numeric,

    cap_ch_mw               numeric,
    cap_dis_mw              numeric,
    cap_contract_mw         numeric,

    alloc_ch_oze_mw         numeric,
    alloc_dis_oze_mw        numeric,
    alloc_ch_arbi_mw        numeric,
    alloc_dis_arbi_mw       numeric,

    note                    text
);
