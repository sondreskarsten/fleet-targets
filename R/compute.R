DNB_ORGNRS <- c("984851006", "816521432", "920953743", "858043042", "985621551", "914782007")

compute_bank_segments <- function(losore_file) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "SET memory_limit='8GB'")

  dnb_list <- paste0("'", DNB_ORGNRS, "'", collapse = ",")

  DBI::dbGetQuery(con, sprintf("
    SELECT orgnr,
      MAX(CASE WHEN bank_orgnr IN (%s) OR bank_name ILIKE '%%dnb%%' THEN 1 ELSE 0 END) AS is_dnb,
      MAX(CASE WHEN bank_orgnr NOT IN (%s) AND bank_name NOT ILIKE '%%dnb%%' AND bank_orgnr IS NOT NULL THEN 1 ELSE 0 END) AS is_other_bank,
      SUM(CASE WHEN bank_orgnr IN (%s) OR bank_name ILIKE '%%dnb%%' THEN belop_nok ELSE 0 END) AS dnb_belop,
      SUM(CASE WHEN bank_orgnr NOT IN (%s) AND bank_name NOT ILIKE '%%dnb%%' THEN belop_nok ELSE 0 END) AS other_belop,
      COUNT(DISTINCT dokumentnummer) AS n_liens,
      STRING_AGG(DISTINCT CASE WHEN bank_orgnr NOT IN (%s) AND bank_name NOT ILIKE '%%dnb%%' AND bank_name IS NOT NULL THEN bank_name END, '; ') AS other_bank_names
    FROM (
      SELECT orgnr, dokumentnummer,
        json_extract_string(full_json, '$.krav.belop[0].belop')::BIGINT AS belop_nok,
        json_extract_string(rolle.json, '$.rolleinnehaver.navn') AS bank_name,
        json_extract_string(rolle.json, '$.rolleinnehaver.organisasjonsnummer') AS bank_orgnr
      FROM read_parquet('%s'),
        LATERAL (SELECT unnest(from_json(json_extract(full_json, '$.roller'), '[\"JSON\"]')) AS json) AS rolle
      WHERE orgnr IS NOT NULL
        AND json_extract_string(rolle.json, '$.rollegruppetype') = 'rollegruppe.rett'
    ) sub
    GROUP BY orgnr
  ", dnb_list, dnb_list, dnb_list, dnb_list, dnb_list, losore_file))
}

compute_latest_events <- function(ledger) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  duckdb::duckdb_register(con, "ledger", ledger)

  DBI::dbGetQuery(con, "
    WITH ranked AS (
      SELECT orgnr, data_source, summary, valid_time,
        ROW_NUMBER() OVER (PARTITION BY orgnr, data_source ORDER BY valid_time DESC) AS rn
      FROM ledger
      WHERE data_source IN ('fiskeridir_vessel','roller','enheter','kunngjoring','doffin')
        AND orgnr IS NOT NULL
    )
    SELECT orgnr,
      MAX(CASE WHEN data_source='fiskeridir_vessel' AND rn=1 THEN summary END) AS latest_vessel_event,
      MAX(CASE WHEN data_source='fiskeridir_vessel' AND rn=1 THEN valid_time END) AS latest_vessel_event_ts,
      MAX(CASE WHEN data_source='roller' AND rn=1 THEN summary END) AS latest_roller_event,
      MAX(CASE WHEN data_source='roller' AND rn=1 THEN valid_time END) AS latest_roller_event_ts,
      MAX(CASE WHEN data_source='enheter' AND rn=1 THEN summary END) AS latest_enheter_event,
      MAX(CASE WHEN data_source='enheter' AND rn=1 THEN valid_time END) AS latest_enheter_event_ts,
      MAX(CASE WHEN data_source='kunngjoring' AND rn=1 THEN summary END) AS latest_kunngj_event,
      MAX(CASE WHEN data_source='kunngjoring' AND rn=1 THEN valid_time END) AS latest_kunngj_event_ts,
      COUNT(*) FILTER (WHERE data_source='kunngjoring') AS kunngj_count_60d
    FROM ranked WHERE rn = 1 GROUP BY orgnr
  ")
}

compute_catch_agg <- function(fangstdata_files, fartoy) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "SET memory_limit='8GB'")
  duckdb::duckdb_register(con, "fartoy", fartoy)

  file_list <- paste0("'", fangstdata_files, "'", collapse = ",")
  DBI::dbGetQuery(con, sprintf("
    SELECT fartoy_id, fr.orgnr, f.fangstar::INT AS year,
      COALESCE(fr.radio_call_sign, f.radiokallesignal_seddel) AS callsign,
      MAX(COALESCE(fr.name, f.fartoynavn)) AS vessel_name,
      MAX(f.lengdegruppe) AS length_group,
      STRING_AGG(DISTINCT f.redskap_hovedgruppe, '/' ORDER BY f.redskap_hovedgruppe) AS gear_types,
      MAX(f.redskap_hovedgruppe) AS primary_gear,
      round(sum(f.rundvekt)/1000, 1) AS catch_tonnes,
      round(sum(CASE WHEN f.fangstverdi > 0 THEN f.fangstverdi ELSE 0 END)/1000, 0) AS catch_value_knok,
      count(DISTINCT f.siste_fangstdato) AS landing_days,
      count(DISTINCT f.art) AS n_species,
      max(f.besetning) AS max_crew,
      max(f.bruttotonnasje_annen) AS gt
    FROM read_parquet([%s]) f
    LEFT JOIN fartoy fr ON f.radiokallesignal_seddel = fr.radio_call_sign
    WHERE f.fangstar::INT BETWEEN 2020 AND 2025
    GROUP BY fartoy_id, fr.orgnr, f.fangstar::INT,
      COALESCE(fr.radio_call_sign, f.radiokallesignal_seddel)
  ", file_list))
}

compute_ref_groups <- function(catch_agg) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  duckdb::duckdb_register(con, "ca", catch_agg)

  DBI::dbGetQuery(con, "
    SELECT length_group, primary_gear AS gear_type,
      count(*) AS ref_n_vessels,
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY catch_tonnes) AS ref_catch_p25,
      PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY catch_tonnes) AS ref_catch_p50,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY catch_tonnes) AS ref_catch_p75,
      PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY catch_value_knok) AS ref_value_p50,
      PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY landing_days) AS ref_landing_days_p50
    FROM ca WHERE year = 2024 AND orgnr IS NOT NULL AND catch_tonnes > 0
    GROUP BY length_group, primary_gear
  ")
}

compute_finstat_clean <- function(finstat_file) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  DBI::dbGetQuery(con, sprintf("
    SELECT LPAD(CAST(CAST(OffentligNr AS BIGINT) AS VARCHAR), 9, '0') AS orgnr,
      Regnskapsar AS year,
      TotaleInntekter/1000.0 AS revenue_knok,
      SumDriftskostnader/1000.0 AS costs_knok,
      Driftsresultat/1000.0 AS ebitda_knok,
      Arsresultat/1000.0 AS net_income_knok,
      SumEiendeler/1000.0 AS total_assets_knok,
      SumEK/1000.0 AS equity_knok,
      Lonnskostnad/1000.0 AS wage_cost_knok
    FROM read_parquet('%s')
    WHERE Regnskapsar BETWEEN 2020 AND 2024 AND RegnskapstypeKode = 'R'
  ", finstat_file))
}

materialize_fleet_panel <- function(catch_agg, ais_stats, finstat_clean, live, nsr, bank_segments) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "SET memory_limit='8GB'")
  duckdb::duckdb_register(con, "ca", catch_agg)
  duckdb::duckdb_register(con, "ais", ais_stats)
  duckdb::duckdb_register(con, "fs", finstat_clean)
  duckdb::duckdb_register(con, "live", live)
  duckdb::duckdb_register(con, "nsr", nsr)
  duckdb::duckdb_register(con, "bs", bank_segments)

  DBI::dbGetQuery(con, "
    SELECT ca.*,
      ais.ais_days, ais.n_positions, ais.positions_underway, ais.ais_first_seen, ais.ais_last_seen,
      fs.revenue_knok, fs.costs_knok, fs.ebitda_knok, fs.net_income_knok,
      fs.total_assets_knok, fs.equity_knok, fs.wage_cost_knok,
      live.latitude AS live_lat, live.longitude AS live_lon, live.sog AS live_sog,
      live.nav_status AS live_nav_status, live.captured_at AS live_captured_at,
      CASE
        WHEN bs.is_dnb=1 AND bs.is_other_bank=0 THEN 'DNB only'
        WHEN bs.is_dnb=1 AND bs.is_other_bank=1 THEN 'DNB + other'
        WHEN bs.is_dnb=0 AND bs.is_other_bank=1 THEN 'Other bank'
        WHEN bs.orgnr IS NOT NULL THEN 'No bank lien'
      END AS bank_segment,
      bs.dnb_belop, bs.other_belop, bs.n_liens, bs.other_bank_names
    FROM ca
    LEFT JOIN ais ON ca.orgnr = ais.orgnr
    LEFT JOIN fs ON ca.orgnr = fs.orgnr AND ca.year = fs.year
    LEFT JOIN nsr ON ca.callsign = nsr.callsign
    LEFT JOIN live ON nsr.mmsino = live.mmsi
    LEFT JOIN bs ON ca.orgnr = bs.orgnr
    WHERE ca.orgnr IS NOT NULL
  ")
}

materialize_portfolio_vessel <- function(fartoy, catch_agg, ais_stats, live, nsr, finstat_clean,
                                         bank_segments, ref_groups, latest_events) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "SET memory_limit='8GB'")
  duckdb::duckdb_register(con, "fartoy", fartoy)
  duckdb::duckdb_register(con, "catch_agg", catch_agg)
  duckdb::duckdb_register(con, "ais", ais_stats)
  duckdb::duckdb_register(con, "live", live)
  duckdb::duckdb_register(con, "nsr", nsr)
  duckdb::duckdb_register(con, "fs", finstat_clean)
  duckdb::duckdb_register(con, "bs", bank_segments)
  duckdb::duckdb_register(con, "rg", ref_groups)
  duckdb::duckdb_register(con, "le", latest_events)

  DBI::dbGetQuery(con, "
    WITH cur AS (SELECT * FROM catch_agg WHERE year = 2024),
    prior AS (SELECT ca.fartoy_id, ca.catch_tonnes AS prior_catch, ca.catch_value_knok AS prior_value,
      ca.landing_days AS prior_days, fs.net_income_knok AS prior_ni
      FROM catch_agg ca LEFT JOIN fs ON ca.orgnr = fs.orgnr AND ca.year = fs.year
      WHERE ca.year = 2023)
    SELECT
      f.vessel_id, f.orgnr, f.name AS vessel_name, f.radio_call_sign AS callsign,
      nsr.mmsino AS mmsi, f.municipality_code,
      f.length, f.build_year, f.rebuild_year, f.engine_power_kw, f.tonnage_gt AS gt,
      2026 - f.build_year AS vessel_age,
      c.length_group, c.gear_types, c.primary_gear,
      c.catch_tonnes, c.catch_value_knok, c.landing_days, c.n_species, c.max_crew,
      p.prior_catch, p.prior_value, p.prior_days,
      round(CASE WHEN p.prior_catch > 0 THEN 100.0*(c.catch_tonnes-p.prior_catch)/p.prior_catch END, 1) AS yoy_catch_pct,
      round(CASE WHEN ais.ais_days > 0 THEN c.catch_tonnes / ais.ais_days END, 1) AS cpue,
      round(CASE WHEN c.catch_tonnes > 0 THEN c.catch_value_knok*1000.0/c.catch_tonnes END, 1) AS nok_per_tonne,
      ais.ais_days, ais.n_positions, ais.positions_underway, ais.ais_first_seen, ais.ais_last_seen,
      live.latitude AS live_lat, live.longitude AS live_lon, live.sog AS live_sog,
      live.nav_status AS live_nav_status, live.captured_at AS live_captured_at,
      fsc.revenue_knok, fsc.ebitda_knok, fsc.net_income_knok, fsc.total_assets_knok,
      fsc.equity_knok, fsc.wage_cost_knok,
      p.prior_ni AS prior_net_income_knok,
      CASE
        WHEN bs.is_dnb=1 AND bs.is_other_bank=0 THEN 'DNB only'
        WHEN bs.is_dnb=1 AND bs.is_other_bank=1 THEN 'DNB + other'
        WHEN bs.is_dnb=0 AND bs.is_other_bank=1 THEN 'Other bank'
        WHEN bs.orgnr IS NOT NULL THEN 'No bank lien'
      END AS bank_segment,
      bs.dnb_belop, bs.other_belop, bs.n_liens, bs.other_bank_names,
      rg.ref_n_vessels,
      round(rg.ref_catch_p25,0) AS ref_catch_p25, round(rg.ref_catch_p50,0) AS ref_catch_p50,
      round(rg.ref_catch_p75,0) AS ref_catch_p75, round(rg.ref_value_p50,0) AS ref_value_p50,
      round(rg.ref_landing_days_p50,0) AS ref_landing_days_p50,
      round(CASE WHEN rg.ref_catch_p50 > 0 THEN c.catch_tonnes/rg.ref_catch_p50 END, 2) AS vs_ref_catch_ratio,
      le.latest_vessel_event, le.latest_vessel_event_ts,
      le.latest_roller_event, le.latest_roller_event_ts,
      le.latest_enheter_event, le.latest_enheter_event_ts,
      le.latest_kunngj_event, le.latest_kunngj_event_ts,
      le.kunngj_count_60d
    FROM fartoy f
    LEFT JOIN nsr ON f.radio_call_sign = nsr.callsign
    LEFT JOIN cur c ON f.orgnr = c.orgnr AND f.vessel_id::VARCHAR = c.fartoy_id
    LEFT JOIN prior p ON c.fartoy_id = p.fartoy_id
    LEFT JOIN ais ON f.orgnr = ais.orgnr
    LEFT JOIN live ON nsr.mmsino = live.mmsi
    LEFT JOIN bs ON f.orgnr = bs.orgnr
    LEFT JOIN rg ON c.length_group = rg.length_group AND c.primary_gear = rg.gear_type
    LEFT JOIN le ON f.orgnr = le.orgnr
    LEFT JOIN fs fsc ON f.orgnr = fsc.orgnr AND 2024 = fsc.year
    WHERE f.orgnr IS NOT NULL
  ")
}

materialize_capacity_util <- function(fleet_panel) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  duckdb::duckdb_register(con, "fp", fleet_panel)

  DBI::dbGetQuery(con, "
    SELECT year, length_group, primary_gear AS gear_type,
      count(DISTINCT fartoy_id) AS n_vessels,
      round(avg(catch_tonnes), 1) AS avg_catch_tonnes,
      round(avg(catch_value_knok), 0) AS avg_catch_value_knok,
      round(avg(landing_days), 0) AS avg_landing_days,
      round(avg(ais_days), 0) AS avg_ais_das,
      round(avg(CASE WHEN ais_days > 0 THEN catch_tonnes / ais_days END), 2) AS cpue_tonnes_per_das,
      round(avg(revenue_knok), 0) AS avg_revenue_knok,
      round(avg(ebitda_knok), 0) AS avg_ebitda_knok,
      round(avg(net_income_knok), 0) AS avg_net_income_knok,
      round(avg(n_species), 1) AS avg_species_count,
      round(avg(max_crew), 1) AS avg_crew
    FROM fp WHERE orgnr IS NOT NULL
    GROUP BY year, length_group, primary_gear
    ORDER BY year, length_group, primary_gear
  ")
}
