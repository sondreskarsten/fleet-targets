# fleet-targets

R `targets` pipeline that materializes the fishing fleet gold layer from upstream CDC sources. Reads 8 GCS inputs, computes bank segments, reference groups, and latest corporate events, then writes 3 gold-layer parquets.

## Overview

| | |
|---|---|
| **What** | Daily gold-layer materialization for fishing fleet monitoring |
| **Schedule** | 09:00 Oslo daily (after all upstream parsers) |
| **Runtime** | ~4 min |
| **Input** | 8 GCS paths (fartøy, fangstdata, AIS stats, live, NSR, løsøre, finstat, ledger) |
| **Output** | `fleet_panel.parquet` + `portfolio_vessel.parquet` + `capacity_utilization.parquet` |
| **Metadata** | targets `_meta` persisted to GCS between runs for incremental builds |

## DAG

```
fartoy ──────────────┬── catch_agg ──┬── fleet_panel ──── upload
fangstdata ──────────┘               │                    ↓
                                     ├── ref_groups       capacity_util ── upload
ais_stats ───────────────────────────┤
live ────────────────────────────────┤
nsr ─────────────────────────────────┤
losore ──── bank_segments ───────────┤
finstat ─── finstat_clean ───────────┤
ledger ──── latest_events ───────────┴── portfolio_vessel ── upload
```

## Gold outputs

| File | Rows | Cols | LUAS |
|---|---|---|---|
| `fleet_panel.parquet` | ~34K | 36 | (fartoy_id, orgnr, year, gear_type) |
| `portfolio_vessel.parquet` | ~4,665 | 64 | (vessel_id) — one per vessel |
| `capacity_utilization.parquet` | ~116 | 15 | (year, length_group, gear_type) |

## Cloud Run

Job `fleet-targets`, 4CPU/16Gi, r-base image. Scheduler 09:00 Oslo.
