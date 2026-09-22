# youbike-analysis

Tools for pulling Taipei's public YouBike 2.0 rental records and turning them
into a Parquet layout suited to multi-year analysis (fact table of trips +
a small station dimension table with H3 spatial indexes).

## The raw data

Taipei City publishes monthly YouBike 2.0 rental records (每月票證刷卡資料) as
zipped CSVs. A catalog CSV lists one row per month with a download URL; as of
the 2026-09-11 update it covers **2020-04 through 2026-07** (76 months).

Each month's CSV is one row per trip:

| Column | Description |
|---|---|
| `rent_time` | rental timestamp |
| `rent_station` | station name (rented from) |
| `return_time` | return timestamp |
| `return_station` | station name (returned to) |
| `rent` | trip duration, `HH:MM:SS` |
| `bike_type` | 一般車 (standard) or 電輔車 (e-assist) |
| `infodate` | date the record belongs to |

July 2026 alone is **7.6M rows** (886 MB raw CSV, 127 MB gzipped, 37.6 MB as
Parquet). See [`data/README.md`](data/README.md) for full sizing, the
Parquet/H3 design rationale, and station-matching caveats.

## `youbike_cli.py`

```
uv venv .venv && source .venv/bin/activate && uv pip install pyarrow h3 duckdb

# list months available in a downloaded catalog CSV
python youbike_cli.py catalog --catalog-csv "/path/to/catalog.csv" --show 5

# download + convert one month (or the newest one in the catalog) to Parquet
python youbike_cli.py fetch-month --month 2026-07 --data-dir data
python youbike_cli.py fetch-latest --catalog-csv "/path/to/catalog.csv" --data-dir data

# build/refresh the station dimension table (name, lat/lon, H3 at res 7/8/9)
python youbike_cli.py stations --data-dir data

# demo query: top stations by rentals, H3 cell via a join to stations.parquet
python youbike_cli.py top-stations --data-dir data --n 10
```

`fetch-month`/`fetch-latest` produce one Parquet fact table per month with
**no H3 columns** — H3 is joined in from `stations.parquet` at query time
rather than duplicated across millions of rows. Rationale in
[`data/README.md`](data/README.md).

## Data isn't committed

`data/` is gitignored (except its `README.md`): the raw CSV alone exceeds
GitHub's 100 MB per-file limit, and Parquet output grows unbounded as more
months are added. Everything under `data/` is reproducible from the CLI —
run the commands above to regenerate it locally.
