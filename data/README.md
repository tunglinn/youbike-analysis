# YouBike 2.0 rental data (Taipei)

Source catalog: `catalog_index.csv` (converted to UTF-8 from the Big5-encoded
file the city publishes at 臺北市政府資料開放平台). Each row is one month's
data with a download URL, e.g.:

```
https://tcgbusfs.blob.core.windows.net/dotapp/youbike_second_ticket_opendata/{year}/{year}-{month}/{yyyymm}_YouBike2.0票證刷卡資料.zip
```

Catalog covers **2020-04 through 2026-07** (76 months) as of the 2026-09-11
update.

## CLI

Everything below can be reproduced with `youbike_cli.py` (project root):

```
uv venv .venv && source .venv/bin/activate && uv pip install pyarrow h3 duckdb

python youbike_cli.py catalog --catalog-csv "/path/to/city's catalog.csv" --show 5
python youbike_cli.py fetch-latest --catalog-csv "/path/to/city's catalog.csv" --data-dir data
python youbike_cli.py fetch-month --month 2026-07 --data-dir data
python youbike_cli.py stations --data-dir data --resolutions 7,8,9
python youbike_cli.py top-stations --data-dir data --n 10
```

`fetch-month`/`fetch-latest` download straight to a temp file, extract, convert
to a Parquet fact table (no H3 columns), then delete the zip and raw CSV by
default (`--keep-zip` / `--keep-raw-csv` to retain them). `stations` rebuilds
`stations.parquet` from the nationwide live API. `top-stations` runs the
DuckDB join demo. Verified end-to-end against a scratch directory: output was
byte-identical to the manually-built files documented below (37,563,126 bytes,
7,596,992 rows, same 99.78% match rate).

## Downloaded so far

- `202607_YouBike.csv.gz` — July 2026 (latest available month), gzip -9
  compressed CSV.

Columns: `rent_time, rent_station, return_time, return_station, rent (HH:MM:SS
duration), bike_type (一般車/電輔車), infodate`.

Read with `pandas.read_csv("202607_YouBike.csv.gz")` (pandas handles gzip
transparently) or `duckdb.sql("select * from read_csv('202607_YouBike.csv.gz')")`.

## Per-month sizing (July 2026, representative)

| Form | Size |
|---|---|
| Zip (as published) | 132.7 MB |
| Raw CSV (extracted) | 886.4 MB |
| Gzip -9 CSV (what we kept) | 127.1 MB |
| Records | 7,596,992 |


## Parquet prototype (measured, not estimated)

Built with `pyarrow` in `.venv` (`uv venv` + `uv pip install pyarrow h3 duckdb`).
Column types: `rent_time`/`return_time` as timestamp, `infodate` as date32,
`rent` kept as the original HH:MM:SS string.

An earlier version of this prototype embedded H3 columns directly in the
fact table (`rent_h3_9`, `return_h3_9` on every row). That design is
**superseded** by the dimension-table approach below — kept here as the
"why not" record:

- Redundant: only ~3,000 distinct stations exist, but the H3 value was
  repeated across all 7.6M rows.
- Wasteful to extend: adding another resolution meant rewriting the whole
  multi-GB fact table.
- Storing the H3 cell as a native `uint64` instead of its 15-char hex string
  made no measurable size difference (zstd already dictionary-encodes the
  repeated strings) — not worth the conversion step either way.
- Partitioning that fact table by day (Hive layout) actually *grew* it to
  121 MB (30 files) vs. 57.5 MB as one file, because each daily file rebuilds
  its own compression dictionary instead of sharing one globally. If you do
  partition, do it by **month** (the level people actually query at), not day.

### Current design: lean fact table + station dimension table

- `202607_baseline.parquet` — the fact table, **no H3 columns**. Just
  `rent_time, rent_station, return_time, return_station, rent, bike_type,
  infodate`. 37.6 MB (4.2% of raw CSV, 30% of gzip CSV).
- `stations.parquet` — the dimension table: one row per station
  (`station_no, name_tw, district_tw, lat, lon, h3_7, h3_8, h3_9`). **362 KB**
  for 9,473 stations nationwide, all three resolutions included.

H3 is derived at query time via a join, e.g. (actually run against these two
files with DuckDB):

```sql
select s.h3_9, s.name_tw, count(*) as rentals
from read_parquet('202607_baseline.parquet') f
join read_parquet('stations.parquet') s on f.rent_station = s.name_tw
group by 1, 2
order by rentals desc
limit 10
```

Ran in 0.09s over 7.6M rows. A full match-rate check (`left join` +
`count(s.name_tw)`) reproduces the same **7,580,632 / 7,596,992 = 99.78%**
row-level match found in the embedded-column version — same coverage, no
duplication.

Why this is the better design:
- Adding a resolution (or fixing a station's coordinates) is a ~360 KB
  rewrite of `stations.parquet`, not a rewrite of the multi-GB fact table.
- The fact table stays identical in size whether you need H3 or not —
  H3 is an optional enrichment applied at query time, not a property baked
  into stored rows.
- Total storage (37.6 MB + 0.36 MB) is smaller than the embedded version
  (57.5 MB) and *shrinks relatively as more months are added*, since
  `stations.parquet` doesn't grow with row count.

Trade-off: every H3-aware query needs a join. Negligible in practice here
(0.09s for a 7.6M-row aggregation via DuckDB), and DuckDB/Parquet make the
join itself nearly free since `stations.parquet` is small enough to fit
entirely in memory and gets broadcast.

### Station list caveats (apply regardless of storage design)

- Taipei's own live station feed (`tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json`,
  1,803 stations) matched only **57%** of July's 3,064 unique station names —
  it excludes New Taipei stations, which Taipei riders do use.
- The **nationwide YouBike API** (`apis.youbike.com.tw/json/station-yb2.json`,
  9,592 stations, field `name_tw` + `lat`/`lng`) is what `stations.parquet`
  is built from — 99.2% of unique names / 99.78% of rows matched.
- Unmatched names are almost all depot/maintenance entries (`XX維護所`) or a
  handful with mangled characters in the source CSV itself — not real
  stations, safe to leave null.
- This is a **live snapshot** joined against a **historical** month. For real
  multi-year use, snapshot `stations.parquet` monthly (or whenever new
  rental data is pulled) rather than re-joining old months against today's
  list, since stations get renamed, relocated, or closed — the dimension
  table would then need an `effective_date` range per station rather than
  being a single flat snapshot. A `station_id` join key would be far more
  robust than name matching if the rental export ever includes one — worth
  checking the station feed's `sno`/`station_no` against future rental
  exports.

### Updated multi-year projections (measured monthly rate × 76 months)

| Form | Full history (76 mo) |
|---|---|
| Raw CSV | ~67.3 GB |
| Gzip CSV | ~9.65 GB |
| Parquet, lean fact table (no H3) | ~2.86 GB |
| + `stations.parquet` (H3 via join, not embedded) | ~2.86 GB + 0.36 MB (flat, doesn't scale with months) |

