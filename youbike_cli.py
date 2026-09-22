#!/usr/bin/env python3
"""CLI for pulling Taipei YouBike 2.0 rental data and building the
lean fact-table + station-dimension Parquet layout.

Requires: pyarrow, h3, duckdb (only for `top-stations`).
    uv venv .venv && source .venv/bin/activate && uv pip install pyarrow h3 duckdb

Subcommands:
    catalog        list months available in a locally downloaded catalog CSV
    fetch-month    download + convert one month (YYYY-MM) to a Parquet fact table
    fetch-latest   same, but picks the newest month from a catalog CSV
    stations       build/refresh the station dimension table (with H3 columns)
    top-stations   demo query: top rental stations by H3 cell, via a join
"""
import argparse
import csv
import io
import json
import os
import sys
import tempfile
import urllib.parse
import urllib.request
import zipfile
from datetime import date

ZIP_URL_TEMPLATE = (
    "https://tcgbusfs.blob.core.windows.net/dotapp/youbike_second_ticket_opendata/"
    "{year}/{year}-{month:02d}/{year}{month:02d}_YouBike2.0票證刷卡資料.zip"
)
DEFAULT_STATION_API = "https://apis.youbike.com.tw/json/station-yb2.json"
FACT_COLUMN_TYPES = {
    "rent_time": "timestamp",
    "return_time": "timestamp",
    "infodate": "date32",
}


def month_zip_url(year: int, month: int) -> str:
    fname = f"{year}{month:02d}_YouBike2.0票證刷卡資料.zip"
    base = f"https://tcgbusfs.blob.core.windows.net/dotapp/youbike_second_ticket_opendata/{year}/{year}-{month:02d}/"
    return base + urllib.parse.quote(fname)


def parse_catalog(path: str):
    """Parse the city's Big5-or-UTF8 catalog CSV into (date, url) rows, newest first."""
    raw = open(path, "rb").read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("big5", errors="replace")
    rows = list(csv.DictReader(io.StringIO(text)))
    entries = []
    for row in rows:
        fileinfo = row.get("fileinfo", "").strip()
        url = row.get("fileURL", "").strip()
        if not fileinfo or not url:
            continue
        y, m, _ = fileinfo.split("/")
        entries.append((date(int(y), int(m), 1), url))
    entries.sort(key=lambda e: e[0], reverse=True)
    return entries


def download(url: str, dest_path: str):
    print(f"downloading {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "youbike-cli"})
    with urllib.request.urlopen(req, timeout=60) as resp, open(dest_path, "wb") as out:
        total = 0
        while chunk := resp.read(1 << 20):
            out.write(chunk)
            total += len(chunk)
    print(f"  saved {total:,} bytes -> {dest_path}")


def extract_member(zip_path: str, extract_dir: str) -> str:
    """Extract the single CSV member, fixing the Big5-mangled filename."""
    with zipfile.ZipFile(zip_path) as zf:
        infos = zf.infolist()
        if len(infos) != 1:
            raise ValueError(f"expected exactly one member in {zip_path}, found {len(infos)}")
        info = infos[0]
        extracted_path = zf.extract(info, path=extract_dir)
    raw_name = info.filename
    try:
        fixed_name = raw_name.encode("cp437").decode("big5")
    except UnicodeError:
        fixed_name = raw_name
    fixed_path = os.path.join(extract_dir, fixed_name)
    if extracted_path != fixed_path:
        os.replace(extracted_path, fixed_path)
    return fixed_path


def csv_to_parquet(csv_path: str, parquet_path: str):
    import pyarrow as pa
    import pyarrow.csv as pv
    import pyarrow.parquet as pq

    read_opts = pv.ReadOptions(block_size=64 << 20)
    convert_opts = pv.ConvertOptions(
        column_types={
            "rent_time": pa.timestamp("s"),
            "return_time": pa.timestamp("s"),
            "infodate": pa.date32(),
        }
    )
    table = pv.read_csv(csv_path, read_options=read_opts, convert_options=convert_opts)
    pq.write_table(table, parquet_path, compression="zstd")
    return table.num_rows


def fetch_and_convert(url: str, year: int, month: int, data_dir: str, keep_zip: bool, keep_raw_csv: bool):
    os.makedirs(data_dir, exist_ok=True)
    yyyymm = f"{year}{month:02d}"
    zip_path = os.path.join(data_dir, f"{yyyymm}_YouBike.zip")
    download(url, zip_path)

    with tempfile.TemporaryDirectory(dir=data_dir) as tmp_dir:
        print("extracting...")
        csv_path = extract_member(zip_path, tmp_dir)

        parquet_path = os.path.join(data_dir, f"{yyyymm}_baseline.parquet")
        print(f"converting to {parquet_path} ...")
        n_rows = csv_to_parquet(csv_path, parquet_path)
        print(f"  {n_rows:,} rows")

        if keep_raw_csv:
            import gzip
            import shutil
            gz_path = os.path.join(data_dir, f"{yyyymm}_YouBike.csv.gz")
            with open(csv_path, "rb") as f_in, gzip.open(gz_path, "wb", compresslevel=9) as f_out:
                shutil.copyfileobj(f_in, f_out)
            print(f"  kept compressed source -> {gz_path}")

    if not keep_zip:
        os.remove(zip_path)
        print(f"  removed {zip_path}")

    print(f"parquet size: {os.path.getsize(parquet_path):,} bytes")


def build_stations(data_dir: str, api_url: str, resolutions):
    import h3
    import pyarrow as pa
    import pyarrow.parquet as pq

    print(f"fetching station list from {api_url}")
    req = urllib.request.Request(api_url, headers={"User-Agent": "youbike-cli"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        stations = json.load(resp)

    rows = {"station_no": [], "name_tw": [], "district_tw": [], "lat": [], "lon": []}
    for r in resolutions:
        rows[f"h3_{r}"] = []

    seen, dupes = set(), 0
    for s in stations:
        name = s.get("name_tw")
        lat, lng = s.get("lat"), s.get("lng")
        if not name or not lat or not lng:
            continue
        if name in seen:
            dupes += 1
            continue
        seen.add(name)
        lat_f, lon_f = float(lat), float(lng)
        rows["station_no"].append(s.get("station_no"))
        rows["name_tw"].append(name)
        rows["district_tw"].append(s.get("district_tw"))
        rows["lat"].append(lat_f)
        rows["lon"].append(lon_f)
        for r in resolutions:
            rows[f"h3_{r}"].append(h3.latlng_to_cell(lat_f, lon_f, r))

    table = pa.table(rows)
    os.makedirs(data_dir, exist_ok=True)
    out_path = os.path.join(data_dir, "stations.parquet")
    pq.write_table(table, out_path, compression="zstd")
    print(f"{table.num_rows:,} stations ({dupes} duplicate names skipped)")
    print(f"stations.parquet size: {os.path.getsize(out_path):,} bytes -> {out_path}")


def top_stations(data_dir: str, fact_path: str, n: int):
    import duckdb

    stations_path = os.path.join(data_dir, "stations.parquet")
    con = duckdb.connect()
    query = f"""
        select s.h3_9, s.name_tw, count(*) as rentals
        from read_parquet('{fact_path}') f
        join read_parquet('{stations_path}') s on f.rent_station = s.name_tw
        group by 1, 2
        order by rentals desc
        limit {n}
    """
    print(con.sql(query))

    coverage = con.sql(f"""
        select count(*) as total_rows, count(s.name_tw) as matched_rows
        from read_parquet('{fact_path}') f
        left join read_parquet('{stations_path}') s on f.rent_station = s.name_tw
    """).fetchone()
    total, matched = coverage
    print(f"station match: {matched:,}/{total:,} ({matched/total*100:.2f}%)")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p_catalog = sub.add_parser("catalog", help="list months available in a downloaded catalog CSV")
    p_catalog.add_argument("--catalog-csv", required=True, help="path to the city's catalog CSV (Big5 or UTF-8)")
    p_catalog.add_argument("--show", type=int, default=12, help="how many recent months to print (default 12)")

    p_month = sub.add_parser("fetch-month", help="download + convert one month")
    p_month.add_argument("--month", required=True, help="YYYY-MM")
    p_month.add_argument("--data-dir", default="data")
    p_month.add_argument("--keep-zip", action="store_true", help="keep the downloaded zip")
    p_month.add_argument("--keep-raw-csv", action="store_true", help="also keep a gzip copy of the raw CSV")

    p_latest = sub.add_parser("fetch-latest", help="download + convert the newest month in a catalog CSV")
    p_latest.add_argument("--catalog-csv", required=True)
    p_latest.add_argument("--data-dir", default="data")
    p_latest.add_argument("--keep-zip", action="store_true")
    p_latest.add_argument("--keep-raw-csv", action="store_true")

    p_stations = sub.add_parser("stations", help="build/refresh the station dimension table")
    p_stations.add_argument("--data-dir", default="data")
    p_stations.add_argument("--api-url", default=DEFAULT_STATION_API)
    p_stations.add_argument("--resolutions", default="7,8,9", help="comma-separated H3 resolutions")

    p_top = sub.add_parser("top-stations", help="demo: top stations by rentals, joined to H3 via stations.parquet")
    p_top.add_argument("--data-dir", default="data")
    p_top.add_argument("--fact", help="fact parquet path (default: newest *_baseline.parquet in --data-dir)")
    p_top.add_argument("--n", type=int, default=10)

    args = parser.parse_args()

    if args.command == "catalog":
        entries = parse_catalog(args.catalog_csv)
        print(f"{len(entries)} months available, newest {args.show}:")
        for d, url in entries[: args.show]:
            print(f"  {d:%Y-%m}  {url}")

    elif args.command == "fetch-month":
        year, month = (int(x) for x in args.month.split("-"))
        url = month_zip_url(year, month)
        fetch_and_convert(url, year, month, args.data_dir, args.keep_zip, args.keep_raw_csv)

    elif args.command == "fetch-latest":
        entries = parse_catalog(args.catalog_csv)
        if not entries:
            sys.exit("no entries found in catalog CSV")
        d, url = entries[0]
        print(f"latest month: {d:%Y-%m}")
        fetch_and_convert(url, d.year, d.month, args.data_dir, args.keep_zip, args.keep_raw_csv)

    elif args.command == "stations":
        resolutions = [int(x) for x in args.resolutions.split(",")]
        build_stations(args.data_dir, args.api_url, resolutions)

    elif args.command == "top-stations":
        fact_path = args.fact
        if not fact_path:
            candidates = sorted(
                f for f in os.listdir(args.data_dir) if f.endswith("_baseline.parquet")
            )
            if not candidates:
                sys.exit(f"no *_baseline.parquet found in {args.data_dir}")
            fact_path = os.path.join(args.data_dir, candidates[-1])
        top_stations(args.data_dir, fact_path, args.n)


if __name__ == "__main__":
    main()
