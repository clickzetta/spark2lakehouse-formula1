#!/usr/bin/env python3
"""
spark2lakehouse-formula1 一键初始化脚本

执行顺序：
  1. 从 Jolpica API 下载 F1 原始数据到 ../datasets/raw/
  2. 连接 ClickZetta Lakehouse
  3. 创建 Volume（<CLICKZETTA_SCHEMA>.formula1_vol）
  4. 上传 ../datasets/raw/ 到 Volume
  5. 执行 04_create_raw_tables/*.sql（建 schema + COPY INTO）
  6. 执行 02_transformation/*.sql
  7. 执行 03_analysis/*.sql

用法：
  pip install clickzetta-connector-python python-dotenv requests
  cp ../.env.sample ../.env  # 填写连接信息
  python setup.py
"""

import os
import sys
import json
import csv
import time
import glob
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

try:
    import requests
except ImportError:
    print("请先安装依赖: pip install clickzetta-connector-python python-dotenv requests")
    sys.exit(1)

try:
    import clickzetta
except ImportError:
    print("请先安装依赖: pip install clickzetta-connector-python")
    sys.exit(1)

# ── 配置 ────────────────────────────────────────────────────────────────────

SCHEMA_NAME   = os.environ.get("CLICKZETTA_SCHEMA", "mcp_demo")
VOLUME_NAME   = os.environ.get("CLICKZETTA_VOLUME", "formula1_vol")
VOLUME_ID     = f"{SCHEMA_NAME}.{VOLUME_NAME}"
VOLUME_PATH   = f"/Volumes/quick_start/{SCHEMA_NAME}/{VOLUME_NAME}"

DATASETS_DIR  = Path(__file__).parent.parent / "datasets" / "raw"
LAKEHOUSE_DIR = Path(__file__).parent

SQL_LAYERS = ["04_create_raw_tables", "02_transformation", "03_analysis"]

ERGAST_BASE = "https://api.jolpi.ca/ergast/f1"

# ── Ergast 数据下载 ──────────────────────────────────────────────────────────

def fetch_json(url: str, retries: int = 3) -> dict:
    for i in range(retries):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if i == retries - 1:
                raise
            print(f" (重试 {i+1}/{retries})", end="", flush=True)
            time.sleep(2)


def download_circuits(out_dir: Path):
    print("  circuits.csv ...", end=" ", flush=True)
    data = fetch_json(f"{ERGAST_BASE}/circuits.json?limit=100")
    rows = data["MRData"]["CircuitTable"]["Circuits"]
    out = out_dir / "circuits.csv"
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt", "url"])
        for i, c in enumerate(rows, 1):
            w.writerow([i, c["circuitId"], c["circuitName"],
                        c["Location"]["locality"], c["Location"]["country"],
                        c["Location"]["lat"], c["Location"]["long"], 0, c["url"]])
    print(f"OK ({len(rows)} rows)")


def download_races(out_dir: Path):
    print("  races.csv ...", end=" ", flush=True)
    all_rows = []
    for season in range(2018, 2024):
        data = fetch_json(f"{ERGAST_BASE}/{season}/races.json?limit=30")
        races = data["MRData"]["RaceTable"]["Races"]
        for r in races:
            all_rows.append([r["round"], season, r["round"], r["Circuit"]["circuitId"],
                             r["raceName"], r["date"], r.get("time", ""), r["url"]])
    out = out_dir / "races.csv"
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["raceId", "year", "round", "circuitId", "name", "date", "time", "url"])
        for i, row in enumerate(all_rows, 1):
            w.writerow([i] + row[1:])
    print(f"OK ({len(all_rows)} rows)")


def download_constructors(out_dir: Path):
    print("  constructors.json ...", end=" ", flush=True)
    data = fetch_json(f"{ERGAST_BASE}/constructors.json?limit=200")
    rows = data["MRData"]["ConstructorTable"]["Constructors"]
    out = out_dir / "constructors.json"
    result = []
    for i, c in enumerate(rows, 1):
        result.append({"constructorId": i, "constructorRef": c["constructorId"],
                       "name": c["name"], "nationality": c["nationality"], "url": c["url"]})
    out.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in result))
    print(f"OK ({len(result)} rows)")


def download_drivers(out_dir: Path):
    print("  drivers.json ...", end=" ", flush=True)
    data = fetch_json(f"{ERGAST_BASE}/drivers.json?limit=1000")
    rows = data["MRData"]["DriverTable"]["Drivers"]
    out = out_dir / "drivers.json"
    result = []
    for i, d in enumerate(rows, 1):
        result.append({"driverId": i, "driverRef": d["driverId"],
                       "number": int(d.get("permanentNumber") or 0),
                       "code": d.get("code", ""),
                       "forename": d["givenName"], "surname": d["familyName"],
                       "dob": d.get("dateOfBirth", ""), "nationality": d.get("nationality", ""),
                       "url": d.get("url", "")})
    out.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in result))
    print(f"OK ({len(result)} rows)")


def download_results(out_dir: Path):
    print("  results.json ...", end=" ", flush=True)
    all_results = []
    rid = 1
    for season in range(2022, 2024):
        data = fetch_json(f"{ERGAST_BASE}/{season}/results.json?limit=500")
        races = data["MRData"]["RaceTable"]["Races"]
        for race in races:
            for r in race.get("Results", []):
                all_results.append({
                    "resultId": rid, "raceId": int(race["round"]),
                    "driverId": r["Driver"]["driverId"],
                    "constructorId": r["Constructor"]["constructorId"],
                    "number": int(r.get("number") or 0), "grid": int(r.get("grid") or 0),
                    "position": int(r.get("position") or 0),
                    "positionText": r.get("positionText", ""),
                    "positionOrder": int(r.get("positionOrder") or 0),
                    "points": float(r.get("points") or 0),
                    "laps": int(r.get("laps") or 0), "time": r.get("Time", {}).get("time", ""),
                    "milliseconds": int(r.get("Time", {}).get("millis") or 0),
                    "fastestLap": int(r.get("FastestLap", {}).get("lap") or 0),
                    "rank": int(r.get("FastestLap", {}).get("rank") or 0),
                    "fastestLapTime": r.get("FastestLap", {}).get("Time", {}).get("time", ""),
                    "fastestLapSpeed": float(r.get("FastestLap", {}).get("AverageSpeed", {}).get("speed") or 0),
                    "statusId": r.get("status", "")
                })
                rid += 1
    out = out_dir / "results.json"
    out.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in all_results))
    print(f"OK ({len(all_results)} rows)")


def download_pit_stops(out_dir: Path):
    print("  pit_stops.json ...", end=" ", flush=True)
    all_stops = []
    for season in range(2022, 2024):
        # 先获取该赛季的 rounds 数
        season_data = fetch_json(f"{ERGAST_BASE}/{season}/races.json?limit=30")
        races = season_data["MRData"]["RaceTable"]["Races"]
        for race in races:
            rnd = race["round"]
            try:
                data = fetch_json(f"{ERGAST_BASE}/{season}/{rnd}/pitstops.json?limit=100")
                pit_races = data["MRData"]["RaceTable"]["Races"]
                for pr in pit_races:
                    for s in pr.get("PitStops", []):
                        all_stops.append({
                            "raceId": int(rnd), "driverId": s["driverId"],
                            "stop": int(s["stop"]), "lap": int(s["lap"]),
                            "time": s["time"], "duration": s["duration"],
                            "milliseconds": int(s.get("milliseconds") or 0)
                        })
            except Exception:
                pass  # 某些 round 无 pit stop 数据
    out = out_dir / "pit_stops.json"
    out.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in all_stops))
    print(f"OK ({len(all_stops)} rows)")


def download_datasets():
    print("\n[1/4] 下载 F1 原始数据（Jolpica API）")
    DATASETS_DIR.mkdir(parents=True, exist_ok=True)
    download_circuits(DATASETS_DIR)
    download_races(DATASETS_DIR)
    download_constructors(DATASETS_DIR)
    download_drivers(DATASETS_DIR)
    download_results(DATASETS_DIR)
    download_pit_stops(DATASETS_DIR)

# ── 连接 ────────────────────────────────────────────────────────────────────

def get_conn():
    required = ["CLICKZETTA_SERVICE", "CLICKZETTA_INSTANCE", "CLICKZETTA_WORKSPACE",
                "CLICKZETTA_USERNAME", "CLICKZETTA_PASSWORD"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        print(f"[ERROR] .env 缺少必填项: {', '.join(missing)}")
        sys.exit(1)

    return clickzetta.connect(
        service=os.environ["CLICKZETTA_SERVICE"],
        instance=os.environ["CLICKZETTA_INSTANCE"],
        workspace=os.environ["CLICKZETTA_WORKSPACE"],
        username=os.environ["CLICKZETTA_USERNAME"],
        password=os.environ["CLICKZETTA_PASSWORD"],
        schema=os.environ.get("CLICKZETTA_SCHEMA", SCHEMA_NAME),
        vcluster=os.environ.get("CLICKZETTA_VCLUSTER", "default_ap"),
    )

# ── Volume 操作 ──────────────────────────────────────────────────────────────

def create_volume(cur):
    print(f"\n[2/4] 创建 Volume: {VOLUME_ID}")
    cur.execute(f"CREATE VOLUME IF NOT EXISTS {VOLUME_ID}")
    print(f"      OK → {VOLUME_PATH}")


def upload_datasets(cur):
    print(f"\n[3/4] 上传数据集到 Volume")
    cur.execute(f"USE SCHEMA {SCHEMA_NAME}")
    files = sorted(DATASETS_DIR.glob("*"))
    if not files:
        print(f"      [WARN] {DATASETS_DIR} 下没有文件，请先运行下载步骤")
        return
    for f in files:
        if not f.is_file():
            continue
        print(f"      PUT {f.name} ...", end=" ", flush=True)
        cur.execute(f"PUT '{f.resolve()}' TO VOLUME {VOLUME_NAME} FILE '{f.name}'")
        print("OK")

# ── SQL 执行 ─────────────────────────────────────────────────────────────────

def run_sql_file(cur, path: Path, warn_on_error: bool = False):
    sql = path.read_text()
    sql = sql.replace("<your_volume_path>", VOLUME_PATH)
    sql = sql.replace("<volume_path>", VOLUME_PATH)

    statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
    for stmt in statements:
        preview = stmt.replace("\n", " ")[:72]
        print(f"      SQL: {preview}...", end=" ", flush=True)
        try:
            cur.execute(stmt)
            print("OK")
        except Exception as e:
            if warn_on_error:
                print(f"WARN ({e})")
            else:
                raise


def run_layers(cur):
    print(f"\n[4/4] 执行 Lakehouse SQL")
    for layer in SQL_LAYERS:
        sql_files = sorted((LAKEHOUSE_DIR / layer).glob("*.sql"))
        if not sql_files:
            print(f"      [SKIP] {layer}/ 下没有 SQL 文件")
            continue
        # analysis 层依赖 transformation 数据，允许报错继续
        warn_only = layer == "03_analysis"
        print(f"\n  ── {layer.upper()} ──")
        for f in sql_files:
            print(f"  {f.name}")
            run_sql_file(cur, f, warn_on_error=warn_only)

# ── 主流程 ───────────────────────────────────────────────────────────────────

def main():
    skip_download = "--skip-download" in sys.argv

    print("=" * 60)
    print("spark2lakehouse-formula1 初始化")
    print(f"  Service  : {os.environ.get('CLICKZETTA_SERVICE', '(未设置)')}")
    print(f"  Instance : {os.environ.get('CLICKZETTA_INSTANCE', '(未设置)')}")
    print(f"  Workspace: {os.environ.get('CLICKZETTA_WORKSPACE', '(未设置)')}")
    print(f"  Volume   : {VOLUME_ID}  →  {VOLUME_PATH}")
    print("=" * 60)

    if not skip_download:
        download_datasets()
    else:
        print("\n[1/4] 跳过数据下载（--skip-download）")

    print("\n连接 Lakehouse ...", end=" ", flush=True)
    conn = get_conn()
    cur = conn.cursor()
    print("OK")

    try:
        create_volume(cur)
        upload_datasets(cur)
        # 建三个 schema（IF NOT EXISTS，幂等）
        for schema in ["f1_raw", "f1_processed", "f1_presentation"]:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        run_layers(cur)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        cur.close()
        conn.close()
        sys.exit(1)

    cur.close()
    conn.close()
    print("\n" + "=" * 60)
    print("初始化完成！")
    print(f"  Raw    表: f1_raw.circuits / races / constructors / drivers / results / pit_stops")
    print(f"  分析结果: f1_presentation.race_results / driver_standings / constructor_standings")
    print("=" * 60)


if __name__ == "__main__":
    main()
