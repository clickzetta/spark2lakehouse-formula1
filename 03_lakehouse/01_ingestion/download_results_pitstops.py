"""
从 Jolpica API 下载 results 和 pit_stops 数据，写入本地 NDJSON 文件，
再上传到 Volume。

使用与 download_lap_times_qualifying.py 相同的 RACES 列表，
确保 raceId 与 f1_processed.races 一致（顺序编号 1-125）。

用法：
  cd 03_lakehouse
  python -u 01_ingestion/download_results_pitstops.py
"""

import sys
import os
import json
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.stdout.reconfigure(line_buffering=True)

from dotenv import load_dotenv
load_dotenv()

import urllib.request

BASE_URL         = "https://api.jolpi.ca/ergast/f1"
DELAY            = 0.25
LOCAL_RES_FILE   = "/tmp/f1_results.json"
LOCAL_PIT_FILE   = "/tmp/f1_pit_stops.json"

RACES = [
    (1,2018,1),(2,2018,2),(3,2018,3),(4,2018,4),(5,2018,5),(6,2018,6),(7,2018,7),
    (8,2018,8),(9,2018,9),(10,2018,10),(11,2018,11),(12,2018,12),(13,2018,13),
    (14,2018,14),(15,2018,15),(16,2018,16),(17,2018,17),(18,2018,18),(19,2018,19),
    (20,2018,20),(21,2018,21),
    (22,2019,1),(23,2019,2),(24,2019,3),(25,2019,4),(26,2019,5),(27,2019,6),(28,2019,7),
    (29,2019,8),(30,2019,9),(31,2019,10),(32,2019,11),(33,2019,12),(34,2019,13),
    (35,2019,14),(36,2019,15),(37,2019,16),(38,2019,17),(39,2019,18),(40,2019,19),
    (41,2019,20),(42,2019,21),
    (43,2020,1),(44,2020,2),(45,2020,3),(46,2020,4),(47,2020,5),(48,2020,6),(49,2020,7),
    (50,2020,8),(51,2020,9),(52,2020,10),(53,2020,11),(54,2020,12),(55,2020,13),
    (56,2020,14),(57,2020,15),(58,2020,16),(59,2020,17),
    (60,2021,1),(61,2021,2),(62,2021,3),(63,2021,4),(64,2021,5),(65,2021,6),(66,2021,7),
    (67,2021,8),(68,2021,9),(69,2021,10),(70,2021,11),(71,2021,12),(72,2021,13),
    (73,2021,14),(74,2021,15),(75,2021,16),(76,2021,17),(77,2021,18),(78,2021,19),
    (79,2021,20),(80,2021,21),(81,2021,22),
    (82,2022,1),(83,2022,2),(84,2022,3),(85,2022,4),(86,2022,5),(87,2022,6),(88,2022,7),
    (89,2022,8),(90,2022,9),(91,2022,10),(92,2022,11),(93,2022,12),(94,2022,13),
    (95,2022,14),(96,2022,15),(97,2022,16),(98,2022,17),(99,2022,18),(100,2022,19),
    (101,2022,20),(102,2022,21),(103,2022,22),
    (104,2023,1),(105,2023,2),(106,2023,3),(107,2023,4),(108,2023,5),(109,2023,6),
    (110,2023,7),(111,2023,8),(112,2023,9),(113,2023,10),(114,2023,11),(115,2023,12),
    (116,2023,13),(117,2023,14),(118,2023,15),(119,2023,16),(120,2023,17),(121,2023,18),
    (122,2023,19),(123,2023,20),(124,2023,21),(125,2023,22),
]


def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise RuntimeError(f"请求失败 {url}: {e}")


# ── 步骤 1a：下载 results ──────────────────────────────────────────────────────
print(f"=== 步骤 1a：下载 results ({len(RACES)} 场) ===", flush=True)
result_id = 1
total_res = 0
with open(LOCAL_RES_FILE, "w", encoding="utf-8") as out:
    for race_id, year, round_num in RACES:
        url = f"{BASE_URL}/{year}/{round_num}/results.json"
        try:
            data = fetch_json(url)
            race_list = data["MRData"]["RaceTable"]["Races"]
            if not race_list:
                print(f"  [SKIP] {year} R{round_num}", flush=True)
                time.sleep(DELAY)
                continue
            results = race_list[0].get("Results", [])
            for r in results:
                rec = {
                    "resultId":        result_id,
                    "raceId":          race_id,
                    "driverId":        r["Driver"]["driverId"],
                    "constructorId":   r["Constructor"]["constructorId"],
                    "number":          int(r.get("number") or 0),
                    "grid":            int(r.get("grid") or 0),
                    "position":        int(r.get("position") or 0),
                    "positionText":    r.get("positionText", ""),
                    "positionOrder":   int(r.get("positionOrder") or 0),
                    "points":          float(r.get("points") or 0),
                    "laps":            int(r.get("laps") or 0),
                    "time":            r.get("Time", {}).get("time", "") if r.get("Time") else "",
                    "milliseconds":    int(r.get("Time", {}).get("millis") or 0) if r.get("Time") else 0,
                    "fastestLap":      int(r.get("FastestLap", {}).get("lap") or 0) if r.get("FastestLap") else 0,
                    "rank":            int(r.get("FastestLap", {}).get("rank") or 0) if r.get("FastestLap") else 0,
                    "fastestLapTime":  r.get("FastestLap", {}).get("Time", {}).get("time", "") if r.get("FastestLap") else "",
                    "fastestLapSpeed": float(r.get("FastestLap", {}).get("AverageSpeed", {}).get("speed") or 0) if r.get("FastestLap") else 0.0,
                    "statusId":        r.get("status", ""),
                }
                out.write(json.dumps(rec) + "\n")
                result_id += 1
            total_res += len(results)
            print(f"  {year} R{round_num}: {len(results)} 条", flush=True)
        except Exception as e:
            print(f"  [ERROR] {year} R{round_num}: {e}", flush=True)
        time.sleep(DELAY)

print(f"\nresults 合计 {total_res} 条 → {LOCAL_RES_FILE}\n", flush=True)


# ── 步骤 1b：下载 pit_stops ────────────────────────────────────────────────────
print(f"=== 步骤 1b：下载 pit_stops ({len(RACES)} 场) ===", flush=True)
total_pit = 0
with open(LOCAL_PIT_FILE, "w", encoding="utf-8") as out:
    for race_id, year, round_num in RACES:
        url = f"{BASE_URL}/{year}/{round_num}/pitstops.json?limit=100"
        try:
            data = fetch_json(url)
            race_list = data["MRData"]["RaceTable"]["Races"]
            if not race_list:
                print(f"  [SKIP] {year} R{round_num}", flush=True)
                time.sleep(DELAY)
                continue
            stops = race_list[0].get("PitStops", [])
            for s in stops:
                rec = {
                    "raceId":       race_id,
                    "driverId":     s["driverId"],
                    "stop":         int(s["stop"]),
                    "lap":          int(s["lap"]),
                    "time":         s["time"],
                    "duration":     s["duration"],
                    "milliseconds": int(s.get("milliseconds") or 0),
                }
                out.write(json.dumps(rec) + "\n")
            total_pit += len(stops)
            if stops:
                print(f"  {year} R{round_num}: {len(stops)} 条", flush=True)
        except Exception as e:
            print(f"  [ERROR] {year} R{round_num}: {e}", flush=True)
        time.sleep(DELAY)

print(f"\npit_stops 合计 {total_pit} 条 → {LOCAL_PIT_FILE}\n", flush=True)


# ── 步骤 2：上传到 Volume + 本地 datasets/raw/ ─────────────────────────────────
print("=== 步骤 2：上传到 Volume ===", flush=True)

from clickzetta.zettapark.session import Session
from includes.configuration import SCHEMA_NAME, raw_folder_path

DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets" / "raw"

session = Session.builder.configs({
    "username":  os.environ["CLICKZETTA_USERNAME"],
    "password":  os.environ["CLICKZETTA_PASSWORD"],
    "service":   os.environ["CLICKZETTA_SERVICE"],
    "instance":  os.environ["CLICKZETTA_INSTANCE"],
    "workspace": os.environ["CLICKZETTA_WORKSPACE"],
    "schema":    SCHEMA_NAME,
    "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default_ap"),
}).create()

for local, remote_name in [(LOCAL_RES_FILE, "results.json"), (LOCAL_PIT_FILE, "pit_stops.json")]:
    dest = f"{raw_folder_path}/{remote_name}"
    session.file.put(local, dest, auto_compress=False, overwrite=True)
    lines = sum(1 for _ in open(local, encoding="utf-8"))
    print(f"  {remote_name}: {lines} 条 → {dest}", flush=True)
    # 同步到本地 datasets/raw/
    import shutil
    shutil.copy(local, DATASETS_DIR / remote_name)
    print(f"  {remote_name}: 已同步到 {DATASETS_DIR / remote_name}", flush=True)

session.close()
print("\n完成。", flush=True)
