"""
从 Jolpica API 下载 lap_times 和 qualifying 数据，展平后写入本地 NDJSON 文件，
再通过 ZettaPark session.file.put() 上传到 Volume。

两步走：
  步骤 1 — 纯 HTTP 下载，不需要 Lakehouse 连接，结果写到本地 /tmp/
  步骤 2 — 连接 Lakehouse，把本地文件 PUT 到 Volume

用法：
  cd 03_lakehouse
  python -u 01_ingestion/download_lap_times_qualifying.py
"""

import sys
import os
import json
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.stdout.reconfigure(line_buffering=True)   # 强制行缓冲，后台运行时也能看到进度

from dotenv import load_dotenv
load_dotenv()

import urllib.request

BASE_URL      = "https://api.jolpi.ca/ergast/f1"
DELAY         = 0.25
LOCAL_Q_FILE  = "/tmp/f1_qualifying.json"
LOCAL_LT_FILE = "/tmp/f1_lap_times.json"

# races 列表硬编码，避免在下载阶段就连 Lakehouse
# 格式：(race_id, year, round)  — 与 f1_processed.races 一致
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


# ── 步骤 1a：下载 qualifying ────────────────────────────────────────────────────
print(f"=== 步骤 1a：下载 qualifying ({len(RACES)} 场) ===", flush=True)
qualify_id = 1
with open(LOCAL_Q_FILE, "w", encoding="utf-8") as out:
    for race_id, year, round_num in RACES:
        url = f"{BASE_URL}/{year}/{round_num}/qualifying.json"
        try:
            data = fetch_json(url)
            race_list = data["MRData"]["RaceTable"]["Races"]
            if not race_list:
                print(f"  [SKIP] {year} R{round_num}", flush=True)
                time.sleep(DELAY)
                continue
            results = race_list[0].get("QualifyingResults", [])
            for r in results:
                rec = {
                    "qualifyId":     qualify_id,
                    "raceId":        race_id,
                    "driverId":      r["Driver"]["driverId"],
                    "constructorId": r["Constructor"]["constructorId"],
                    "number":        int(r.get("number", 0) or 0),
                    "position":      int(r.get("position", 0) or 0),
                    "q1":            r.get("Q1", ""),
                    "q2":            r.get("Q2", ""),
                    "q3":            r.get("Q3", ""),
                }
                out.write(json.dumps(rec) + "\n")
                qualify_id += 1
            print(f"  {year} R{round_num}: {len(results)} 条", flush=True)
        except Exception as e:
            print(f"  [ERROR] {year} R{round_num}: {e}", flush=True)
        time.sleep(DELAY)

total_q = qualify_id - 1
print(f"\nqualifying 合计 {total_q} 条 → {LOCAL_Q_FILE}\n", flush=True)


# ── 步骤 1b：下载 lap_times ─────────────────────────────────────────────────────
print(f"=== 步骤 1b：下载 lap_times ({len(RACES)} 场) ===", flush=True)
total_lt = 0
with open(LOCAL_LT_FILE, "w", encoding="utf-8") as out:
    for race_id, year, round_num in RACES:
        offset, limit, lap_count = 0, 100, 0
        while True:
            url = f"{BASE_URL}/{year}/{round_num}/laps.json?limit={limit}&offset={offset}"
            try:
                data = fetch_json(url)
                api_total = int(data["MRData"]["total"])
                race_list = data["MRData"]["RaceTable"]["Races"]
                if not race_list:
                    break
                for lap in race_list[0].get("Laps", []):
                    lap_num = int(lap["number"])
                    for timing in lap.get("Timings", []):
                        rec = {
                            "raceId":       race_id,
                            "driverId":     timing["driverId"],
                            "lap":          lap_num,
                            "position":     int(timing.get("position", 0) or 0),
                            "time":         timing.get("time", ""),
                            "milliseconds": 0,
                        }
                        out.write(json.dumps(rec) + "\n")
                        lap_count += 1
                offset += limit
                if offset >= api_total:
                    break
            except Exception as e:
                print(f"  [ERROR] {year} R{round_num} offset={offset}: {e}", flush=True)
                break
            time.sleep(DELAY)
        total_lt += lap_count
        print(f"  {year} R{round_num}: {lap_count} 条", flush=True)

print(f"\nlap_times 合计 {total_lt} 条 → {LOCAL_LT_FILE}\n", flush=True)


# ── 步骤 2：上传到 Volume ────────────────────────────────────────────────────────
print("=== 步骤 2：上传到 Volume ===", flush=True)

from clickzetta.zettapark.session import Session
from includes.configuration import SCHEMA_NAME, raw_folder_path

session = Session.builder.configs({
    "username":  os.environ["CLICKZETTA_USERNAME"],
    "password":  os.environ["CLICKZETTA_PASSWORD"],
    "service":   os.environ["CLICKZETTA_SERVICE"],
    "instance":  os.environ["CLICKZETTA_INSTANCE"],
    "workspace": os.environ["CLICKZETTA_WORKSPACE"],
    "schema":    SCHEMA_NAME,
    "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default_ap"),
}).create()

for local, remote_name in [(LOCAL_Q_FILE, "qualifying.json"), (LOCAL_LT_FILE, "lap_times.json")]:
    dest = f"{raw_folder_path}/{remote_name}"
    session.file.put(local, dest, auto_compress=False, overwrite=True)
    lines = sum(1 for _ in open(local, encoding="utf-8"))
    print(f"  {remote_name}: {lines} 条 → {dest}", flush=True)

session.close()
print("\n完成。", flush=True)
