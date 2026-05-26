"""
修复 drivers.json 和 constructors.json：
从 Jolpica 按赛季下载 2018-2023 参赛车手和车队，去重后写入本地文件并上传 Volume。

问题根因：Jolpica /drivers.json?limit=1000 实际只返回 100 条（按字母序），
导致 2018-2023 的车手（hamilton, vettel 等）全部缺失。
修复方案：改用 /f1/{season}/drivers.json 按赛季下载，再合并去重。

用法：
  cd 03_lakehouse
  python -u 01_ingestion/fix_drivers_constructors.py
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

BASE_URL  = "https://api.jolpi.ca/ergast/f1"
DELAY     = 0.25
SEASONS   = list(range(2018, 2024))

DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets" / "raw"
LOCAL_DRV_FILE = "/tmp/f1_drivers.json"
LOCAL_CON_FILE = "/tmp/f1_constructors.json"


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


# ── 下载 drivers ───────────────────────────────────────────────────────────────
print("=== 下载 drivers (2018-2023) ===", flush=True)
seen_drivers = {}  # driverRef → record (dedup)
driver_id = 1

for season in SEASONS:
    url = f"{BASE_URL}/{season}/drivers.json?limit=100"
    data = fetch_json(url)
    drivers = data["MRData"]["DriverTable"]["Drivers"]
    new_count = 0
    for d in drivers:
        ref = d["driverId"]
        if ref not in seen_drivers:
            seen_drivers[ref] = {
                "driverId":    driver_id,
                "driverRef":   ref,
                "number":      int(d.get("permanentNumber") or 0),
                "code":        d.get("code", ""),
                "forename":    d["givenName"],
                "surname":     d["familyName"],
                "dob":         d.get("dateOfBirth", ""),
                "nationality": d.get("nationality", ""),
                "url":         d.get("url", ""),
            }
            driver_id += 1
            new_count += 1
    print(f"  {season}: {len(drivers)} 条，新增 {new_count}", flush=True)
    time.sleep(DELAY)

with open(LOCAL_DRV_FILE, "w", encoding="utf-8") as f:
    for rec in seen_drivers.values():
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
print(f"\ndrivers 合计 {len(seen_drivers)} 条 → {LOCAL_DRV_FILE}\n", flush=True)


# ── 下载 constructors ──────────────────────────────────────────────────────────
print("=== 下载 constructors (2018-2023) ===", flush=True)
seen_constructors = {}  # constructorId → record
constructor_id = 1

for season in SEASONS:
    url = f"{BASE_URL}/{season}/constructors.json?limit=100"
    data = fetch_json(url)
    constructors = data["MRData"]["ConstructorTable"]["Constructors"]
    new_count = 0
    for c in constructors:
        ref = c["constructorId"]
        if ref not in seen_constructors:
            seen_constructors[ref] = {
                "constructorId":  constructor_id,
                "constructorRef": ref,
                "name":           c["name"],
                "nationality":    c.get("nationality", ""),
                "url":            c.get("url", ""),
            }
            constructor_id += 1
            new_count += 1
    print(f"  {season}: {len(constructors)} 条，新增 {new_count}", flush=True)
    time.sleep(DELAY)

with open(LOCAL_CON_FILE, "w", encoding="utf-8") as f:
    for rec in seen_constructors.values():
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
print(f"\nconstructors 合计 {len(seen_constructors)} 条 → {LOCAL_CON_FILE}\n", flush=True)


# ── 上传到 Volume + 同步本地 ───────────────────────────────────────────────────
print("=== 上传到 Volume ===", flush=True)

from clickzetta.zettapark.session import Session
from includes.configuration import SCHEMA_NAME, raw_folder_path
import shutil

session = Session.builder.configs({
    "username":  os.environ["CLICKZETTA_USERNAME"],
    "password":  os.environ["CLICKZETTA_PASSWORD"],
    "service":   os.environ["CLICKZETTA_SERVICE"],
    "instance":  os.environ["CLICKZETTA_INSTANCE"],
    "workspace": os.environ["CLICKZETTA_WORKSPACE"],
    "schema":    SCHEMA_NAME,
    "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default_ap"),
}).create()

for local, remote_name in [(LOCAL_DRV_FILE, "drivers.json"), (LOCAL_CON_FILE, "constructors.json")]:
    dest = f"{raw_folder_path}/{remote_name}"
    session.file.put(local, dest, auto_compress=False, overwrite=True)
    lines = sum(1 for _ in open(local, encoding="utf-8"))
    print(f"  {remote_name}: {lines} 条 → {dest}", flush=True)
    shutil.copy(local, DATASETS_DIR / remote_name)
    print(f"  {remote_name}: 已同步到 {DATASETS_DIR / remote_name}", flush=True)

session.close()
print("\n完成。", flush=True)
