"""
upload_to_volume.py — 把 datasets/raw/ 里的本地文件上传到 Lakehouse Volume

这是 API 下载的替代方案。datasets/raw/ 已包含所有原始数据文件，
直接上传到 Volume 后即可运行 01_ingestion/0.ingest_all_files.py。

用法：
  cd spark2lakehouse-formula1
  python 03_lakehouse/upload_to_volume.py
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(line_buffering=True)

from dotenv import load_dotenv
load_dotenv()

from clickzetta.zettapark.session import Session
from includes.configuration import SCHEMA_NAME, raw_folder_path

DATASETS_DIR = Path(__file__).parent.parent / "datasets" / "raw"

FILES = [
    "circuits.csv",
    "races.csv",
    "constructors.json",
    "drivers.json",
    "results.json",
    "pit_stops.json",
    "lap_times.json",
    "qualifying.json",
]

session = Session.builder.configs({
    "username":  os.environ["CLICKZETTA_USERNAME"],
    "password":  os.environ["CLICKZETTA_PASSWORD"],
    "service":   os.environ["CLICKZETTA_SERVICE"],
    "instance":  os.environ["CLICKZETTA_INSTANCE"],
    "workspace": os.environ["CLICKZETTA_WORKSPACE"],
    "schema":    SCHEMA_NAME,
    "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default_ap"),
}).create()

print(f"上传目标 Volume: {raw_folder_path}\n")

uploaded, skipped = 0, 0
for fname in FILES:
    local = DATASETS_DIR / fname
    if not local.exists():
        print(f"  [SKIP] {fname} — 本地文件不存在")
        skipped += 1
        continue
    dest = f"{raw_folder_path}/{fname}"
    size_kb = local.stat().st_size // 1024
    print(f"  上传 {fname} ({size_kb} KB)...", end=" ", flush=True)
    session.file.put(str(local), dest, auto_compress=False, overwrite=True)
    print("完成")
    uploaded += 1

session.close()
print(f"\n上传完成：{uploaded} 个文件，跳过 {skipped} 个。")
print("接下来运行：python 03_lakehouse/01_ingestion/0.ingest_all_files.py")
