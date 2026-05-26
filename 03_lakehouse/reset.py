#!/usr/bin/env python3
"""
reset.py — 清空所有 f1_processed 和 f1_presentation 表及视图

用途：
  - E2E 测试前从零开始
  - 重新摄取全量数据前清空旧数据
  - 排查问题时复现干净状态

用法：
  python 03_lakehouse/reset.py              # 直接执行
  python 03_lakehouse/reset.py --dry-run    # 只打印将要执行的操作，不实际删除
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(line_buffering=True)

from dotenv import load_dotenv
load_dotenv()

from clickzetta.zettapark.session import Session
from includes.configuration import SCHEMA_NAME, processed_schema, presentation_schema

DRY_RUN = "--dry-run" in sys.argv

PROCESSED_TABLES = [
    "circuits", "constructors", "drivers", "races",
    "results", "pit_stops", "lap_times", "qualifying",
]
PRESENTATION_TABLES = [
    "race_results", "driver_standings",
    "constructor_standings", "calculated_race_results",
]
PRESENTATION_VIEWS = [
    "race_result_updated",
]


def drop(session, kind, schema, name):
    full = f"{schema}.{name}"
    stmt = f"DROP {kind} IF EXISTS {full}"
    if DRY_RUN:
        print(f"  [DRY] {stmt}")
        return
    try:
        session.sql(stmt).collect()
        print(f"  dropped {full}")
    except Exception as e:
        print(f"  WARN {full}: {e}")


def main():
    if DRY_RUN:
        print("=== DRY RUN — 不会实际删除任何表 ===\n")

    session = Session.builder.configs({
        "username":  os.environ["CLICKZETTA_USERNAME"],
        "password":  os.environ["CLICKZETTA_PASSWORD"],
        "service":   os.environ["CLICKZETTA_SERVICE"],
        "instance":  os.environ["CLICKZETTA_INSTANCE"],
        "workspace": os.environ["CLICKZETTA_WORKSPACE"],
        "schema":    SCHEMA_NAME,
        "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default_ap"),
    }).create()

    print(f"清空 {processed_schema} ...")
    for t in PROCESSED_TABLES:
        drop(session, "TABLE", processed_schema, t)

    print(f"\n清空 {presentation_schema} ...")
    for v in PRESENTATION_VIEWS:
        drop(session, "VIEW", presentation_schema, v)
    for t in PRESENTATION_TABLES:
        drop(session, "TABLE", presentation_schema, t)

    session.close()

    if DRY_RUN:
        print("\n=== DRY RUN 完成，未执行任何删除 ===")
    else:
        print("\n全部清空完成。接下来运行：")
        print("  python 03_lakehouse/01_ingestion/0.ingest_all_files.py")
        print("  python 03_lakehouse/02_transformation/0.transform_all.py")


if __name__ == "__main__":
    main()
