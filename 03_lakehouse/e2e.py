#!/usr/bin/env python3
"""
e2e.py — 端到端全流程运行脚本

执行顺序：
  1. （可选）reset：清空所有 processed / presentation 表
  2. upload：上传 datasets/raw/ 到 Volume
  3. ingest：运行 01_ingestion/0.ingest_all_files.py
  4. transform：运行 02_transformation/0.transform_all.py
  5. 打印各层行数汇总

用法：
  python 03_lakehouse/e2e.py               # 增量运行（不清空已有表）
  python 03_lakehouse/e2e.py --reset       # 先清空所有表，再全量跑
  python 03_lakehouse/e2e.py --skip-upload # 跳过上传（Volume 已有文件时）
"""

import os
import sys
import importlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(line_buffering=True)

from dotenv import load_dotenv
load_dotenv()

from clickzetta.zettapark.session import Session
from includes.configuration import SCHEMA_NAME, processed_schema, presentation_schema

DO_RESET    = "--reset"       in sys.argv
SKIP_UPLOAD = "--skip-upload" in sys.argv

LAKEHOUSE_DIR = Path(__file__).parent
DATASETS_DIR  = LAKEHOUSE_DIR.parent / "datasets" / "raw"


def make_session():
    return Session.builder.configs({
        "username":  os.environ["CLICKZETTA_USERNAME"],
        "password":  os.environ["CLICKZETTA_PASSWORD"],
        "service":   os.environ["CLICKZETTA_SERVICE"],
        "instance":  os.environ["CLICKZETTA_INSTANCE"],
        "workspace": os.environ["CLICKZETTA_WORKSPACE"],
        "schema":    SCHEMA_NAME,
        "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default_ap"),
    }).create()


def step_reset():
    print("\n[1/4] RESET — 清空所有表")
    import reset as r
    r.DRY_RUN = False
    session = make_session()
    for t in r.PROCESSED_TABLES:
        r.drop(session, "TABLE", processed_schema, t)
    for v in r.PRESENTATION_VIEWS:
        r.drop(session, "VIEW", presentation_schema, v)
    for t in r.PRESENTATION_TABLES:
        r.drop(session, "TABLE", presentation_schema, t)
    session.close()
    print("  完成")


def step_upload():
    print("\n[2/4] UPLOAD — 上传数据文件到 Volume")
    files = list(DATASETS_DIR.glob("*"))
    if not files:
        print(f"  [ERROR] {DATASETS_DIR} 下没有文件，请先下载数据集")
        sys.exit(1)
    import upload_to_volume  # 直接 import 执行


def step_ingest():
    print("\n[3/4] INGEST — 数据摄取")
    sys.path.insert(0, str(LAKEHOUSE_DIR / "01_ingestion"))
    import importlib, ingest_all_files_mod
    # 用 exec 方式运行，避免模块缓存问题
    exec(open(LAKEHOUSE_DIR / "01_ingestion" / "0.ingest_all_files.py").read())


def step_transform():
    print("\n[4/4] TRANSFORM — 数据转换")
    exec(open(LAKEHOUSE_DIR / "02_transformation" / "0.transform_all.py").read())


def step_summary():
    print("\n=== 数据汇总 ===")
    session = make_session()

    processed = ["circuits", "constructors", "drivers", "races",
                 "results", "pit_stops", "lap_times", "qualifying"]
    presentation = ["race_results", "driver_standings",
                    "constructor_standings", "calculated_race_results"]

    print(f"\n{processed_schema}:")
    for t in processed:
        try:
            n = session.sql(f"SELECT COUNT(*) cnt FROM {processed_schema}.{t}").collect()[0]["cnt"]
            print(f"  {t:<20} {n:>8} 行")
        except Exception:
            print(f"  {t:<20}  (不存在)")

    print(f"\n{presentation_schema}:")
    for t in presentation:
        try:
            n = session.sql(f"SELECT COUNT(*) cnt FROM {presentation_schema}.{t}").collect()[0]["cnt"]
            print(f"  {t:<30} {n:>8} 行")
        except Exception:
            print(f"  {t:<30}  (不存在)")

    session.close()


def run_script(path: Path):
    """在当前进程中执行脚本，共享 sys.path。"""
    code = compile(path.read_text(), str(path), "exec")
    exec(code, {"__file__": str(path), "__name__": "__main__"})


def main():
    print("=" * 60)
    print("spark2lakehouse-formula1  E2E 全流程")
    mode = "RESET + 全量" if DO_RESET else "增量（保留已有表）"
    print(f"  模式: {mode}")
    print(f"  数据: {DATASETS_DIR}")
    print("=" * 60)

    if DO_RESET:
        print("\n[1/4] RESET — 清空所有表")
        import reset as r
        session = make_session()
        for t in r.PROCESSED_TABLES:
            r.drop(session, "TABLE", processed_schema, t)
        for v in r.PRESENTATION_VIEWS:
            r.drop(session, "VIEW", presentation_schema, v)
        for t in r.PRESENTATION_TABLES:
            r.drop(session, "TABLE", presentation_schema, t)
        session.close()
        print("  完成")
    else:
        print("\n[1/4] RESET — 跳过（增量模式）")

    if not SKIP_UPLOAD:
        print("\n[2/4] UPLOAD — 上传数据文件到 Volume")
        run_script(LAKEHOUSE_DIR / "upload_to_volume.py")
    else:
        print("\n[2/4] UPLOAD — 跳过（--skip-upload）")

    print("\n[3/4] INGEST — 数据摄取")
    run_script(LAKEHOUSE_DIR / "01_ingestion" / "0.ingest_all_files.py")

    print("\n[4/4] TRANSFORM — 数据转换")
    run_script(LAKEHOUSE_DIR / "02_transformation" / "0.transform_all.py")

    step_summary()

    print("\n" + "=" * 60)
    print("E2E 完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
