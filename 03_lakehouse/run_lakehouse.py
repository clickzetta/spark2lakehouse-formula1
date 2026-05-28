#!/usr/bin/env python3
"""Run Lakehouse SQL files against ClickZetta Lakehouse via Python connector."""

import os
import sys
import glob
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

try:
    import clickzetta
except ImportError:
    print("Install clickzetta connector: pip install clickzetta-connector")
    sys.exit(1)


def get_conn():
    return clickzetta.connect(
        service=os.environ["CLICKZETTA_SERVICE"],
        instance=os.environ["CLICKZETTA_INSTANCE"],
        workspace=os.environ["CLICKZETTA_WORKSPACE"],
        username=os.environ["CLICKZETTA_USERNAME"],
        password=os.environ["CLICKZETTA_PASSWORD"],
        schema=os.environ.get("CLICKZETTA_SCHEMA", "f1_raw"),
        vcluster=os.environ.get("CLICKZETTA_VCLUSTER", "default_ap"),
    )


def run_sql_file(cursor, path: Path, params: dict):
    sql = path.read_text()
    for k, v in params.items():
        sql = sql.replace(f"<{k}>", v)
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        print(f"  >> {stmt[:80]}...")
        cursor.execute(stmt)
    print(f"  [OK] {path.name}")


def main():
    # Usage: python run_lakehouse.py [ddl|transformation|analysis]
    layer = sys.argv[1] if len(sys.argv) > 1 else None
    params = {}

    base = Path(__file__).parent
    if layer:
        patterns = [str(base / layer / "*.sql")]
    else:
        patterns = [
            str(base / "04_create_raw_tables" / "*.sql"),
            str(base / "02_transformation" / "*.sql"),
            str(base / "03_analysis" / "*.sql"),
        ]

    sql_files = []
    for pattern in patterns:
        sql_files.extend(sorted(glob.glob(pattern)))

    if not sql_files:
        print(f"No SQL files found under {base}/")
        sys.exit(1)

    print(f"Connecting to {os.environ['CLICKZETTA_SERVICE']} ...")
    conn = get_conn()
    cursor = conn.cursor()

    for f in sql_files:
        print(f"\nRunning {f}")
        run_sql_file(cursor, Path(f), params)

    cursor.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
