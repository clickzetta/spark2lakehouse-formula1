"""
test_partition_and_view.py — 测试分区表写入和 CREATE VIEW

测试项：
  1. saveAsTable + partitionBy 参数（显式分区）
  2. 先建分区表，再 saveAsTable 写入（隐式分区）
  3. CREATE VIEW（持久化视图）
  4. CREATE OR REPLACE VIEW
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(line_buffering=True)

from dotenv import load_dotenv
load_dotenv()

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from includes.configuration import SCHEMA_NAME

session = Session.builder.configs({
    "username":  os.environ["CLICKZETTA_USERNAME"],
    "password":  os.environ["CLICKZETTA_PASSWORD"],
    "service":   os.environ["CLICKZETTA_SERVICE"],
    "instance":  os.environ["CLICKZETTA_INSTANCE"],
    "workspace": os.environ["CLICKZETTA_WORKSPACE"],
    "schema":    SCHEMA_NAME,
    "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default_ap"),
}).create()

T = f"{SCHEMA_NAME}._part_test"
V = f"{SCHEMA_NAME}._view_test"

results = []

def check(name, fn):
    try:
        result = fn()
        msg = str(result) if result else ""
        print(f"  ✅ PASS  {name}" + (f"  → {msg}" if msg else ""))
        results.append((name, "PASS", msg))
    except Exception as e:
        msg = str(e)[:150]
        print(f"  ❌ FAIL  {name}")
        print(f"          {msg}")
        results.append((name, "FAIL", msg))

# 测试数据：3 个赛季
base_df = session.create_dataframe(
    [[2021, 1, "hamilton", 25],
     [2021, 2, "verstappen", 18],
     [2022, 1, "verstappen", 25],
     [2022, 2, "leclerc", 18],
     [2023, 1, "verstappen", 25],
     [2023, 2, "perez", 18]],
    schema=["race_year", "race_id", "driver", "points"]
)

# 清理
for name in [T, V]:
    try:
        session.sql(f"DROP TABLE IF EXISTS {name}").collect()
        session.sql(f"DROP VIEW IF EXISTS {name}").collect()
    except Exception:
        pass

print("=== 1. saveAsTable + partitionBy 参数 ===")

def test_saveastable_partitionby():
    session.sql(f"DROP TABLE IF EXISTS {T}").collect()
    base_df.write.saveAsTable(T, mode="overwrite", partition_by=["race_year"])
    # 验证分区是否生效
    rows = session.sql(f"SHOW PARTITIONS {T}").collect()
    return f"{len(rows)} 个分区"

def test_saveastable_partitionby_overwrite_existing():
    # 再次写入，覆盖
    base_df.write.saveAsTable(T, mode="overwrite", partition_by=["race_year"])
    count = session.sql(f"SELECT COUNT(*) AS n FROM {T}").collect()[0]["N"]
    return f"行数={count}"

check("saveAsTable + partition_by=['race_year']（新表）", test_saveastable_partitionby)
check("saveAsTable + partition_by（已有分区表，overwrite）", test_saveastable_partitionby_overwrite_existing)

print()
print("=== 2. 隐式分区：先建表再写入 ===")

def test_implicit_partition_create_table():
    session.sql(f"DROP TABLE IF EXISTS {T}").collect()
    session.sql(f"""
        CREATE TABLE {T} (
            race_year INT,
            race_id   INT,
            driver    STRING,
            points    INT
        ) PARTITION BY (race_year)
    """).collect()
    return "建表成功"

def test_implicit_partition_insert():
    # 建好分区表后，直接 saveAsTable append 写入
    base_df.write.saveAsTable(T, mode="append")
    count = session.sql(f"SELECT COUNT(*) AS n FROM {T}").collect()[0]["N"]
    return f"行数={count}"

def test_implicit_partition_overwrite():
    # overwrite 写入已有分区表
    base_df.write.saveAsTable(T, mode="overwrite")
    count = session.sql(f"SELECT COUNT(*) AS n FROM {T}").collect()[0]["N"]
    return f"行数={count}"

def test_implicit_partition_show():
    rows = session.sql(f"SHOW PARTITIONS {T}").collect()
    parts = [str(dict(r.asDict())) for r in rows]
    return f"{len(rows)} 个分区: {parts}"

check("CREATE TABLE ... PARTITION BY (race_year)", test_implicit_partition_create_table)
check("saveAsTable append 写入已有分区表", test_implicit_partition_insert)
check("saveAsTable overwrite 写入已有分区表", test_implicit_partition_overwrite)
check("SHOW PARTITIONS 确认分区", test_implicit_partition_show)

print()
print("=== 3. CREATE VIEW ===")

def test_create_view():
    session.sql(f"DROP VIEW IF EXISTS {V}").collect()
    session.sql(f"CREATE VIEW {V} AS SELECT * FROM {T} WHERE race_year = 2023").collect()
    count = session.sql(f"SELECT COUNT(*) AS n FROM {V}").collect()[0]["N"]
    return f"行数={count}"

def test_create_or_replace_view():
    session.sql(f"CREATE OR REPLACE VIEW {V} AS SELECT * FROM {T} WHERE race_year >= 2022").collect()
    count = session.sql(f"SELECT COUNT(*) AS n FROM {V}").collect()[0]["N"]
    return f"行数={count}"

def test_create_temp_view_sql():
    # 确认 TEMP VIEW 确实不支持
    try:
        session.sql(f"CREATE OR REPLACE TEMP VIEW _tv_test AS SELECT 1 AS x").collect()
        return "意外成功"
    except Exception as e:
        return f"预期失败: {str(e)[:80]}"

check("CREATE VIEW（持久化视图）", test_create_view)
check("CREATE OR REPLACE VIEW", test_create_or_replace_view)
check("CREATE TEMP VIEW（预期失败）", test_create_temp_view_sql)

# 清理
for name in [T, V]:
    try:
        session.sql(f"DROP TABLE IF EXISTS {name}").collect()
        session.sql(f"DROP VIEW IF EXISTS {name}").collect()
    except Exception:
        pass
session.close()

print()
print("=" * 60)
passed = sum(1 for _, s, _ in results if s == "PASS")
failed = sum(1 for _, s, _ in results if s == "FAIL")
print(f"结果：{passed} 通过，{failed} 失败（共 {len(results)} 项）")
