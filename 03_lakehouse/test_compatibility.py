"""
test_compatibility.py — 验证 ZettaPark 各 API 的实际行为

目的：确认哪些限制是 ZettaPark 版本 bug，哪些是用法问题，哪些已修复。

用法：
  cd spark2lakehouse-formula1
  python 03_lakehouse/test_compatibility.py
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
from clickzetta.zettapark.window import Window
from includes.configuration import SCHEMA_NAME

import clickzetta.zettapark
print(f"ZettaPark 版本: {clickzetta.zettapark.__version__}\n")

session = Session.builder.configs({
    "username":  os.environ["CLICKZETTA_USERNAME"],
    "password":  os.environ["CLICKZETTA_PASSWORD"],
    "service":   os.environ["CLICKZETTA_SERVICE"],
    "instance":  os.environ["CLICKZETTA_INSTANCE"],
    "workspace": os.environ["CLICKZETTA_WORKSPACE"],
    "schema":    SCHEMA_NAME,
    "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default_ap"),
}).create()

TEST_TABLE = f"{SCHEMA_NAME}._compat_test"
results = []

def check(name, fn):
    try:
        fn()
        print(f"  ✅ PASS  {name}")
        results.append((name, "PASS", ""))
    except Exception as e:
        msg = str(e)[:120]
        print(f"  ❌ FAIL  {name}")
        print(f"          {msg}")
        results.append((name, "FAIL", msg))

# ── 准备测试数据 ────────────────────────────────────────────────────────────────
base_df = session.create_dataframe(
    [[1, "hamilton", 25], [2, "verstappen", 18], [3, "leclerc", 15]],
    schema=["id", "driver", "points"]
)

print("=== 1. withColumn 系列 ===")

def test_withcolumn_simple():
    df = base_df.withColumn("doubled", F.col("points") * 2)
    df.collect()

def test_withcolumn_after_join():
    df2 = session.create_dataframe([[1, "GBR"], [2, "NLD"], [3, "MON"]], schema=["id", "country"])
    joined = base_df.join(df2, "id")
    df = joined.withColumn("label", F.concat(F.col("driver"), F.lit("-"), F.col("country")))
    df.collect()

def test_withcolumn_window():
    window_spec = Window.orderBy(F.desc("points"))
    df = base_df.withColumn("rank", F.rank().over(window_spec))
    df.collect()

def test_withcolumnrenamed():
    df = base_df.withColumnRenamed("points", "score")
    df.collect()

def test_drop():
    df = base_df.drop("points")
    df.collect()

check("withColumn 简单计算列", test_withcolumn_simple)
check("withColumn JOIN 后添加列", test_withcolumn_after_join)
check("withColumn + Window 函数", test_withcolumn_window)
check("withColumnRenamed", test_withcolumnrenamed)
check("drop()", test_drop)

print()
print("=== 2. saveAsTable 系列 ===")

# 清理
try:
    session.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}").collect()
except Exception:
    pass

def test_saveastable_new_no_mode():
    session.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}").collect()
    base_df.write.saveAsTable(TEST_TABLE)

def test_saveastable_new_overwrite():
    session.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}").collect()
    base_df.write.saveAsTable(TEST_TABLE, mode="overwrite")

def test_saveastable_existing_overwrite():
    # 先建表
    session.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}").collect()
    base_df.write.saveAsTable(TEST_TABLE, mode="overwrite")
    # 再覆盖
    base_df.write.saveAsTable(TEST_TABLE, mode="overwrite")

def test_saveastable_existing_append():
    base_df.write.saveAsTable(TEST_TABLE, mode="append")

check("saveAsTable 新表（无 mode）", test_saveastable_new_no_mode)
check("saveAsTable 新表（mode=overwrite）", test_saveastable_new_overwrite)
check("saveAsTable 已有表（mode=overwrite）", test_saveastable_existing_overwrite)
check("saveAsTable 已有表（mode=append）", test_saveastable_existing_append)

print()
print("=== 3. session.sql 惰性执行 ===")

def test_sql_ddl_without_collect():
    # 不加 collect，看表是否真的被创建
    tmp = f"{SCHEMA_NAME}._compat_lazy_test"
    session.sql(f"DROP TABLE IF EXISTS {tmp}").collect()
    session.sql(f"CREATE TABLE {tmp} AS SELECT 1 AS x")  # 故意不加 collect
    # 检查表是否存在
    try:
        session.sql(f"DESCRIBE TABLE {tmp}").collect()
        # 如果能 describe，说明表被创建了（collect 不是必须的）
        session.sql(f"DROP TABLE IF EXISTS {tmp}").collect()
        raise AssertionError("表被创建了——说明不加 collect 也会执行")
    except AssertionError:
        raise
    except Exception:
        # describe 失败说明表没创建，符合预期（惰性执行）
        pass

check("CREATE TABLE 不加 collect 确实不执行", test_sql_ddl_without_collect)

print()
print("=== 4. CREATE TEMP VIEW ===")

def test_create_temp_view_sql():
    session.sql("CREATE OR REPLACE TEMP VIEW _compat_tv AS SELECT 1 AS x").collect()
    session.sql("SELECT * FROM _compat_tv").collect()

def test_create_or_replace_temp_view():
    base_df.create_or_replace_temp_view("_compat_tv2")
    session.sql("SELECT * FROM _compat_tv2").collect()

check("CREATE TEMP VIEW（SQL 方式）", test_create_temp_view_sql)
check("create_or_replace_temp_view（DataFrame 方式）", test_create_or_replace_temp_view)

print()
print("=== 5. JOIN 后列名 ===")

def test_join_column_names_bare_string():
    df2 = session.create_dataframe([[1, "GBR"], [2, "NLD"], [3, "MON"]], schema=["id", "country"])
    joined = base_df.join(df2, base_df["id"] == df2["id"])
    # 用裸字符串 select，看列名是否被污染
    result = joined.select("driver", "country").collect()
    cols = [f.name for f in joined.select("driver", "country").schema.fields]
    for c in cols:
        if "_" in c and len(c) > 10 and c[0] in "lr":
            raise AssertionError(f"列名被加前缀: {cols}")

def test_join_column_names_explicit():
    df2 = session.create_dataframe([[1, "GBR"], [2, "NLD"], [3, "MON"]], schema=["id", "country"])
    joined = base_df.join(df2, base_df["id"] == df2["id"])
    result = joined.select(
        base_df["driver"].alias("driver"),
        df2["country"].alias("country"),
    ).collect()

check("JOIN 后裸字符串 select（列名是否污染）", test_join_column_names_bare_string)
check("JOIN 后显式 df['col'].alias() select", test_join_column_names_explicit)

# ── 清理 ────────────────────────────────────────────────────────────────────────
try:
    session.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}").collect()
except Exception:
    pass
session.close()

# ── 汇总 ────────────────────────────────────────────────────────────────────────
print()
print("=" * 60)
passed = sum(1 for _, s, _ in results if s == "PASS")
failed = sum(1 for _, s, _ in results if s == "FAIL")
print(f"结果：{passed} 通过，{failed} 失败（共 {len(results)} 项）")
print()
if failed:
    print("失败项：")
    for name, status, msg in results:
        if status == "FAIL":
            print(f"  - {name}")
            if msg:
                print(f"    {msg}")
