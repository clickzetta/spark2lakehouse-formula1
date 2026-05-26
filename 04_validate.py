"""
validate.py — 验证 Lakehouse 中 formula1 数据的完整性和一致性

覆盖范围：
  1. 行数检查（processed + presentation 层所有表）
  2. NULL 检查（关键列不允许为空）
  3. 数据一致性（presentation 层能从 processed 层推导出来）
  4. 业务规则（position 范围、points 非负、calculated_points 公式、rank 起始值）
  5. 重复记录检测

用法：
  cd 03_lakehouse
  python validate.py
"""

import sys
import os
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from clickzetta.zettapark.session import Session
from includes.configuration import SCHEMA_NAME, processed_schema, presentation_schema

# ── ANSI 颜色 ──────────────────────────────────────────────────────────────────
GREEN  = "\033[32m"
RED    = "\033[31m"
YELLOW = "\033[33m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

passed = 0
failed = 0
warned = 0


def ok(msg):
    global passed
    passed += 1
    print(f"  {GREEN}✓{RESET} {msg}")


def fail(msg, detail=None):
    global failed
    failed += 1
    print(f"  {RED}✗ FAIL{RESET} {msg}")
    if detail:
        print(f"         {YELLOW}{detail}{RESET}")


def warn(msg, detail=None):
    global warned
    warned += 1
    print(f"  {YELLOW}⚠ WARN{RESET} {msg}")
    if detail:
        print(f"         {detail}")


def section(title):
    print(f"\n{BOLD}{title}{RESET}")
    print("─" * 60)


def q(session, sql):
    return session.sql(sql).collect()


def scalar(session, sql):
    return q(session, sql)[0][0]


def count(session, table):
    return scalar(session, f"SELECT COUNT(*) FROM {table}")


# ── 连接 ───────────────────────────────────────────────────────────────────────
session = Session.builder.configs({
    "username":  os.environ["CLICKZETTA_USERNAME"],
    "password":  os.environ["CLICKZETTA_PASSWORD"],
    "service":   os.environ["CLICKZETTA_SERVICE"],
    "instance":  os.environ["CLICKZETTA_INSTANCE"],
    "workspace": os.environ["CLICKZETTA_WORKSPACE"],
    "schema":    SCHEMA_NAME,
    "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default_ap"),
}).create()

p  = processed_schema
pr = presentation_schema

print(f"\n{BOLD}Formula1 Lakehouse 数据验证{RESET}")
print(f"processed   : {p}")
print(f"presentation: {pr}")

# ══════════════════════════════════════════════════════════════════════════════
# 1. 行数检查
# ══════════════════════════════════════════════════════════════════════════════
section("1. 行数检查")

row_counts = {}

TABLES = {
    "processed":     ["circuits", "races", "constructors", "drivers",
                      "results", "pit_stops", "lap_times", "qualifying"],
    "presentation":  ["race_results", "driver_standings",
                      "constructor_standings", "calculated_race_results"],
}

for layer, tables in TABLES.items():
    schema = p if layer == "processed" else pr
    for t in tables:
        full = f"{schema}.{t}"
        try:
            n = count(session, full)
            row_counts[full] = n
            if n > 0:
                ok(f"{full}: {n:,} 行")
            else:
                fail(f"{full}: 0 行（表为空）")
        except Exception as e:
            row_counts[full] = -1
            warn(f"{full}: 表不存在或查询失败", str(e)[:80])

# ══════════════════════════════════════════════════════════════════════════════
# 2. NULL 检查（关键列）
# ══════════════════════════════════════════════════════════════════════════════
section("2. NULL 检查（关键列）")

NULL_CHECKS = [
    (f"{p}.circuits",      ["circuit_id", "circuit_ref", "location", "country"]),
    (f"{p}.races",         ["race_id", "race_year", "circuit_ref", "name"]),
    (f"{p}.drivers",       ["driver_id", "driver_ref", "name", "nationality"]),
    (f"{p}.constructors",  ["constructor_id", "constructor_ref", "name"]),
    (f"{p}.results",       ["result_id", "race_id", "driver_id", "constructor_id", "position", "points"]),
    (f"{pr}.race_results",          ["race_id", "race_year", "driver_name", "team", "points", "position"]),
    (f"{pr}.driver_standings",      ["race_year", "driver_name", "total_points", "rank"]),
    (f"{pr}.constructor_standings", ["race_year", "team", "total_points", "rank"]),
    (f"{pr}.calculated_race_results", ["race_year", "driver_name", "team_name", "points", "position"]),
]

for table, cols in NULL_CHECKS:
    if row_counts.get(table, -1) <= 0:
        warn(f"{table}: 跳过（表为空或不存在）")
        continue
    for col in cols:
        try:
            nulls = scalar(session, f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
            if nulls == 0:
                ok(f"{table}.{col}: 无 NULL")
            else:
                fail(f"{table}.{col}: {nulls} 行为 NULL")
        except Exception as e:
            warn(f"{table}.{col}: 检查失败", str(e)[:80])

# ══════════════════════════════════════════════════════════════════════════════
# 3. 重复记录检测
# ══════════════════════════════════════════════════════════════════════════════
section("3. 重复记录检测")

dup_checks = [
    (
        "results: 同一场比赛同一车手不应重复",
        f"SELECT COUNT(*) FROM (SELECT race_id, driver_id, COUNT(*) AS cnt FROM {p}.results GROUP BY race_id, driver_id HAVING cnt > 1)",
    ),
    (
        "results: 同一场比赛同一名次不应重复",
        f"SELECT COUNT(*) FROM (SELECT race_id, position, COUNT(*) AS cnt FROM {p}.results WHERE position IS NOT NULL GROUP BY race_id, position HAVING cnt > 1)",
    ),
    (
        "race_results: 同一场比赛同一车手不应重复",
        f"SELECT COUNT(*) FROM (SELECT race_id, driver_name, COUNT(*) AS cnt FROM {pr}.race_results GROUP BY race_id, driver_name HAVING cnt > 1)",
    ),
    (
        "drivers: driver_ref 不应重复",
        f"SELECT COUNT(*) FROM (SELECT driver_ref, COUNT(*) AS cnt FROM {p}.drivers GROUP BY driver_ref HAVING cnt > 1)",
    ),
    (
        "constructors: constructor_ref 不应重复",
        f"SELECT COUNT(*) FROM (SELECT constructor_ref, COUNT(*) AS cnt FROM {p}.constructors GROUP BY constructor_ref HAVING cnt > 1)",
    ),
]

for label, sql in dup_checks:
    try:
        dups = scalar(session, sql)
        if dups == 0:
            ok(label)
        else:
            fail(label, f"{dups} 组重复")
    except Exception as e:
        warn(label, str(e)[:80])

# ══════════════════════════════════════════════════════════════════════════════
# 4. 业务规则
# ══════════════════════════════════════════════════════════════════════════════
section("4. 业务规则")

biz_checks = [
    (
        "results.position 在 1–20 之间（非 NULL 时）",
        f"SELECT COUNT(*) FROM {p}.results WHERE position IS NOT NULL AND (position < 1 OR position > 20)",
        "==", 0,
    ),
    (
        "results.points 非负",
        f"SELECT COUNT(*) FROM {p}.results WHERE points < 0",
        "==", 0,
    ),
    (
        "races.race_year 合理范围（1950–2030）",
        f"SELECT COUNT(*) FROM {p}.races WHERE race_year < 1950 OR race_year > 2030",
        "==", 0,
    ),
    (
        "calculated_race_results.position <= 10（只取前 10 名）",
        f"SELECT COUNT(*) FROM {pr}.calculated_race_results WHERE position > 10",
        "==", 0,
    ),
    (
        "calculated_race_results.calculated_points = 11 - position",
        f"SELECT COUNT(*) FROM {pr}.calculated_race_results WHERE calculated_points != 11 - position",
        "==", 0,
    ),
    (
        "driver_standings.rank 最小值为 1",
        f"SELECT MIN(rank) FROM {pr}.driver_standings",
        "==", 1,
    ),
    (
        "constructor_standings.rank 最小值为 1",
        f"SELECT MIN(rank) FROM {pr}.constructor_standings",
        "==", 1,
    ),
    (
        "driver_standings.total_points > 0",
        f"SELECT COUNT(*) FROM {pr}.driver_standings WHERE total_points <= 0",
        "==", 0,
    ),
    (
        "constructor_standings.total_points > 0",
        f"SELECT COUNT(*) FROM {pr}.constructor_standings WHERE total_points <= 0",
        "==", 0,
    ),
]

for label, sql, op, expected in biz_checks:
    try:
        val = scalar(session, sql)
        passed_check = (val == expected) if op == "==" else (val >= expected)
        if passed_check:
            ok(label)
        else:
            fail(label, f"期望 {op} {expected}，实际 {val}")
    except Exception as e:
        warn(label, str(e)[:80])

# ══════════════════════════════════════════════════════════════════════════════
# 5. 数据一致性（presentation 能从 processed 推导）
# ══════════════════════════════════════════════════════════════════════════════
section("5. 数据一致性（presentation ↔ processed）")

# race_results 的 race_id 全部来自 processed.races
try:
    orphan = scalar(session, f"""
        SELECT COUNT(DISTINCT rr.race_id)
        FROM {pr}.race_results rr
        LEFT JOIN {p}.races rc ON rr.race_id = rc.race_id
        WHERE rc.race_id IS NULL
    """)
    if orphan == 0:
        ok("race_results.race_id 全部存在于 processed.races")
    else:
        fail("race_results 中有 race_id 在 processed.races 中不存在", f"{orphan} 个")
except Exception as e:
    warn("race_results.race_id 检查失败", str(e)[:80])

# race_results 的 race_year 与 processed.races 一致
try:
    mismatch = scalar(session, f"""
        SELECT COUNT(*)
        FROM {pr}.race_results rr
        JOIN {p}.races rc ON rr.race_id = rc.race_id
        WHERE rr.race_year != rc.race_year
    """)
    if mismatch == 0:
        ok("race_results.race_year 与 processed.races 一致")
    else:
        fail("race_results.race_year 与 processed.races 不一致", f"{mismatch} 行")
except Exception as e:
    warn("race_year 一致性检查失败", str(e)[:80])

# driver_standings.total_points 与从 race_results 聚合的结果一致
try:
    rows = q(session, f"""
        SELECT ds.driver_name, ds.total_points AS ds_pts,
               COALESCE(agg.agg_pts, 0) AS agg_pts
        FROM {pr}.driver_standings ds
        LEFT JOIN (
            SELECT driver_name, SUM(points) AS agg_pts
            FROM {pr}.race_results
            GROUP BY driver_name
        ) agg ON ds.driver_name = agg.driver_name
        WHERE ABS(ds.total_points - COALESCE(agg.agg_pts, 0)) > 0.01
    """)
    if len(rows) == 0:
        ok("driver_standings.total_points 与 race_results 聚合一致")
    else:
        fail("driver_standings.total_points 与 race_results 聚合不一致",
             f"{len(rows)} 个车手分值不匹配")
        for r in rows[:3]:
            print(f"         {r[0]}: standings={r[1]}, race_results 聚合={r[2]}")
except Exception as e:
    warn("driver_standings 分值一致性检查失败", str(e)[:80])

# constructor_standings.total_points 与从 race_results 聚合的结果一致
try:
    rows = q(session, f"""
        SELECT cs.team, cs.total_points AS cs_pts,
               COALESCE(agg.agg_pts, 0) AS agg_pts
        FROM {pr}.constructor_standings cs
        LEFT JOIN (
            SELECT team, SUM(points) AS agg_pts
            FROM {pr}.race_results
            GROUP BY team
        ) agg ON cs.team = agg.team
        WHERE ABS(cs.total_points - COALESCE(agg.agg_pts, 0)) > 0.01
    """)
    if len(rows) == 0:
        ok("constructor_standings.total_points 与 race_results 聚合一致")
    else:
        fail("constructor_standings.total_points 与 race_results 聚合不一致",
             f"{len(rows)} 个车队分值不匹配")
        for r in rows[:3]:
            print(f"         {r[0]}: standings={r[1]}, race_results 聚合={r[2]}")
except Exception as e:
    warn("constructor_standings 分值一致性检查失败", str(e)[:80])

# calculated_race_results 的 race_id 全部来自 processed.races
try:
    orphan = scalar(session, f"""
        SELECT COUNT(DISTINCT cr.race_id)
        FROM {pr}.calculated_race_results cr
        LEFT JOIN {p}.races rc ON cr.race_id = rc.race_id
        WHERE rc.race_id IS NULL
    """)
    if orphan == 0:
        ok("calculated_race_results.race_id 全部存在于 processed.races")
    else:
        fail("calculated_race_results 中有 race_id 在 processed.races 中不存在", f"{orphan} 个")
except Exception as e:
    warn("calculated_race_results.race_id 检查失败", str(e)[:80])

# ══════════════════════════════════════════════════════════════════════════════
# 汇总
# ══════════════════════════════════════════════════════════════════════════════
session.close()

total = passed + failed + warned
print(f"\n{'═' * 60}")
print(f"{BOLD}验证完成{RESET}  共 {total} 项")
print(f"  {GREEN}通过{RESET} {passed}   {RED}失败{RESET} {failed}   {YELLOW}警告{RESET} {warned}")
print("═" * 60)

if failed > 0:
    sys.exit(1)
