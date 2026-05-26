# 数据兼容性：Jolpica API 与原始 Databricks 数据集的差异

本文记录从 Jolpica API 获取 F1 数据时遇到的兼容性问题，以及与原始 Databricks 项目数据集的结构差异。

---

## 1. 主键类型：字符串 ref 替代整数 ID

### 现象

原始 Databricks 项目使用整数 ID 作为主键（`driverId INT`、`constructorId INT`、`circuitId INT`），JOIN 条件为：

```python
results_df.join(drivers_df, results_df["driverId"] == drivers_df["driverId"])
```

Jolpica API 返回的是字符串标识符（`driverId STRING`，值如 `"max_verstappen"`、`"hamilton"`）。

### 根因

Jolpica API 基于 Ergast API 设计，使用人类可读的 slug 作为标识符，而非数据库自增 ID。

### 解决方案

迁移后的 JOIN 条件改为字符串 ref 匹配：

```python
# ❌ 原始 Databricks 写法（整数 ID）
results_df.join(drivers_df, results_df["driverId"] == drivers_df["driverId"])

# ✅ Jolpica 数据写法（字符串 ref）
results_df.join(drivers_df, results_df["driverId"] == drivers_df["driver_ref"])
results_df.join(constructors_df, results_df["constructorId"] == constructors_df["constructor_ref"])
```

### 影响范围

- `results` → `drivers` JOIN
- `results` → `constructors` JOIN
- `qualifying` → `drivers` JOIN
- `qualifying` → `constructors` JOIN

---

## 2. 数据格式：NDJSON 而非 JSON 数组

### 现象

ZettaPark 的 `session.read.json()` 要求每行一个 JSON 对象（NDJSON 格式）：

```
{"raceId": 1, "driverId": "hamilton", "lap": 1, ...}
{"raceId": 1, "driverId": "verstappen", "lap": 1, ...}
```

如果文件是 JSON 数组格式（`[{...}, {...}]`），读取会失败或只读到第一行。

### 解决方案

下载脚本在写入时逐行输出：

```python
with open(LOCAL_LT_FILE, "w", encoding="utf-8") as out:
    for record in records:
        out.write(json.dumps(record) + "\n")  # 每条记录单独一行
```

---

## 3. lap_times：milliseconds 字段缺失

### 现象

原始 Databricks 数据集的 `lap_times` 表有 `milliseconds` 字段（整数，圈速毫秒数）。Jolpica API 的 laps 端点只返回 `time` 字段（字符串，格式如 `"1:23.456"`），不提供毫秒数。

### 解决方案

下载时将 `milliseconds` 填充为 `0`，保留字段以维持 schema 兼容性：

```python
rec = {
    "raceId":       race_id,
    "driverId":     timing["driverId"],
    "lap":          lap_num,
    "position":     int(timing.get("position", 0) or 0),
    "time":         timing.get("time", ""),
    "milliseconds": 0,   # Jolpica API 不提供，填充占位
}
```

如需毫秒数，可在摄取后用 SQL 从 `time` 字段解析：

```sql
-- 将 "1:23.456" 转换为毫秒
SELECT
    time,
    (CAST(SPLIT(time, ':')[0] AS INT) * 60000
     + CAST(SPLIT(SPLIT(time, ':')[1], '\\.')[0] AS INT) * 1000
     + CAST(SPLIT(SPLIT(time, ':')[1], '\\.')[1] AS INT)) AS milliseconds
FROM f1_processed.lap_times
WHERE time != ''
```

---

## 4. lap_times：分页 API

### 现象

Jolpica laps 端点默认每次返回 30 条，最多 100 条。一场比赛有 1000–1500 条圈速记录，必须分页获取。

### 解决方案

循环请求直到 `offset >= total`：

```python
offset, limit = 0, 100
while True:
    url = f"{BASE_URL}/{year}/{round_num}/laps.json?limit={limit}&offset={offset}"
    data = fetch_json(url)
    api_total = int(data["MRData"]["total"])
    # ... 处理数据 ...
    offset += limit
    if offset >= api_total:
        break
```

---

## 5. results：多赛季数据混入导致重复记录

### 现象

`f1_processed.results` 表中出现同一 `race_id` + `position` 的重复记录，例如：

```
race_id=1, position=1, driverId=max_verstappen, points=25
race_id=1, position=1, driverId=leclerc,        points=26
```

验证脚本会报告：`results 同场次同名次重复: 3 条`。

### 根因

Jolpica API 的 results 端点在某些情况下会为同一场比赛返回来自不同赛季的数据（数据源问题），或者 `race_id` 分配与 API 返回的 round 编号存在偏差。

### 影响范围

重复记录不只影响 ingestion 层，**transformation 层的 JOIN 也会放大重复**。例如 `race_results` 表将 results 与 drivers、constructors、races 做多表 JOIN，同一 `(driver_name, race_id)` 可能出现多行，触发 Lakehouse MERGE INTO 的 `EXISTS_NONDETERMINISTIC_ROWS` 错误。

### 解决方案

在所有调用 `merge_delta_data()` 之前加 `dropDuplicates()`，覆盖 ingestion 和 transformation 两层：

```python
# ingestion 层（以 results 为例）
deduped_df = final_df.dropDuplicates(["result_id", "race_id"])
merge_delta_data(deduped_df, processed_schema, "results", merge_condition, "race_id")

# transformation 层（以 race_results 为例）
deduped_df = final_df.dropDuplicates(["driver_name", "race_id"])
merge_delta_data(deduped_df, presentation_schema, "race_results", merge_condition, "race_id")
```

对于使用 SQL MERGE 的脚本（`4.calculated_race_results.py`），在 VIEW 定义中加 `QUALIFY ROW_NUMBER()` 去重：

```sql
CREATE OR REPLACE VIEW schema.race_result_updated AS
SELECT ...
FROM results JOIN drivers ... JOIN constructors ... JOIN races ...
WHERE results.position <= 10
QUALIFY ROW_NUMBER() OVER (PARTITION BY drivers.driver_id, races.race_id ORDER BY results.result_id) = 1
```

---

## 6. drivers / constructors：历史数据 vs 现役数据不匹配

### 现象

`f1_processed.drivers` 表包含 100 名历史车手（含已退役车手），而 `f1_processed.results` 只包含 2018–2023 赛季的现役车手。直接做 referential integrity 检查会发现大量"孤儿"记录。

### 根因

Jolpica API 的 `/drivers` 端点返回所有历史车手，而 `/results` 只返回指定赛季的参赛车手。两个端点的数据范围不同。

### 解决方案

验证脚本不做跨表 referential integrity 检查，改为检查**跨层一致性**（processed 层聚合结果与 presentation 层是否一致）：

```python
# ❌ 不适合：referential integrity（会有大量误报）
orphan_results = results_df.join(drivers_df, "driverId", "left_anti").count()

# ✅ 适合：跨层一致性
# 检查 driver_standings.total_points 是否等于 race_results 的聚合值
```

---

## 7. qualifying：部分场次无数据

### 现象

某些早期场次（2018 年部分）的 qualifying 数据在 Jolpica API 中返回空列表。下载脚本会跳过这些场次并打印 `[SKIP]`。

### 影响

`f1_processed.qualifying` 表的行数会少于理论值（125 场 × 20 名车手 = 2500 条），实际约 2400+ 条。这是正常现象，不影响分析。

---

## 总结

| 差异点 | 原始 Databricks 数据 | Jolpica API 数据 | 处理方式 |
|--------|---------------------|-----------------|---------|
| 主键类型 | 整数 ID | 字符串 ref | JOIN 条件改用 ref 字段 |
| 文件格式 | CSV / JSON 数组 | NDJSON（每行一条） | 下载时逐行写入 |
| lap_times milliseconds | 有（整数） | 无 | 填充 0，保留字段 |
| lap_times 分页 | 单文件 | 需分页（limit/offset） | 循环请求 |
| results 重复记录 | 无 | 偶有（数据源问题） | ingestion + transformation 均加 `dropDuplicates()`；SQL MERGE 视图加 `QUALIFY ROW_NUMBER()` |
| drivers 覆盖范围 | 与 results 匹配 | 历史全量 vs 现役 | 不做 referential integrity 检查 |
| qualifying 缺失场次 | 完整 | 部分场次无数据 | 跳过，属正常现象 |
