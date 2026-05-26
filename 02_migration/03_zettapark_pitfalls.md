# ZettaPark 迁移踩坑与最佳实践

本文记录将 PySpark 代码迁移到 ZettaPark 过程中实际踩过的坑，每条都有复现场景、根因分析和推荐写法。

> **版本说明**：部分坑已在 ZettaPark 0.1.5 修复，文档中会标注。用 `pip show clickzetta_zettapark_python` 确认当前版本。

---

## 1. `withColumn` 触发服务端 schema 解析

> **0.1.5 已修复** — 升级后 `withColumn` 可正常使用。旧版本项目保留 `select().alias()` 写法无需回改，新项目可直接用 `withColumn`。

### 现象（旧版本）

```python
df = df.withColumn("rank", F.rank().over(window_spec))
# 报错：schema resolution failed / internal prefix mangling
```

### 根因

ZettaPark 旧版本中 `withColumn`、`withColumnRenamed`、`drop()` 会调用 `self._output`，触发对服务端的 schema 预解析请求。在某些版本下，这个请求会失败或返回带内部前缀的列名（如 `r_f6fw_race_id`）。

### 解决方案（旧版本兼容写法）

**改用 `select()` + `.alias()`**，一次性完成所有列操作：

```python
# 旧版本有问题
df = df.withColumn("rank", F.rank().over(window_spec))

# 兼容所有版本的写法
df = df.select(
    "race_year", "driver_name", "total_points", "wins",
    F.rank().over(window_spec).alias("rank"),
)
```

---

## 2. `session.sql()` 惰性执行，不加 `.collect()` 不会运行

### 现象

```python
session.sql("MERGE INTO tgt USING src ON ...")
# 代码不报错，但数据没有变化
```

### 根因

ZettaPark 的 `session.sql()` 返回一个 DataFrame 对象，是**惰性的**——只有触发 action（`collect()`、`show()`、`count()`）才会真正提交到服务端执行。没有 action 时，SQL 语句完全没有发出去。

### 解决方案

```python
# ❌ 不会执行
session.sql("CREATE TABLE ... AS SELECT ...")
session.sql("MERGE INTO ...")

# ✅ 加 .collect() 强制执行
session.sql("CREATE TABLE ... AS SELECT ...").collect()
session.sql("MERGE INTO ...").collect()
```

### 适用规则

> 所有 DDL 和 DML（`CREATE TABLE`、`INSERT`、`MERGE INTO`、`DROP`）都必须加 `.collect()`。查询语句如果只是触发执行也要加，不需要结果时用 `.collect()` 即可。

---

## 3. 多表 JOIN 后列名被加内部前缀

> **0.1.5 已修复** — 裸字符串 select 在当前版本不会污染列名。显式 `df["col"].alias("col")` 写法仍然推荐，可读性更好且对所有版本安全。

### 现象（旧版本）

```python
final_df = results_df.join(drivers_df, ...).join(races_df, ...)
final_df.show()
# 列名变成：r_f6fw_race_id, l_y63n_file_date, ...
```

### 根因

旧版本多表 JOIN 后，ZettaPark 为了区分来源不同的同名列，会给列名加内部哈希前缀，带入到 `saveAsTable` 的建表 schema 中导致下游查询失败。

### 推荐写法（兼容所有版本）

JOIN 后的 `select()` 对每一列显式指定来源 DataFrame 和 `.alias()`，可读性更好，也规避旧版本问题：

```python
final_df = (
    results_df
    .join(race_circuits_df, results_df["race_id"] == race_circuits_df["race_id"])
    .join(drivers_df,       results_df["driver_id"] == drivers_df["driver_ref"])
    .select(
        race_circuits_df["race_id"].alias("race_id"),
        race_circuits_df["race_year"].alias("race_year"),
        drivers_df["name"].alias("driver_name"),
        results_df["points"].alias("points"),
    )
)
```

---

## 4. `saveAsTable` 对不存在的表会失败

> **0.1.5 已修复** — `saveAsTable("schema.t", mode="overwrite")` 对新表和已有表均可正常工作。`merge_delta_data` 封装仍然有价值，因为它实现了 MERGE INTO 增量更新语义，而不只是覆盖写入。

### 现象（旧版本）

```python
df.write.saveAsTable("schema.new_table", mode="overwrite")
# 报错：table does not exist / schema resolution failed
```

### 根因

旧版本 `saveAsTable` 在写入时会先解析目标表的 schema，对于不存在的表，这个解析步骤会失败。

### 解决方案（旧版本 / 需要 MERGE 语义时）

用 `_plan.queries[-1].sql` 提取 DataFrame 的底层 SQL，改用 CTAS：

```python
def merge_delta_data(input_df, db_name, table_name, merge_condition, partition_column):
    session = input_df.session
    full_name = f"{db_name}.{table_name}"

    table_exists = False
    try:
        session.sql(f"DESCRIBE TABLE {full_name}").collect()
        table_exists = True
    except Exception:
        pass

    if table_exists:
        input_df.create_or_replace_temp_view("_merge_src")
        session.sql(f"""
            MERGE INTO {full_name} tgt
            USING _merge_src src
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """).collect()
    else:
        src_sql = input_df._plan.queries[-1].sql.strip()
        session.sql(f"CREATE TABLE {full_name} AS {src_sql}").collect()
```

> **注意**：`_plan.queries[-1].sql` 是内部 API，未来版本可能变更。0.1.5+ 新项目可以直接用 `saveAsTable(t, mode="overwrite")` 建表，只在需要 MERGE 增量语义时才用上面的封装。

---

## 5. MERGE INTO VALUES 子句列名歧义

### 现象

```sql
MERGE INTO tgt
USING upd ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
WHEN NOT MATCHED THEN
    INSERT (race_year, team_name, driver_id, ...)
    VALUES (race_year, team_name, driver_id, ...)  -- ❌ 歧义
```

报错或插入空值。

### 根因

`VALUES` 子句中裸列名在 Lakehouse 中会被解析为目标表（`tgt`）的列，而不是 `upd` 的列。

### 解决方案

`VALUES` 里的列名必须加 `upd.` 前缀：

```sql
WHEN NOT MATCHED THEN
    INSERT (race_year, team_name, driver_id, driver_name, race_id, ...)
    VALUES (upd.race_year, upd.team_name, upd.driver_id, upd.driver_name, upd.race_id, ...)
```

---

## 6. `CREATE TEMP VIEW` 不支持，`CREATE VIEW` 支持

### 现象

```python
session.sql("CREATE TEMP VIEW v AS SELECT ...").collect()
# 报错：CZLH-42000 Syntax error - missing KW_ENDPOINT at 'VIEW'
```

### 根因

ClickZetta Lakehouse SQL 不支持 `CREATE TEMP VIEW` 语法，但支持标准的 `CREATE VIEW`（持久化视图）。

### 解决方案

```python
# ❌ 不支持
session.sql("CREATE TEMP VIEW v AS SELECT ...").collect()

# ✅ 方案 A：CREATE VIEW（持久化，用完需要 DROP VIEW）
session.sql("CREATE OR REPLACE VIEW schema.v AS SELECT ...").collect()
# 注意：DROP 时必须用 DROP VIEW，不能用 DROP TABLE
session.sql("DROP VIEW IF EXISTS schema.v").collect()

# ✅ 方案 B：DataFrame 临时视图（推荐，进程内有效，无需手动清理）
df.create_or_replace_temp_view("v")
session.sql("SELECT * FROM v").collect()
```

方案 B 更接近原始 PySpark 的 `createOrReplaceTempView` 行为，推荐优先使用。

---

## 7. 分区表：`partition_by` 参数不支持，但 `PARTITIONED BY` SQL 可用

### 现象

```python
# saveAsTable 的 partition_by 参数不存在
df.write.saveAsTable("schema.t", mode="overwrite", partition_by=["race_year"])
# 报错：DataFrameWriter.save_as_table() got an unexpected keyword argument 'partition_by'

# CREATE TABLE ... PARTITION BY 语法不支持（注意：是 PARTITION BY，不是 PARTITIONED BY）
session.sql("CREATE TABLE t (...) PARTITION BY (race_year)").collect()
# 报错：CZLH-42000 Syntax error at or near 'PARTITION'
```

### 根因

ZettaPark 0.1.5 的 `DataFrameWriter.save_as_table()` 不接受 `partition_by` 参数。Lakehouse SQL 不支持 `PARTITION BY` 建表语法，但支持 Hive 风格的 `PARTITIONED BY`。

### 解决方案

用 `CREATE TABLE ... PARTITIONED BY (...)` + `INSERT INTO` 手动建分区表：

```python
def _create_partitioned_table(session, full_name, input_df, partition_column):
    fields = input_df.schema.fields
    col_defs = ", ".join(
        f"{f.name.strip('`')} {sql_type(f.datatype)}"
        for f in fields if f.name.strip('`') != partition_column
    )
    part_field = next(f for f in fields if f.name.strip('`') == partition_column)
    part_def = f"{part_field.name.strip('`')} {sql_type(part_field.datatype)}"

    session.sql(
        f"CREATE TABLE {full_name} ({col_defs}) PARTITIONED BY ({part_def})"
    ).collect()

    # 必须显式列名，不能用 SELECT *（见第 8 节）
    all_cols = ", ".join(f.name.strip('`') for f in fields)
    input_df.create_or_replace_temp_view("_insert_src")
    session.sql(
        f"INSERT INTO {full_name} ({all_cols}) SELECT {all_cols} FROM _insert_src"
    ).collect()
```

> **注意**：ZettaPark 的 `StructField` 使用 `datatype`（小写），不是 PySpark 的 `dataType`。

---

## 8. `create_or_replace_temp_view` 后列名被规范化（小写 + 去下划线）

### 现象

```python
df.create_or_replace_temp_view("_src")
session.sql("INSERT INTO schema.t SELECT * FROM _src").collect()
# 报错：cannot resolve column 'raceid'（期望 'race_id'）
```

### 根因

ZettaPark 在将 DataFrame 注册为 temp view 时，会对列名做规范化处理：小写化并去掉下划线（`race_id` → `raceid`）。Lakehouse 对象名统一转小写是设计行为，但去掉下划线导致 `SELECT *` 时 temp view 的列名与目标表不匹配。

### 解决方案

`INSERT INTO` 时显式列出列名，不依赖 `SELECT *`：

```python
all_cols = ", ".join(f.name.strip('`') for f in df.schema.fields)
df.create_or_replace_temp_view("_src")
session.sql(
    f"INSERT INTO schema.t ({all_cols}) SELECT {all_cols} FROM _src"
).collect()
```

Lakehouse 按列名位置匹配，显式列名绕过了 temp view 的列名规范化问题。

---

## 7. 连接配置 `schema` 字段的含义

### 现象

设置 `schema = "f1_processed"` 后，查询 `f1_presentation` 的表时报错 `table not found`。

### 根因

ZettaPark 连接配置中的 `schema` 是**默认 schema**，影响不带 schema 前缀的表名解析。带完整前缀（`f1_presentation.race_results`）的表名不受影响。

### 解决方案

代码中所有表名都写完整的 `schema.table` 格式，不依赖默认 schema：

```python
# ❌ 依赖默认 schema，可移植性差
session.table("race_results")

# ✅ 完整路径，始终明确
session.table(f"{presentation_schema}.race_results")
```

---

## 总结

| 坑点 | 状态 | 根因 | 解决方案 |
|------|------|------|---------|
| `withColumn` 报错或列名乱码 | ✅ 0.1.5 已修复 | 触发服务端 schema 预解析 | 旧版本改用 `select()` + `.alias()` |
| `session.sql()` 不执行 | ⚠️ 设计行为 | 惰性执行机制 | 必须加 `.collect()` |
| JOIN 后列名有内部前缀 | ✅ 0.1.5 已修复 | ZettaPark 自动去歧义 | 推荐仍用 `df["col"].alias("col")` |
| `saveAsTable` 对新表报错 | ✅ 0.1.5 已修复 | 写入前解析目标表 schema | 需 MERGE 语义时仍用封装函数 |
| MERGE VALUES 插入空值 | ⚠️ 平台行为 | 列名歧义解析为 tgt | VALUES 里加 `upd.` 前缀 |
| `CREATE TEMP VIEW` 不支持 | ⚠️ 平台限制 | Lakehouse SQL 不支持该语法 | 改用 `create_or_replace_temp_view()` 或 `CREATE VIEW` |
| 表名解析错误 | ⚠️ 配置行为 | 依赖默认 schema | 全部用 `schema.table` 完整格式 |
| `partition_by` 参数不支持 | ⚠️ 平台限制 | 参数不存在；`PARTITION BY` 语法不支持 | 改用 `PARTITIONED BY` 手写 DDL |
| temp view 列名规范化 | ⚠️ 平台行为 | 列名小写化 + 去下划线（`race_id` → `raceid`） | INSERT INTO 显式列名，不用 `SELECT *` |

---

## 附录：调试技巧

### 查看 DataFrame 的底层 SQL

ZettaPark 的每个 DataFrame 都对应一段 SQL，可以在报错前先打印出来确认逻辑是否正确：

```python
# 查看 DataFrame 将要执行的 SQL
print(df._plan.queries[-1].sql)
```

这在排查列名前缀污染、JOIN 条件错误时非常有用——直接看生成的 SQL 比猜 DataFrame 状态快得多。

> **注意**：`_plan` 是内部 API，未来版本可能变更。仅用于调试，不要在生产代码中依赖它。

---

### 确认 ZettaPark 版本

某些坑（如 `withColumn` 触发 schema 预解析）是特定版本的行为。遇到奇怪报错时先确认版本：

```python
import clickzetta.zettapark
print(clickzetta.zettapark.__version__)
```

或在终端：

```bash
pip show clickzetta_zettapark_python
```

---

### 后台运行脚本时看不到输出

Python 默认对重定向到文件的 stdout 做块缓冲，后台运行时日志会长时间不刷新，看起来像卡住了。

```bash
# ❌ 后台运行，日志可能几分钟不更新
python script.py > output.log &

# ✅ 方案 1：-u 参数强制无缓冲
python -u script.py > output.log &

# ✅ 方案 2：脚本开头加一行
sys.stdout.reconfigure(line_buffering=True)
```

同时，所有 `print()` 调用加 `flush=True`：

```python
print(f"处理 {year} R{round_num}...", flush=True)
```

---

### 快速验证连接是否正常

创建 session 后先跑一条轻量查询，确认连接和权限都 OK，再跑业务逻辑：

```python
session = Session.builder.configs(config).create()
session.sql("SELECT current_user(), current_schema()").show()
```

---

### 报错信息中的内部前缀

如果报错信息或 `show()` 输出中出现类似 `r_f6fw_race_id`、`l_y63n_file_date` 这样的列名，说明触发了 JOIN 后的列名歧义处理。解决方法见 [第 3 节](#3-多表-join-后列名被加内部前缀)。

---

### 分步调试长链式操作

ZettaPark 是惰性执行，链式操作中的错误往往在最后 `.collect()` 时才抛出，难以定位。调试时可以在中间步骤加 `.show()` 强制触发：

```python
# 分步触发，缩小报错范围
step1 = results_df.join(drivers_df, ...)
step1.show()  # 先确认 JOIN 结果正常

step2 = step1.join(races_df, ...)
step2.show()  # 再确认第二个 JOIN

final = step2.select(...)
final.show()  # 最后确认 select
```
