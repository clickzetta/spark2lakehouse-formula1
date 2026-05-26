# PySpark → ZettaPark 语法对照

ZettaPark 的 API 设计与 PySpark 高度一致，大多数代码可以直接复用。本文只列出**有差异的部分**，完全兼容的语法不重复列出。

---

## 环境初始化

| 场景 | PySpark（Databricks） | ZettaPark |
|------|----------------------|-----------|
| 获取 session | `spark`（全局注入，无需创建） | `Session.builder.configs({...}).create()` |
| 导入 functions | `from pyspark.sql import functions as F` | `from clickzetta.zettapark import functions as F` |
| 导入 Window | `from pyspark.sql.window import Window` | `from clickzetta.zettapark.window import Window` |
| 导入类型 | `from pyspark.sql.types import StructType, ...` | `from clickzetta.zettapark.types import StructType, ...` |

---

## 文件读取

| 场景 | PySpark | ZettaPark |
|------|---------|-----------|
| 读 CSV（推断 schema） | `spark.read.option("header", True).csv("/mnt/...")` | `session.read.option("header", True).csv("vol://schema.vol/...")` |
| 读 CSV（显式 schema） | `spark.read.schema(schema).csv(path)` | `session.read.schema(schema).csv(path)` ✅ 相同 |
| 读 JSON（NDJSON） | `spark.read.json(path)` | `session.read.schema(schema).json(path)`（建议显式 schema） |
| 文件路径格式 | `/mnt/formula1dltr/raw/` | `vol://f1_raw.formula1_raw_vol/raw/` |
| 上传本地文件到存储 | DBFS / ADLS 工具 | `session.file.put(local_path, vol_path, auto_compress=False)` |

---

## 列操作 ⚠️

这是最常见的改动点。ZettaPark 中 `withColumn` 会触发服务端 schema 预解析，在某些版本下失败。

| 场景 | PySpark | ZettaPark（推荐写法） |
|------|---------|----------------------|
| 添加新列 | `df.withColumn("col", expr)` | `df.select("*", expr.alias("col"))` |
| 修改已有列 | `df.withColumn("col", new_expr)` | `df.select(..., new_expr.alias("col"))` |
| 重命名列 | `df.withColumnRenamed("old", "new")` | `df.select(F.col("old").alias("new"), ...)` |
| 删除列 | `df.drop("col")` | `df.select([c for c in df.columns if c != "col"])` |
| 添加摄取时间 | `df.withColumn("ingestion_date", F.current_timestamp())` | `df.select("*", F.current_timestamp().alias("ingestion_date"))` |

---

## SQL 执行 ⚠️

| 场景 | PySpark | ZettaPark |
|------|---------|-----------|
| 执行查询（返回 DataFrame） | `spark.sql("SELECT ...")` | `session.sql("SELECT ...")` ✅ |
| 执行 DDL/DML（必须触发） | `spark.sql("CREATE TABLE ...")` | `session.sql("CREATE TABLE ...").collect()` ⚠️ |
| MERGE INTO | `spark.sql("MERGE INTO ...").collect()` | `session.sql("MERGE INTO ...").collect()` ✅ |
| 创建临时视图（SQL 方式） | `spark.sql("CREATE TEMP VIEW v AS ...")` | `TEMP VIEW` 不支持；改用 `session.sql("CREATE OR REPLACE VIEW schema.v AS ...").collect()` ⚠️ |
| 创建临时视图（DataFrame 方式） | `df.createOrReplaceTempView("v")` | `df.create_or_replace_temp_view("v")` ✅ 推荐 |
| 删除视图 | `spark.sql("DROP VIEW IF EXISTS v")` | `session.sql("DROP VIEW IF EXISTS schema.v").collect()` — 不能用 `DROP TABLE` ⚠️ |

> **关键规则**：所有 DDL 和 DML（`CREATE`、`INSERT`、`MERGE`、`DROP`）都必须加 `.collect()` 才会真正执行。

---

## JOIN 操作 ⚠️

| 场景 | PySpark | ZettaPark |
|------|---------|-----------|
| 两表 JOIN | `df1.join(df2, df1["id"] == df2["id"])` | 相同 ✅ |
| JOIN 后取列（单表） | `joined.select("col1", "col2")` | 相同 ✅ |
| JOIN 后取列（多表，有同名列） | `joined.select("col1", df2["col2"])` | `joined.select(df1["col1"].alias("col1"), df2["col2"].alias("col2"))` ⚠️ |
| 三表以上 JOIN | 同上 | **每列都必须写 `df["col"].alias("col")`**，否则列名会被加内部前缀 ⚠️ |

---

## 写入数据

| 场景 | PySpark | ZettaPark |
|------|---------|-----------|
| 覆盖写入 | `df.write.mode("overwrite").saveAsTable("db.t")` | `df.write.saveAsTable("schema.t", mode="overwrite")` |
| 追加写入 | `df.write.mode("append").saveAsTable("db.t")` | `df.write.saveAsTable("schema.t", mode="append")` |
| 写入新表（不存在） | `df.write.saveAsTable("db.t")` | 0.1.5 已修复，可直接调用 ✅ |
| 分区写入 | `df.write.partitionBy("col").saveAsTable("db.t")` | 不支持，退化为普通表写入 ⚠️ |
| 增量 MERGE | 自定义 `overwrite_partition()` | 自定义 `merge_delta_data()`（见下方） |

### merge_delta_data 封装

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

---

## Window 函数

语法与 PySpark **完全兼容**，只需替换导入路径：

```python
# PySpark
from pyspark.sql.window import Window
from pyspark.sql import functions as F

window_spec = Window.partitionBy("race_year").orderBy(F.desc("total_points"), F.desc("wins"))
# ❌ 不要用 withColumn
df = df.withColumn("rank", F.rank().over(window_spec))

# ZettaPark
from clickzetta.zettapark.window import Window
from clickzetta.zettapark import functions as F

window_spec = Window.partitionBy("race_year").orderBy(F.desc("total_points"), F.desc("wins"))
# ✅ 用 select
df = df.select(
    "race_year", "driver_name", "total_points", "wins",
    F.rank().over(window_spec).alias("rank"),
)
```

---

## DDL

| 场景 | Spark SQL | Lakehouse SQL |
|------|-----------|---------------|
| 创建 Schema | `CREATE DATABASE IF NOT EXISTS f1_raw` | `CREATE SCHEMA IF NOT EXISTS f1_raw` |
| 删除表 | `DROP TABLE IF EXISTS t` | `DROP TABLE IF EXISTS t` ✅ |
| 查看表结构 | `DESCRIBE TABLE t` | `DESCRIBE TABLE t` ✅ |

---

## 完全兼容（无需修改）

以下 API 与 PySpark 行为一致，迁移时不需要改动：

- `df.select(...)` / `df.filter(...)` / `df.where(...)`
- `df.groupBy(...).agg(...)`
- `df.sort(...)` / `df.orderBy(...)`
- `df.limit(n)` / `df.count()`
- `df.show()` / `df.collect()` / `df.to_pandas()`
- `F.col()` / `F.lit()` / `F.sum()` / `F.count()` / `F.avg()` / `F.max()` / `F.min()`
- `F.rank()` / `F.dense_rank()` / `F.row_number()`
- `F.current_timestamp()` / `F.current_date()`
- `F.concat()` / `F.split()` / `F.trim()` / `F.upper()` / `F.lower()`
- `F.when(...).otherwise(...)`
- `F.isNull()` / `F.isNotNull()`
- `Window.partitionBy(...).orderBy(...)`
- `df.join(other, condition, how)`
- `df.union(other)` / `df.distinct()`
- `df.schema` / `df.columns` / `df.dtypes`
