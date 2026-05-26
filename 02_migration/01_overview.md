# 迁移概述

## 迁移目标

将基于 Azure Databricks + PySpark 的 F1 数据工程项目迁移到 ClickZetta Lakehouse，保留原有三层架构（Raw → Processed → Presentation），使用 ZettaPark Python DataFrame API 替代 PySpark。

迁移后的代码在 `03_lakehouse/` 目录，可对照 `01_spark/` 目录逐文件比较。

---

## 技术路径选择

迁移有两条路径，本项目选择 **ZettaPark Python API**：

| 路径 | 适用场景 | 本项目选择 |
|------|---------|-----------|
| **纯 SQL 迁移**：将 PySpark 逻辑改写为 Lakehouse SQL | 转换层以 SQL 为主、逻辑简单 | 部分使用（MERGE INTO） |
| **ZettaPark Python API**：用类 PySpark 的 Python SDK | 已有大量 DataFrame 代码，想最小化改动 | ✅ 主要路径 |

ZettaPark 的 API 与 PySpark 高度相似，大多数 `df.select()`、`df.join()`、`df.filter()`、`F.rank().over(window)` 可以直接复用，改动集中在几个已知差异点（详见 [03_zettapark_pitfalls.md](03_zettapark_pitfalls.md)）。

---

## 架构映射

| 原始（Databricks） | 迁移后（ClickZetta Lakehouse） |
|-------------------|-------------------------------|
| Azure Data Lake Storage（ADLS）挂载路径 `/mnt/formula1dltr/` | External Volume `vol://f1_raw.formula1_raw_vol/` |
| Databricks `spark` 全局对象 | ZettaPark `session`（显式创建） |
| `DATABASE` | `SCHEMA` |
| Delta Lake 表（托管存储） | Lakehouse 内部表（等价） |
| `spark.read.csv("/mnt/...")` | `session.read.csv("vol://schema.vol/...")` |
| `df.write.saveAsTable("db.t")` | `df.write.saveAsTable("schema.t", mode=...)` |
| `spark.sql("...")` | `session.sql("...").collect()`（必须加 `.collect()`） |

---

## 各层迁移工作量

### 摄取层（01_ingestion）

工作量：**中**

主要改动：
1. 数据源路径：ADLS 挂载路径 → Volume 路径（`vol://`）
2. 读取方式：`spark.read.csv(path)` → `session.read.schema(schema).csv(path)`，需显式定义 schema
3. `withColumn` 替换：所有 `df.withColumn(name, expr)` 改为 `df.select(..., expr.alias(name))`
4. `saveAsTable` 调用：第一次运行时表不存在会失败，需用 `merge_delta_data()` 封装
5. 数据格式：lap_times 和 qualifying 来自 Jolpica API，格式为 NDJSON，与原始 CSV 不同

### 转换层（02_transformation）

工作量：**低**

主要改动：
1. `withColumn` 替换（与摄取层一致）
2. 多表 JOIN 后的 `select()` 必须对每列显式指定来源 DataFrame 和 `.alias()`
3. `session.sql("MERGE INTO ...").collect()` — 必须加 `.collect()`
4. `createOrReplaceTempView` → ZettaPark 的同名方法（接口一致，无需改动）

Window 函数、聚合、过滤、排序的语法与 PySpark **完全一致**。

### 分析层

工作量：**极低**

原始 Spark SQL 查询文件几乎可以原样在 `session.sql()` 中执行，只需加 `.collect()` 或 `.show()`。

---

## 核心差异汇总

详细的踩坑记录和解决方案见专项文档：

- **[03_zettapark_pitfalls.md](03_zettapark_pitfalls.md)**：ZettaPark API 层面的 7 个已知坑（`withColumn`、惰性执行、JOIN 列名前缀等）
- **[04_data_compatibility.md](04_data_compatibility.md)**：数据源层面的 7 个差异（Jolpica API vs 原始 Ergast CSV，字符串 ref vs 整数 ID 等）

快速参考：

| 差异点 | 原始（PySpark） | ZettaPark |
|--------|----------------|-----------|
| 添加/修改列 | `df.withColumn(name, expr)` | `df.select(..., expr.alias(name))` ⚠️ |
| SQL 执行 | `spark.sql(query)` | `session.sql(query).collect()` ⚠️ |
| JOIN 后取列 | `select("col")` | `select(df["col"].alias("col"))` ⚠️ |
| 文件路径 | `/mnt/formula1dltr/raw/` | `vol://f1_raw.formula1_raw_vol/` |
| 导入 Window | `from pyspark.sql.window import Window` | `from clickzetta.zettapark.window import Window` |
| 导入 functions | `from pyspark.sql import functions as F` | `from clickzetta.zettapark import functions as F` |
| 写入已有表 | `df.write.mode("overwrite").saveAsTable(t)` | `df.write.saveAsTable(t, mode="overwrite")` |
| 写入新表 | `df.write.saveAsTable(t)` | 需用 `merge_delta_data()` 封装 ⚠️ |
