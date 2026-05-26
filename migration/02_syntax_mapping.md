# Spark SQL → Lakehouse SQL 语法对照

## DDL

| 场景 | Spark SQL | Lakehouse SQL |
|------|-----------|---------------|
| 创建数据库/Schema | `CREATE DATABASE IF NOT EXISTS f1_raw` | `CREATE SCHEMA IF NOT EXISTS f1_raw` |
| 外部表（CSV） | `CREATE TABLE t USING csv OPTIONS(path "...", header true)` | `CREATE EXTERNAL TABLE t (...) STORED AS CSV LOCATION '...'` |
| 外部表（JSON） | `CREATE TABLE t USING json OPTIONS(path "...")` | `CREATE EXTERNAL TABLE t (...) STORED AS JSON LOCATION '...'` |
| 删除表 | `DROP TABLE IF EXISTS t` | `DROP TABLE IF EXISTS t` ✅ 完全兼容 |
| 分区表 | `df.write.partitionBy("race_id").saveAsTable(t)` | `CREATE TABLE t (...) PARTITION BY (race_id)` |

## 数据读写

| 场景 | Spark（PySpark） | Lakehouse SQL |
|------|----------------|---------------|
| 读 Delta 表 | `spark.read.format('delta').load(path)` | `SELECT * FROM schema.table` |
| 写入新表 | `df.write.format('delta').saveAsTable('db.t')` | `CREATE TABLE db.t AS SELECT ...` |
| 追加写入 | `df.write.mode('append').saveAsTable('db.t')` | `INSERT INTO db.t SELECT ...` |
| 覆盖写入 | `df.write.mode('overwrite').saveAsTable('db.t')` | `INSERT OVERWRITE db.t SELECT ...` |
| 分区覆盖 | `overwrite_partition(df, 'db', 't', 'race_id')` | `INSERT OVERWRITE db.t PARTITION (race_id=x) SELECT ...` |
| 增量加载（按日期过滤） | `.filter(f"file_date = '{v_file_date}'")` | `WHERE file_date = '${bizdate}'` |

## 数据转换

| 场景 | Spark（PySpark） | Lakehouse SQL |
|------|----------------|---------------|
| 列重命名 | `.withColumnRenamed("old", "new")` | `SELECT old AS new FROM ...` |
| 添加列 | `.withColumn("col", expr(...))` | `SELECT *, expr AS col FROM ...` |
| 添加摄取时间 | `.withColumn("ingestion_date", current_timestamp())` | `SELECT *, CURRENT_TIMESTAMP() AS ingestion_date FROM ...` |
| 过滤 | `.filter(col("year").isin(year_list))` | `WHERE year IN (2020, 2021)` |
| 多表 JOIN | `df1.join(df2, df1.id == df2.id, "inner")` | `SELECT ... FROM t1 JOIN t2 ON t1.id = t2.id` ✅ |
| 临时视图 | `df.createOrReplaceTempView("v")` | `CREATE OR REPLACE VIEW v AS SELECT ...` |

## Window 函数

Window 函数语法**完全兼容**，几乎无需修改：

```sql
-- Spark SQL（原始）
Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
df.withColumn("rank", rank().over(driver_rank_spec))

-- Lakehouse SQL（等价）
RANK() OVER (PARTITION BY race_year ORDER BY total_points DESC, wins DESC) AS rank
```

## 聚合分析

分析 SQL 语法**完全兼容**，无需修改：

```sql
-- 原始 Spark SQL（analysis/1.find_dominant_drivers.sql）
SELECT driver_name,
       count(*) as race_counts,
       sum(calculated_position) as total_points,
       round(avg(calculated_position), 2) as avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(*) >= 50
ORDER BY avg_points DESC

-- Lakehouse SQL — 完全相同，无需修改 ✅
```
