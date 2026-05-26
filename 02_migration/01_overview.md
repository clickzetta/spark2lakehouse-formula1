# 迁移概述

## 为什么迁移？

ClickZetta Lakehouse 是一个云原生的存算分离数仓，兼容标准 SQL，无需维护 Spark 集群，按需计费。对于以 Spark SQL 为主的数据管道，迁移成本低，收益明显：

- **无需 PySpark 代码**：绝大多数转换逻辑可以用纯 SQL 表达
- **更低运维成本**：无需管理 Spark 集群、Driver/Executor 配置
- **更好的并发**：分析型计算集群支持多实例横向弹缩
- **存储格式兼容**：支持读取 Delta Lake、Parquet、JSON、CSV 等格式

## 迁移策略

本项目采用**逐层迁移**策略，与原始 Medallion 架构保持一致：

```
原始层（f1_raw）→ 处理层（f1_processed）→ 展示层（f1_presentation）
```

每一层的迁移工作量：

| 层次 | 原始实现 | 迁移工作量 | 说明 |
|------|---------|-----------|------|
| 原始层 DDL | `CREATE TABLE ... USING csv OPTIONS(path)` | 低 | 语法差异小，主要是 OPTIONS 写法 |
| 数据摄取 | PySpark DataFrame API | 中 | 改为 `CREATE EXTERNAL TABLE` 直接映射源文件，摄取逻辑转为 SQL 查询 |
| 数据转换 | PySpark + Spark SQL | 低 | Window 函数、JOIN 语法完全兼容 |
| 数据分析 | 纯 Spark SQL | 极低 | 几乎零改动 |

## 关键差异

1. **数据库 → Schema**：Spark 的 `DATABASE` 对应 Lakehouse 的 `SCHEMA`
2. **Delta Lake 路径读写 → 表名读写**：`spark.read.format('delta').load(path)` → `SELECT * FROM table`
3. **PySpark DataFrame API → SQL**：`withColumnRenamed`、`filter`、`join` 等操作用 SQL 表达
4. **挂载路径 → External Volume**：`/mnt/formula1dltr/` → External Volume 路径
5. **`current_timestamp()` 函数**：两者完全兼容
