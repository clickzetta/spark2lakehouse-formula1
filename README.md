# spark2lakehouse-formula1

> **Spark SQL → ClickZetta Lakehouse 迁移示例**

本项目 fork 自 [FerhattSimsekk/formula1-data-engineering](https://github.com/FerhattSimsekk/formula1-data-engineering)，在保留原始 Spark/PySpark 代码的基础上，新增了对应的 **ClickZetta Lakehouse SQL 实现**和**迁移说明文档**，帮助数据工程师将 Spark 数据管道迁移到云器 Lakehouse。

## 迁移兼容性结论

原始代码以 Spark SQL 为主，少量 PySpark DataFrame API，迁移目标是 **ClickZetta Lakehouse SQL**。

| 迁移项 | 原始实现 | Lakehouse 实现 | 兼容性 |
|--------|---------|---------------|--------|
| 创建 Schema | `CREATE DATABASE IF NOT EXISTS f1_raw` | `CREATE SCHEMA IF NOT EXISTS f1_raw` | ✅ 仅关键字不同 |
| 外部表（CSV/JSON） | `CREATE TABLE t USING csv OPTIONS(path "/mnt/...")` | `CREATE EXTERNAL TABLE t (...) STORED AS CSV LOCATION '...'` | ✅ 语法略有调整 |
| 数据转换 | PySpark DataFrame API（join、withColumn 等） | 纯 SQL（JOIN、SELECT ... AS ...） | ✅ 逻辑完全等价 |
| Window 函数 | `RANK() OVER (PARTITION BY ... ORDER BY ...)` | 完全相同 | ✅ 零改动 |
| 聚合分析 | 纯 Spark SQL | 纯 Lakehouse SQL | ✅ 几乎零改动 |
| 分区写入 | `df.write.partitionBy("race_id").saveAsTable(t)` | `INSERT OVERWRITE t PARTITION (race_id) SELECT ...` | ✅ 语义等价 |
| 时间戳函数 | `current_timestamp()` | `CURRENT_TIMESTAMP()` | ✅ 完全兼容 |
| 挂载路径 | `/mnt/formula1dltr/` | External Volume 路径 | ⚠️ 路径格式不同，setup.py 自动替换 |

**结论：本项目涉及的 Spark SQL 语法全部可迁移到 ClickZetta Lakehouse，分析层几乎零改动，转换层用纯 SQL 替代 PySpark DataFrame API，DDL 层语法差异小。唯一需要适配的是存储路径（DBFS 挂载点 → External Volume），由 setup.py 自动处理。**

## 项目结构

```
├── 01_spark/                   # 📦 原始 Spark/PySpark 代码（只读参考）
│   ├── 01_ingestion/           #   数据摄取：spark.read.csv/json() → Delta
│   ├── 02_transformation/      #   数据转换：DataFrame join + 聚合
│   ├── 03_analysis/            #   数据分析：Spark SQL 查询
│   ├── 04_create_raw_tables/   #   建表 DDL（Databricks 格式）
│   ├── 05_includes/            #   公共函数和配置
│   └── 06_utils/               #   工具脚本
│
├── 02_migration/               # 📖 迁移说明文档
│   ├── 01_overview.md              迁移策略与关键差异
│   └── 02_syntax_mapping.md        Spark SQL → Lakehouse SQL 语法对照
│
├── 03_lakehouse/               # ✅ 迁移后的 ClickZetta Lakehouse SQL
│   ├── 01_ddl/                 #   建表语句（External Table + Schema）
│   ├── 02_transformation/      #   数据转换（INSERT OVERWRITE + Window 函数）
│   └── 03_analysis/            #   数据分析（与 Spark SQL 基本兼容）
│
├── datasets/raw/           # 原始数据（由 setup.py 从 Ergast API 下载）
├── setup.py                # 🚀 一键初始化（下载数据、创建 Volume、执行 SQL）
└── .env.sample             # 连接配置模板
```

## 数据架构

| 层次 | Spark（原始） | Lakehouse（迁移后） | 说明 |
|------|-------------|-------------------|------|
| 原始层 | `f1_raw` database | `f1_raw` schema | 外部表，直接映射源文件 |
| 处理层 | `f1_processed` database | `f1_processed` schema | 清洗、标准化后的数据 |
| 展示层 | `f1_presentation` database | `f1_presentation` schema | 聚合分析结果 |

## 快速开始

1. 安装依赖：`pip install clickzetta python-dotenv requests`
2. 复制配置：`cp .env.sample .env`，填写 ClickZetta 连接信息
3. 一键初始化：`python setup.py`（下载数据集、创建 Schema/Volume、上传文件、执行 DDL）
4. 按顺序运行 `03_lakehouse/` 下的 SQL：
   - `01_ddl/` — 建表（Schema + External Table）
   - `02_transformation/` — 数据转换（INSERT OVERWRITE）
   - `03_analysis/` — 聚合分析查询

阅读 [迁移概述](02_migration/01_overview.md) 和 [语法对照表](02_migration/02_syntax_mapping.md) 了解迁移细节。

## 原始项目

- 原始作者：[FerhattSimsekk](https://github.com/FerhattSimsekk)
- 原始 Repo：[formula1-data-engineering](https://github.com/FerhattSimsekk/formula1-data-engineering)
- 技术栈：Azure Databricks、PySpark、Spark SQL、Delta Lake、Azure Data Factory
