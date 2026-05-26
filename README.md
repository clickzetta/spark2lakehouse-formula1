# spark2lakehouse-formula1

> **Spark SQL → ClickZetta Lakehouse 迁移示例**

本项目 fork 自 [FerhattSimsekk/formula1-data-engineering](https://github.com/FerhattSimsekk/formula1-data-engineering)，在保留原始 Spark/PySpark 代码的基础上，新增了对应的 **ClickZetta Lakehouse SQL 实现**和**迁移说明文档**，帮助数据工程师将 Spark 数据管道迁移到云器 Lakehouse。

## 项目结构

```
├── spark/                      # 📦 原始 Spark/PySpark 代码（只读参考）
│   ├── 01_ingestion/           #   数据摄取：spark.read.csv/json() → Delta
│   ├── 02_transformation/      #   数据转换：DataFrame join + 聚合
│   ├── 03_analysis/            #   数据分析：Spark SQL 查询
│   ├── 04_create_raw_tables/   #   建表 DDL（Databricks 格式）
│   ├── 05_includes/            #   公共函数和配置
│   └── 06_utils/               #   工具脚本
│
├── lakehouse/                  # ✅ 迁移后的 ClickZetta Lakehouse SQL
│   ├── 01_ddl/                 #   建表语句（External Table + Schema）
│   ├── 02_transformation/      #   数据转换（INSERT OVERWRITE + Window 函数）
│   └── 03_analysis/            #   数据分析（与 Spark SQL 基本兼容）
│
├── migration/              # 📖 迁移说明文档
│   ├── 01_overview.md          迁移策略与关键差异
│   └── 02_syntax_mapping.md    Spark SQL → Lakehouse SQL 语法对照
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

1. 阅读 [迁移概述](migration/01_overview.md) 了解整体迁移思路
2. 参考 [语法对照表](migration/02_syntax_mapping.md) 了解 Spark SQL 与 Lakehouse SQL 的差异
3. 按照 [逐步迁移指南](migration/04_step_by_step.md) 执行迁移

## 原始项目

- 原始作者：[FerhattSimsekk](https://github.com/FerhattSimsekk)
- 原始 Repo：[formula1-data-engineering](https://github.com/FerhattSimsekk/formula1-data-engineering)
- 技术栈：Azure Databricks、PySpark、Spark SQL、Delta Lake、Azure Data Factory
