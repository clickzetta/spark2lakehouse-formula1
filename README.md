# spark2lakehouse-formula1

> **Spark SQL → ClickZetta Lakehouse 迁移示例**

本项目 fork 自 [FerhattSimsekk/formula1-data-engineering](https://github.com/FerhattSimsekk/formula1-data-engineering)，在保留原始 Spark/PySpark 代码的基础上，新增了对应的 **ClickZetta Lakehouse SQL 实现**和**迁移说明文档**，帮助数据工程师将 Spark 数据管道迁移到云器 Lakehouse。

## 项目结构

```
├── ingestion/          # 原始 Spark 代码：数据摄取（PySpark）
├── trans/              # 原始 Spark 代码：数据转换（PySpark + Spark SQL）
├── analysis/           # 原始 Spark 代码：数据分析（Spark SQL）
├── create_raw_tables/  # 原始 Spark 代码：建表 DDL
├── includes/           # 原始 Spark 代码：公共函数和配置
├── lakehouse/          # ✅ Lakehouse 实现（ClickZetta SQL）
│   ├── ddl/            #    建表语句
│   ├── ingestion/      #    数据摄取
│   ├── transformation/ #    数据转换
│   └── analysis/       #    数据分析
└── migration/          # ✅ 迁移说明文档
    ├── 01_overview.md
    ├── 02_syntax_mapping.md
    ├── 03_data_types.md
    └── 04_step_by_step.md
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
