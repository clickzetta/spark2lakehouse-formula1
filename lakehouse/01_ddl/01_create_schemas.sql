-- ClickZetta Lakehouse DDL
-- 对应原始：create_raw_tables/1.create_raw_tables.sql 中的 CREATE DATABASE

-- 原始 Spark SQL:
--   CREATE DATABASE IF NOT EXISTS f1_raw;
--   CREATE DATABASE IF NOT EXISTS f1_processed;
--   CREATE DATABASE IF NOT EXISTS f1_presentation;
-- 迁移说明：Spark 的 DATABASE 对应 Lakehouse 的 SCHEMA，语法略有不同

CREATE SCHEMA IF NOT EXISTS f1_raw;
CREATE SCHEMA IF NOT EXISTS f1_processed;
CREATE SCHEMA IF NOT EXISTS f1_presentation;
