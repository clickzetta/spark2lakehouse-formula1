-- ClickZetta Lakehouse
-- 对应原始：01_ingestion/9.create_processed_database.sql
--
-- 迁移说明：Spark 的 CREATE DATABASE 对应 Lakehouse 的 CREATE SCHEMA

CREATE SCHEMA IF NOT EXISTS f1_processed;
