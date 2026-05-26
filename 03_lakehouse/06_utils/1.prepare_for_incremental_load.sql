-- ClickZetta Lakehouse — 对应原始：06_utils/1.prepare_for_incremental_load.sql
--
-- 迁移说明：
--   原始使用 DROP DATABASE ... CASCADE + CREATE DATABASE ... LOCATION "/mnt/..."
--   Lakehouse 使用 DROP SCHEMA ... CASCADE + CREATE SCHEMA（无 LOCATION，存储由平台管理）

DROP SCHEMA IF EXISTS f1_processed CASCADE;
CREATE SCHEMA IF NOT EXISTS f1_processed;

DROP SCHEMA IF EXISTS f1_presentation CASCADE;
CREATE SCHEMA IF NOT EXISTS f1_presentation;
