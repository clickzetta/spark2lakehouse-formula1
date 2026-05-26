-- ClickZetta Lakehouse SQL
-- 对应原始：analysis/1.find_dominant_drivers.sql
--
-- 迁移说明：此文件几乎无需修改，Lakehouse SQL 与 Spark SQL 语法完全兼容
-- 唯一差异：去掉 Databricks notebook 的 "-- COMMAND ----------" 分隔符注释

-- 全时段最强车手
SELECT
    driver_name,
    COUNT(*)                            AS race_counts,
    SUM(calculated_position)           AS total_points,
    ROUND(AVG(calculated_position), 2) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(*) >= 50
ORDER BY avg_points DESC;

-- 2011-2020 赛季
SELECT
    driver_name,
    COUNT(*)                            AS race_counts,
    SUM(calculated_position)           AS total_points,
    ROUND(AVG(calculated_position), 2) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING COUNT(*) >= 50
ORDER BY avg_points DESC;

-- 2001-2010 赛季
SELECT
    driver_name,
    COUNT(*)                            AS race_counts,
    SUM(calculated_position)           AS total_points,
    ROUND(AVG(calculated_position), 2) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING COUNT(*) >= 50
ORDER BY avg_points DESC;
