-- ClickZetta Lakehouse
-- 对应原始：03_analysis/3.viz_dominant_drivers.sql
--
-- 迁移说明：Lakehouse 不支持 CREATE TEMP VIEW SQL 语法，
-- 将 SELECT ... FROM v_dominant_drivers 改写为内联子查询，逻辑等价

-- 按赛季展示 Top 10 车手趋势
SELECT
    race_year,
    driver_name,
    COUNT(1)                 AS race_counts,
    SUM(calculated_position) AS total_points,
    AVG(calculated_position) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (
    SELECT driver_name FROM (
        SELECT
            driver_name,
            RANK() OVER (ORDER BY AVG(calculated_position) DESC) AS driver_rank
        FROM f1_presentation.calculated_race_results
        GROUP BY driver_name
        HAVING COUNT(*) >= 50
    ) t WHERE driver_rank <= 10
)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC;
