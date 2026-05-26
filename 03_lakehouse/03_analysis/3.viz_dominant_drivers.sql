-- ClickZetta Lakehouse
-- 对应原始：03_analysis/3.viz_dominant_drivers.sql
-- 语法与 Spark SQL 完全兼容，零改动

CREATE OR REPLACE TEMP VIEW v_dominant_drivers AS
SELECT
    driver_name,
    COUNT(*)                                                    AS race_counts,
    SUM(calculated_position)                                    AS total_points,
    ROUND(AVG(calculated_position), 2)                         AS avg_points,
    RANK() OVER (ORDER BY AVG(calculated_position) DESC)       AS driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(*) >= 50
ORDER BY avg_points DESC;

-- 按赛季展示 Top 10 车手趋势（将 v_dominant_drivers 展开为子查询，避免跨语句 TEMP VIEW 失效）
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
