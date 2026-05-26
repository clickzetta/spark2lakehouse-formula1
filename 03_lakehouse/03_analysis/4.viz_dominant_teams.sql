-- ClickZetta Lakehouse
-- 对应原始：03_analysis/4.viz_dominant_teams.sql
-- 语法与 Spark SQL 完全兼容，零改动

CREATE OR REPLACE TEMP VIEW v_dominant_teams AS
SELECT
    team_name,
    COUNT(1)                                                   AS total_races,
    SUM(calculated_position)                                   AS total_points,
    AVG(calculated_position)                                   AS avg_points,
    RANK() OVER (ORDER BY AVG(calculated_position) DESC)       AS team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC;

-- 按赛季展示 Top 5 车队趋势（将 v_dominant_teams 展开为子查询，避免跨语句 TEMP VIEW 失效）
SELECT
    race_year,
    team_name,
    COUNT(1)                 AS total_races,
    SUM(calculated_position) AS total_points,
    AVG(calculated_position) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE team_name IN (
    SELECT team_name FROM (
        SELECT
            team_name,
            RANK() OVER (ORDER BY AVG(calculated_position) DESC) AS team_rank
        FROM f1_presentation.calculated_race_results
        GROUP BY team_name
        HAVING COUNT(1) >= 100
    ) t WHERE team_rank <= 5
)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC;
