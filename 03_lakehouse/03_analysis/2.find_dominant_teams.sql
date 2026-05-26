-- ClickZetta Lakehouse
-- 对应原始：03_analysis/2.find_dominant_teams.sql
-- 语法与 Spark SQL 完全兼容，零改动

SELECT team_name,
       COUNT(*)                  AS race_counts,
       SUM(calculated_position)  AS total_points,
       AVG(points)               AS avg_points,
       COUNT(1)                  AS total_races
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(*) >= 100
ORDER BY avg_points DESC;

-- 2011-2020 赛季
SELECT team_name,
       COUNT(*)                  AS race_counts,
       SUM(calculated_position)  AS total_points,
       AVG(points)               AS avg_points,
       COUNT(1)                  AS total_races
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING COUNT(*) >= 100
ORDER BY avg_points DESC;

-- 2001-2010 赛季
SELECT team_name,
       COUNT(*)                  AS race_counts,
       SUM(calculated_position)  AS total_points,
       AVG(points)               AS avg_points,
       COUNT(1)                  AS total_races
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY team_name
HAVING COUNT(*) >= 100
ORDER BY avg_points DESC;
