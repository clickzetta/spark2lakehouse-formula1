-- ClickZetta Lakehouse SQL
-- 对应原始：trans/2.driver_standings.py
--
-- 迁移说明：
--   Window 函数语法完全兼容，RANK() OVER (PARTITION BY ... ORDER BY ...) 无需修改
--   PySpark 的 Window.partitionBy().orderBy() + rank().over() 直接翻译为标准 SQL

CREATE TABLE IF NOT EXISTS f1_presentation.driver_standings (
    race_year           INT,
    driver_name         STRING,
    driver_nationality  STRING,
    team                STRING,
    total_points        FLOAT,
    wins                INT,
    rank                INT
)
PARTITION BY (race_year);

INSERT OVERWRITE f1_presentation.driver_standings
PARTITION (race_year)
SELECT
    race_year,
    driver_name,
    driver_nationality,
    team,
    total_points,
    wins,
    RANK() OVER (
        PARTITION BY race_year
        ORDER BY total_points DESC, wins DESC
    ) AS rank
FROM (
    SELECT
        race_year,
        driver_name,
        driver_nationality,
        team,
        SUM(points)                         AS total_points,
        SUM(CASE WHEN position = 1 THEN 1 ELSE 0 END) AS wins
    FROM f1_presentation.race_results
    WHERE race_year IN (${race_year_list})  -- 替换原始 race_year_list 参数
    GROUP BY race_year, driver_name, driver_nationality, team
) standings;
