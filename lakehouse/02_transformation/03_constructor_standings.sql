-- ClickZetta Lakehouse SQL
-- 对应原始：trans/3.constructor_standings.py
--
-- 迁移说明：与 driver_standings 迁移方式相同，Window 函数完全兼容

CREATE TABLE IF NOT EXISTS f1_presentation.constructor_standings (
    race_year       INT,
    team            STRING,
    total_points    FLOAT,
    wins            INT,
    rank            INT
)
PARTITION BY (race_year);

INSERT OVERWRITE f1_presentation.constructor_standings
PARTITION (race_year)
SELECT
    race_year,
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
        team,
        SUM(points)                         AS total_points,
        SUM(CASE WHEN position = 1 THEN 1 ELSE 0 END) AS wins
    FROM f1_presentation.race_results
    WHERE race_year IN (${race_year_list})
    GROUP BY race_year, team
) standings;
