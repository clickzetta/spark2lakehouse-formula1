-- ClickZetta Lakehouse SQL
-- 对应原始：trans/1.race_results.py
--
-- 迁移说明：
--   原始使用 PySpark DataFrame API 进行多表 join，最后写入 Delta 分区表
--   Lakehouse 用纯 SQL 表达相同逻辑，INSERT OVERWRITE 替代 overwrite_partition()
--   current_timestamp() 函数完全兼容

-- 创建目标表（首次执行）
CREATE TABLE IF NOT EXISTS f1_presentation.race_results (
    race_id             INT,
    race_year           INT,
    race_name           STRING,
    race_date           DATE,
    circuit_location    STRING,
    driver_name         STRING,
    driver_number       INT,
    driver_nationality  STRING,
    team                STRING,
    grid                INT,
    fastest_lap         INT,
    race_time           STRING,
    points              FLOAT,
    position            INT,
    created_date        TIMESTAMP
)
PARTITION BY (race_id);

-- 增量写入（按 race_id 分区覆盖）
-- 原始：overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')
INSERT OVERWRITE f1_presentation.race_results
PARTITION (race_id)
SELECT
    re.raceId                               AS race_id,
    ra.year                                 AS race_year,
    ra.name                                 AS race_name,
    ra.date                                 AS race_date,
    ci.location                             AS circuit_location,
    CONCAT(dr.forename, ' ', dr.surname)    AS driver_name,
    dr.number                               AS driver_number,
    dr.nationality                          AS driver_nationality,
    co.name                                 AS team,
    re.grid,
    re.fastestLap                           AS fastest_lap,
    re.time                                 AS race_time,
    re.points,
    re.position,
    CURRENT_TIMESTAMP()                     AS created_date
FROM f1_processed.results   re
JOIN f1_processed.races     ra ON re.raceId      = ra.raceId
JOIN f1_processed.circuits  ci ON ra.circuitId   = ci.circuitId
JOIN f1_processed.drivers   dr ON re.driverId    = dr.driverId
JOIN f1_processed.constructors co ON re.constructorId = co.constructorId
WHERE re.file_date = '${bizdate}';   -- 替换原始 v_file_date 参数
