-- ClickZetta Lakehouse DDL
-- 对应原始：create_raw_tables/1.create_raw_tables.sql
--
-- 迁移说明：
--   原始使用 CREATE TABLE ... USING csv OPTIONS(path "/mnt/...")
--   Lakehouse 使用 CREATE EXTERNAL TABLE ... STORED AS CSV LOCATION '...'
--   路径从 DBFS 挂载点改为 External Volume 路径
--   STRUCT 类型（如 drivers.name）改为展开的列或 MAP 类型

-- 替换 <your_volume_path> 为实际的 External Volume 路径，例如：
--   /Volumes/main/f1_raw/raw_data/

-- circuits 表
DROP TABLE IF EXISTS f1_raw.circuits;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.circuits (
    circuitId   INT,
    circuitRef  STRING,
    name        STRING,
    location    STRING,
    country     STRING,
    lat         DOUBLE,
    lng         DOUBLE,
    alt         INT,
    url         STRING
)
STORED AS CSV
LOCATION '<your_volume_path>/circuits.csv'
TBLPROPERTIES ('skip.header.line.count'='1');

-- races 表
DROP TABLE IF EXISTS f1_raw.races;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.races (
    raceId      INT,
    year        INT,
    round       INT,
    circuitId   INT,
    name        STRING,
    date        DATE,
    time        STRING,
    url         STRING
)
STORED AS CSV
LOCATION '<your_volume_path>/races.csv'
TBLPROPERTIES ('skip.header.line.count'='1');

-- constructors 表
DROP TABLE IF EXISTS f1_raw.constructors;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.constructors (
    constructorId   INT,
    constructorRef  STRING,
    name            STRING,
    nationality     STRING,
    url             STRING
)
STORED AS JSON
LOCATION '<your_volume_path>/constructors.json';

-- drivers 表
-- 迁移说明：原始 STRUCT<forename: STRING, surname: STRING> 展开为两列
DROP TABLE IF EXISTS f1_raw.drivers;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.drivers (
    driverId        INT,
    driverRef       STRING,
    number          INT,
    code            STRING,
    forename        STRING,
    surname         STRING,
    dob             DATE,
    nationality     STRING,
    url             STRING
)
STORED AS JSON
LOCATION '<your_volume_path>/drivers.json';

-- results 表
DROP TABLE IF EXISTS f1_raw.results;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.results (
    resultId        INT,
    raceId          INT,
    driverId        INT,
    constructorId   INT,
    number          INT,
    grid            INT,
    position        INT,
    positionText    STRING,
    positionOrder   INT,
    points          INT,
    laps            INT,
    time            STRING,
    milliseconds    INT,
    fastestLap      INT,
    rank            INT,
    fastestLapTime  STRING,
    fastestLapSpeed FLOAT,
    statusId        STRING
)
STORED AS JSON
LOCATION '<your_volume_path>/results.json';

-- pit_stops 表
DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.pit_stops (
    driverId        INT,
    duration        STRING,
    lap             INT,
    milliseconds    INT,
    raceId          INT,
    stop            INT,
    time            STRING
)
STORED AS JSON
LOCATION '<your_volume_path>/pit_stops.json';
