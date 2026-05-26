-- ClickZetta Lakehouse DDL
-- 对应原始：create_raw_tables/1.create_raw_tables.sql
--
-- 迁移说明：
--   原始使用 CREATE TABLE ... USING csv OPTIONS(path "/mnt/...")
--   Lakehouse Named Volume 不支持 STORED AS / LOCATION 语法
--   改为：CREATE TABLE（建空表）+ COPY INTO（从 Named Volume 导入）

-- 切换到 mcp_demo schema，使 COPY INTO ... FROM VOLUME 能解析到正确的 volume
USE SCHEMA mcp_demo;

-- circuits（CSV）
DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits (
    circuitId   INT,
    circuitRef  STRING,
    name        STRING,
    location    STRING,
    country     STRING,
    lat         DOUBLE,
    lng         DOUBLE,
    alt         INT,
    url         STRING
);
COPY INTO f1_raw.circuits
FROM VOLUME formula1_vol
USING CSV
OPTIONS ('header' = 'true')
FILES ('circuits.csv');

-- races（CSV）
DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races (
    raceId      INT,
    year        INT,
    round       INT,
    circuitId   INT,
    name        STRING,
    date        STRING,
    time        STRING,
    url         STRING
);
COPY INTO f1_raw.races
FROM VOLUME formula1_vol
USING CSV
OPTIONS ('header' = 'true')
FILES ('races.csv');

-- constructors（JSON 数组）
DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors (
    constructorId   INT,
    constructorRef  STRING,
    name            STRING,
    nationality     STRING,
    url             STRING
);
COPY INTO f1_raw.constructors
FROM VOLUME formula1_vol
USING JSON
FILES ('constructors.json');

-- drivers（JSON 数组）
DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers (
    driverId        INT,
    driverRef       STRING,
    number          INT,
    code            STRING,
    forename        STRING,
    surname         STRING,
    dob             STRING,
    nationality     STRING,
    url             STRING
);
COPY INTO f1_raw.drivers
FROM VOLUME formula1_vol
USING JSON
FILES ('drivers.json');

-- results（JSON 数组）
DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results (
    resultId        INT,
    raceId          INT,
    driverId        STRING,
    constructorId   STRING,
    number          INT,
    grid            INT,
    position        INT,
    positionText    STRING,
    positionOrder   INT,
    points          DOUBLE,
    laps            INT,
    time            STRING,
    milliseconds    INT,
    fastestLap      INT,
    rank            INT,
    fastestLapTime  STRING,
    fastestLapSpeed DOUBLE,
    statusId        STRING
);
COPY INTO f1_raw.results
FROM VOLUME formula1_vol
USING JSON
FILES ('results.json');

-- pit_stops（JSON 数组）
DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (
    raceId          INT,
    driverId        STRING,
    stop            INT,
    lap             INT,
    time            STRING,
    duration        STRING,
    milliseconds    INT
);
COPY INTO f1_raw.pit_stops
FROM VOLUME formula1_vol
USING JSON
FILES ('pit_stops.json');
