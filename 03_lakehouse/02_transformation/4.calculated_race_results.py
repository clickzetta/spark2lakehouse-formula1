# ZettaPark — 对应原始：02_transformation/4.calculated_race_results.py
#
# 迁移说明：
#   原始用 spark.sql() 执行 CREATE TABLE + TEMP VIEW + MERGE INTO
#   Lakehouse 不支持 CREATE TEMP VIEW SQL 语法，改为 CREATE OR REPLACE VIEW（普通视图）
#   MERGE INTO 是标准 SQL，ZettaPark 直接支持

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

from clickzetta.zettapark.session import Session
from includes.configuration import processed_schema, presentation_schema

v_file_date = "2021-03-28"


def produce_calculated_race_results(session: Session):
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {presentation_schema}.calculated_race_results (
            race_year        INT,
            team_name        STRING,
            driver_id        INT,
            driver_name      STRING,
            race_id          INT,
            position         INT,
            points           INT,
            calculated_points INT,
            created_date     TIMESTAMP,
            updated_date     TIMESTAMP
        )
    """).collect()

    session.sql(f"""
        CREATE OR REPLACE VIEW {presentation_schema}.race_result_updated AS
        SELECT
            races.race_year,
            constructors.name        AS team_name,
            drivers.driver_id,
            drivers.name             AS driver_name,
            races.race_id,
            results.position,
            results.points,
            11 - results.position    AS calculated_points
        FROM {processed_schema}.results
        JOIN {processed_schema}.drivers      ON results.driver_id      = drivers.driver_ref
        JOIN {processed_schema}.constructors ON results.constructor_id = constructors.constructor_ref
        JOIN {processed_schema}.races        ON results.race_id        = races.race_id
        WHERE results.position <= 10
          AND results.file_date = '{v_file_date}'
    """).collect()

    session.sql(f"""
        MERGE INTO {presentation_schema}.calculated_race_results tgt
        USING {presentation_schema}.race_result_updated upd
        ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.position          = upd.position,
                tgt.points            = upd.points,
                tgt.calculated_points = upd.calculated_points,
                tgt.updated_date      = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT (race_year, team_name, driver_id, driver_name, race_id,
                    position, points, calculated_points, created_date)
            VALUES (upd.race_year, upd.team_name, upd.driver_id, upd.driver_name, upd.race_id,
                    upd.position, upd.points, upd.calculated_points, current_timestamp())
    """).collect()


if __name__ == "__main__":
    from includes.configuration import SCHEMA_NAME
    import os
    from dotenv import load_dotenv
    load_dotenv()

    session = Session.builder.configs({
        "username":  os.environ["CLICKZETTA_USERNAME"],
        "password":  os.environ["CLICKZETTA_PASSWORD"],
        "service":   os.environ["CLICKZETTA_SERVICE"],
        "instance":  os.environ["CLICKZETTA_INSTANCE"],
        "workspace": os.environ["CLICKZETTA_WORKSPACE"],
        "schema":    SCHEMA_NAME,
        "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default_ap"),
    }).create()

    produce_calculated_race_results(session)
    session.close()
