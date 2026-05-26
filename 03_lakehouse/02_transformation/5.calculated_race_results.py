# ZettaPark — 对应原始：02_transformation/5.calculated_race_results.py
#
# 迁移说明：
#   原始是 CTAS（CREATE TABLE AS SELECT），用 session.sql() 直接执行
#   这是 4.calculated_race_results.py 的简化版（全量覆盖，无增量 MERGE）

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

from clickzetta.zettapark.session import Session
from includes.configuration import processed_schema, presentation_schema


def produce_calculated_race_results_full(session: Session):
    session.sql(f"""
        CREATE OR REPLACE TABLE {presentation_schema}.calculated_race_results AS
        SELECT
            races.race_year,
            constructors.name        AS team_name,
            drivers.name             AS driver_name,
            results.position,
            results.points,
            11 - results.position    AS calculated_position
        FROM {processed_schema}.results
        JOIN {processed_schema}.drivers      ON results.driver_id      = drivers.driver_ref
        JOIN {processed_schema}.constructors ON results.constructor_id = constructors.constructor_ref
        JOIN {processed_schema}.races        ON results.race_id        = races.race_id
        WHERE results.position <= 10
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

    produce_calculated_race_results_full(session)
    session.close()
