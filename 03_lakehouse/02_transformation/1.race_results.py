# ZettaPark — 对应原始：02_transformation/1.race_results.py
#
# 迁移说明：
#   spark.read.format('delta').load(path) → session.table("schema.table")
#   withColumnRenamed → rename
#   join + select → 相同语法
#   merge_delta_data → ZettaPark 版本需要传入 session

import sys
sys.path.insert(0, "..")

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from includes.configuration import processed_schema, presentation_schema
from includes.common_functions import merge_delta_data

v_file_date = "2021-03-21"


def produce_race_results(session: Session):
    drivers_df = (
        session.table(f"{processed_schema}.drivers")
        .rename(F.col("number"),      "driver_number")
        .rename(F.col("name"),        "driver_name")
        .rename(F.col("nationality"), "driver_nationality")
    )

    constructors_df = (
        session.table(f"{processed_schema}.constructors")
        .rename(F.col("name"), "team")
    )

    circuits_df = (
        session.table(f"{processed_schema}.circuits")
        .rename(F.col("location"), "circuit_location")
    )

    races_df = (
        session.table(f"{processed_schema}.races")
        .rename(F.col("name"),           "race_name")
        .rename(F.col("race_timestamp"), "race_date")
    )

    results_df = (
        session.table(f"{processed_schema}.results")
        .filter(F.col("file_date") == v_file_date)
        .rename(F.col("time"),    "race_time")
        .rename(F.col("race_id"), "result_race_id")
        .rename(F.col("file_date"), "result_file_date")
    )

    race_circuits_df = races_df.join(
        circuits_df, races_df["circuit_id"] == circuits_df["circuit_id"], "inner"
    ).select(
        races_df["race_id"], races_df["race_year"], races_df["race_name"],
        races_df["race_date"], circuits_df["circuit_location"]
    )

    race_results_df = (
        results_df
        .join(race_circuits_df, results_df["result_race_id"] == race_circuits_df["race_id"])
        .join(drivers_df,       results_df["driver_id"]      == drivers_df["driver_id"])
        .join(constructors_df,  results_df["constructor_id"] == constructors_df["constructor_id"])
    )

    final_df = (
        race_results_df
        .select(
            "race_id", "race_year", "race_name", "race_date", "circuit_location",
            "driver_name", "driver_number", "driver_nationality",
            "team", "grid", "fastest_lap", "race_time", "points", "position",
            "result_file_date",
        )
        .with_column("created_date", F.current_timestamp())
        .rename(F.col("result_file_date"), "file_date")
    )

    merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
    merge_delta_data(session, final_df, presentation_schema, "race_results",
                     merge_condition, "race_id")
    return final_df


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

    produce_race_results(session)
    session.close()
