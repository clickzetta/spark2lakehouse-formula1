# ZettaPark — 对应原始：02_transformation/1.race_results.py
#
# 迁移说明：
#   spark.read.format('delta').load(path) → session.table("schema.table")
#   withColumnRenamed / join / select → 直接使用，ZettaPark 兼容 PySpark 同名方法
#   merge_delta_data → 签名与原始相同，内部通过 input_df.session 获取 session

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from includes.configuration import processed_schema, presentation_schema
from includes.common_functions import merge_delta_data

v_file_date = "2021-03-28"


def produce_race_results(session: Session):
    drivers_df   = session.table(f"{processed_schema}.drivers")
    constructors_df = session.table(f"{processed_schema}.constructors")
    circuits_df  = session.table(f"{processed_schema}.circuits")
    races_df     = session.table(f"{processed_schema}.races")
    results_df   = session.table(f"{processed_schema}.results").filter(F.col("file_date") == v_file_date)

    race_circuits_df = races_df.join(
        circuits_df, races_df["circuit_ref"] == circuits_df["circuit_ref"], "inner"
    ).select(
        races_df["race_id"],
        races_df["race_year"],
        races_df["name"].alias("race_name"),
        races_df["race_timestamp"].alias("race_date"),
        circuits_df["location"].alias("circuit_location"),
    )

    final_df = (
        results_df
        .join(race_circuits_df, results_df["race_id"] == race_circuits_df["race_id"])
        .join(drivers_df,       results_df["driver_id"] == drivers_df["driver_ref"])
        .join(constructors_df,  results_df["constructor_id"] == constructors_df["constructor_ref"])
        .select(
            race_circuits_df["race_id"].alias("race_id"),
            race_circuits_df["race_year"].alias("race_year"),
            race_circuits_df["race_name"].alias("race_name"),
            race_circuits_df["race_date"].alias("race_date"),
            race_circuits_df["circuit_location"].alias("circuit_location"),
            drivers_df["name"].alias("driver_name"),
            drivers_df["number"].alias("driver_number"),
            drivers_df["nationality"].alias("driver_nationality"),
            constructors_df["name"].alias("team"),
            results_df["grid"].alias("grid"),
            results_df["fastest_lap"].alias("fastest_lap"),
            results_df["time"].alias("race_time"),
            results_df["points"].alias("points"),
            results_df["position"].alias("position"),
            results_df["file_date"].alias("file_date"),
            F.current_timestamp().alias("created_date"),
        )
    )

    merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
    deduped_df = final_df.dropDuplicates(["driver_name", "race_id"])
    merge_delta_data(deduped_df, presentation_schema, "race_results",
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
