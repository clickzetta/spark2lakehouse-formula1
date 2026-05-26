# ZettaPark — 对应原始：01_ingestion/5.ingest_results_file.py
#
# 迁移说明：
#   dropDuplicates(['race_id', 'driver_id']) → drop_duplicates(["race_id", "driver_id"])
#   merge_delta_data → merge_delta_data(session, df, ...) — ZettaPark 版本需要传入 session

import sys
sys.path.insert(0, "..")

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from includes.configuration import raw_folder_path, processed_schema
from includes.common_functions import add_ingestion_date, merge_delta_data

v_data_source = ""
v_file_date   = "2021-03-28"


def ingest_results(session: Session):
    results_df = session.read.json(
        f"{raw_folder_path}/results.json"
    )

    results_with_columns_df = (
        results_df
        .rename(F.col("resultId"),       "result_id")
        .rename(F.col("raceId"),         "race_id")
        .rename(F.col("driverId"),       "driver_id")
        .rename(F.col("constructorId"),  "constructor_id")
        .rename(F.col("positionText"),   "position_text")
        .rename(F.col("positionOrder"),  "position_order")
        .rename(F.col("fastestLap"),     "fastest_lap")
        .rename(F.col("fastestLapTime"), "fastest_lap_time")
        .rename(F.col("fastestLapSpeed"),"fastest_lap_speed")
        .with_column("data_source", F.lit(v_data_source))
        .with_column("file_date",   F.lit(v_file_date))
    )

    results_with_columns_df = add_ingestion_date(results_with_columns_df)

    results_final_df = results_with_columns_df.drop("statusId")

    results_deduped_df = results_final_df.drop_duplicates(["race_id", "driver_id"])

    merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
    merge_delta_data(session, results_deduped_df, processed_schema, "results",
                     merge_condition, "race_id")
    return results_deduped_df


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

    ingest_results(session)
    session.close()
