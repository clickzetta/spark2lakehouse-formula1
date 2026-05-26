# ZettaPark — 对应原始：01_ingestion/5.ingest_results_file.py
#
# 迁移说明：
#   dropDuplicates / merge_delta_data → 直接使用，ZettaPark 兼容 PySpark 同名方法
#   merge_delta_data → 签名与原始相同，内部通过 input_df.session 获取 session

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
        .withColumnRenamed("resultId", "result_id")
        .withColumnRenamed("raceId", "race_id")
        .withColumnRenamed("driverId", "driver_id")
        .withColumnRenamed("constructorId", "constructor_id")
        .withColumnRenamed("positionText", "position_text")
        .withColumnRenamed("positionOrder", "position_order")
        .withColumnRenamed("fastestLap", "fastest_lap")
        .withColumnRenamed("fastestLapTime", "fastest_lap_time")
        .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
        .withColumn("data_source", F.lit(v_data_source))
        .withColumn("file_date",   F.lit(v_file_date))
    )

    results_with_columns_df = add_ingestion_date(results_with_columns_df)

    results_final_df = results_with_columns_df.drop("statusId")

    results_deduped_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

    merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
    merge_delta_data(results_deduped_df, processed_schema, "results",
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
