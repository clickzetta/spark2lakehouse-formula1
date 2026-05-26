# ZettaPark — 对应原始：01_ingestion/5.ingest_results_file.py
#
# 迁移说明：
#   dropDuplicates / merge_delta_data → 直接使用，ZettaPark 兼容 PySpark 同名方法
#   merge_delta_data → 签名与原始相同，内部通过 input_df.session 获取 session

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from clickzetta.zettapark.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, FloatType
)
from includes.configuration import raw_folder_path, processed_schema
from includes.common_functions import add_ingestion_date, merge_delta_data

v_data_source = ""
v_file_date   = "2021-03-28"


def ingest_results(session: Session):
    results_schema = StructType([
        StructField("resultId",        IntegerType(), True),
        StructField("raceId",          IntegerType(), True),
        StructField("driverId",        StringType(),  True),
        StructField("constructorId",   StringType(),  True),
        StructField("number",          IntegerType(), True),
        StructField("grid",            IntegerType(), True),
        StructField("position",        IntegerType(), True),
        StructField("positionText",    StringType(),  True),
        StructField("positionOrder",   IntegerType(), True),
        StructField("points",          DoubleType(),  True),
        StructField("laps",            IntegerType(), True),
        StructField("time",            StringType(),  True),
        StructField("milliseconds",    IntegerType(), True),
        StructField("fastestLap",      IntegerType(), True),
        StructField("rank",            IntegerType(), True),
        StructField("fastestLapTime",  StringType(),  True),
        StructField("fastestLapSpeed", DoubleType(),  True),
        StructField("statusId",        StringType(),  True),
    ])

    results_df = (
        session.read
        .schema(results_schema)
        .json(f"{raw_folder_path}/results.json")
    )

    results_with_columns_df = results_df.select(
        F.col("resultId").alias("result_id"),
        F.col("raceId").alias("race_id"),
        F.col("driverId").alias("driver_id"),
        F.col("constructorId").alias("constructor_id"),
        F.col("number"),
        F.col("grid"),
        F.col("position"),
        F.col("positionText").alias("position_text"),
        F.col("positionOrder").alias("position_order"),
        F.col("points"),
        F.col("laps"),
        F.col("time"),
        F.col("milliseconds"),
        F.col("fastestLap").alias("fastest_lap"),
        F.col("rank"),
        F.col("fastestLapTime").alias("fastest_lap_time"),
        F.col("fastestLapSpeed").alias("fastest_lap_speed"),
        F.lit(v_data_source).alias("data_source"),
        F.lit(v_file_date).alias("file_date"),
        F.current_timestamp().alias("ingestion_date"),
    )

    merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
    deduped_df = results_with_columns_df.dropDuplicates(["result_id", "race_id"])
    merge_delta_data(deduped_df, processed_schema, "results",
                     merge_condition, "race_id")
    return results_with_columns_df


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
