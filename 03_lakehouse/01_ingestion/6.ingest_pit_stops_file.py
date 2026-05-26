# ZettaPark — 对应原始：01_ingestion/6.ingest_pit_stops_file.py
#
# 迁移说明：
#   multiLine JSON → session.read.json 默认支持，无需额外选项
#   merge_delta_data → ZettaPark 版本需要传入 session

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from clickzetta.zettapark.types import (
    StructType, StructField, IntegerType, StringType
)
from includes.configuration import raw_folder_path, processed_schema
from includes.common_functions import add_ingestion_date, merge_delta_data

v_data_source = ""
v_file_date   = "2021-03-21"


def ingest_pit_stops(session: Session):
    pit_stops_schema = StructType([
        StructField("raceId",       IntegerType(), True),
        StructField("driverId",     StringType(),  True),
        StructField("stop",         IntegerType(), True),
        StructField("lap",          IntegerType(), True),
        StructField("time",         StringType(),  True),
        StructField("duration",     StringType(),  True),
        StructField("milliseconds", IntegerType(), True),
    ])

    pit_stops_df = (
        session.read
        .schema(pit_stops_schema)
        .json(f"{raw_folder_path}/pit_stops.json")
    )

    final_df = pit_stops_df.select(
        F.col("raceId").alias("race_id"),
        F.col("driverId").alias("driver_id"),
        F.col("stop"),
        F.col("lap"),
        F.col("time"),
        F.col("duration"),
        F.col("milliseconds"),
        F.lit(v_data_source).alias("data_source"),
        F.lit(v_file_date).alias("file_date"),
        F.current_timestamp().alias("ingestion_date"),
    )

    merge_condition = (
        "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id "
        "AND tgt.stop = src.stop"
    )
    # Jolpica API may return duplicate rows for the same key; deduplicate before MERGE
    deduped_df = final_df.dropDuplicates(["race_id", "driver_id", "stop"])
    merge_delta_data(deduped_df, processed_schema, "pit_stops",
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

    ingest_pit_stops(session)
    session.close()
