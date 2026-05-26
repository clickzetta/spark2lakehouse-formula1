# ZettaPark — 对应原始：01_ingestion/8.ingest_qualifying_file.py
#
# 迁移说明：
#   原始从 DBFS 目录读取多个 JSON；Jolpica API 未提供 qualifying 数据，文件暂未下载到 Volume
#   如需完整数据，可通过 Jolpica /ergast/f1/{season}/{round}/qualifying.json 接口下载

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


def ingest_qualifying(session: Session):
    qualifying_schema = StructType([
        StructField("qualifyId",     IntegerType(), True),
        StructField("raceId",        IntegerType(), True),
        StructField("driverId",      StringType(),  True),
        StructField("constructorId", StringType(),  True),
        StructField("number",        IntegerType(), True),
        StructField("position",      IntegerType(), True),
        StructField("q1",            StringType(),  True),
        StructField("q2",            StringType(),  True),
        StructField("q3",            StringType(),  True),
    ])

    qualifying_df = (
        session.read
        .schema(qualifying_schema)
        .json(f"{raw_folder_path}/qualifying.json")
    )

    final_df = qualifying_df.select(
        F.col("qualifyId").alias("qualify_id"),
        F.col("raceId").alias("race_id"),
        F.col("driverId").alias("driver_id"),
        F.col("constructorId").alias("constructor_id"),
        F.col("number"),
        F.col("position"),
        F.col("q1"),
        F.col("q2"),
        F.col("q3"),
        F.lit(v_data_source).alias("data_source"),
        F.lit(v_file_date).alias("file_date"),
        F.current_timestamp().alias("ingestion_date"),
    )

    merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
    merge_delta_data(final_df, processed_schema, "qualifying",
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

    ingest_qualifying(session)
    session.close()
