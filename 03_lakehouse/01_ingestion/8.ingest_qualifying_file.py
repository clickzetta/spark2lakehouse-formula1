# ZettaPark — 对应原始：01_ingestion/8.ingest_qualifying_file.py
#
# 迁移说明：
#   原始从 DBFS 目录读取多个 JSON；Jolpica API 未提供 qualifying 数据，文件暂未下载到 Volume
#   如需完整数据，可通过 Jolpica /ergast/f1/{season}/{round}/qualifying.json 接口下载

import sys
sys.path.insert(0, "..")

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from includes.configuration import raw_folder_path, processed_schema
from includes.common_functions import add_ingestion_date, merge_delta_data

v_data_source = ""
v_file_date   = "2021-03-21"


def ingest_qualifying(session: Session):
    qualifying_df = session.read.json(
        f"{raw_folder_path}/qualifying.json"
    )

    final_df = (
        qualifying_df
        .withColumnRenamed("qualifyId", "qualify_id")
        .withColumnRenamed("driverId", "driver_id")
        .withColumnRenamed("raceId", "race_id")
        .withColumnRenamed("constructorId", "constructor_id")
        .withColumn("data_source", F.lit(v_data_source))
        .withColumn("file_date",   F.lit(v_file_date))
    )

    final_df = add_ingestion_date(final_df)

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
