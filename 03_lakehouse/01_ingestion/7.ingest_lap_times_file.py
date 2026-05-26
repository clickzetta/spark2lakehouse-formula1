# ZettaPark — 对应原始：01_ingestion/7.ingest_lap_times_file.py
#
# 迁移说明：
#   原始从 DBFS 目录读取多个 CSV；Jolpica API 未提供 lap_times 数据，文件暂未下载到 Volume
#   如需完整数据，可通过 Jolpica /ergast/f1/{season}/{round}/laps.json 接口下载并 PUT 到 Volume

import sys
sys.path.insert(0, "..")

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from includes.configuration import raw_folder_path, processed_schema
from includes.common_functions import add_ingestion_date, merge_delta_data

v_data_source = ""
v_file_date   = "2021-03-21"


def ingest_lap_times(session: Session):
    lap_times_df = session.read.csv(
        f"{raw_folder_path}/lap_times.csv",
        header=True,
    )

    final_df = (
        lap_times_df
        .rename(F.col("driverId"), "driver_id")
        .rename(F.col("raceId"),   "race_id")
        .with_column("data_source", F.lit(v_data_source))
        .with_column("file_date",   F.lit(v_file_date))
    )

    final_df = add_ingestion_date(final_df)

    merge_condition = (
        "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"
    )
    merge_delta_data(session, final_df, processed_schema, "lap_times",
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

    ingest_lap_times(session)
    session.close()
