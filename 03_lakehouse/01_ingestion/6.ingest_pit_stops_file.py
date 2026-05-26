# ZettaPark — 对应原始：01_ingestion/6.ingest_pit_stops_file.py
#
# 迁移说明：
#   multiLine JSON → session.read.json 默认支持，无需额外选项
#   merge_delta_data → ZettaPark 版本需要传入 session

import sys
sys.path.insert(0, "..")

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from includes.configuration import raw_folder_path, processed_schema
from includes.common_functions import add_ingestion_date, merge_delta_data

v_data_source = ""
v_file_date   = "2021-03-21"


def ingest_pit_stops(session: Session):
    pit_stops_df = session.read.json(
        f"{raw_folder_path}/pit_stops.json"
    )

    final_df = (
        pit_stops_df
        .rename(F.col("driverId"), "driver_id")
        .rename(F.col("raceId"),   "race_id")
        .with_column("data_source", F.lit(v_data_source))
        .with_column("file_date",   F.lit(v_file_date))
    )

    final_df = add_ingestion_date(final_df)

    merge_condition = (
        "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id "
        "AND tgt.stop = src.stop"
    )
    merge_delta_data(session, final_df, processed_schema, "pit_stops",
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
