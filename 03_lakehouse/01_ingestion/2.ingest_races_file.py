# ZettaPark — 对应原始：01_ingestion/2.ingest_races_file.py
#
# 迁移说明：
#   to_timestamp(concat(...)) → F.to_timestamp(F.concat(...))
#   coalesce → F.coalesce
#   partitionBy → write.saveAsTable(partitionBy=[...])

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from clickzetta.zettapark.types import (
    StructType, StructField, IntegerType, StringType, DateType
)
from includes.configuration import raw_folder_path, processed_schema
from includes.common_functions import add_ingestion_date

v_data_source = ""
v_file_date   = "2021-03-21"


def ingest_races(session: Session):
    races_schema = StructType([
        StructField("raceId",    IntegerType(), True),
        StructField("year",      IntegerType(), True),
        StructField("round",     IntegerType(), True),
        StructField("circuitId", StringType(),  True),
        StructField("name",      StringType(),  True),
        StructField("date",      StringType(),  True),
        StructField("time",      StringType(),  True),
        StructField("url",       StringType(),  True),
    ])

    races_df = (
        session.read
        .option("header", True)
        .schema(races_schema)
        .csv(f"{raw_folder_path}/races.csv")
    )

    races_selected_df = races_df.select(
        F.col("raceId").alias("race_id"),
        F.col("year").alias("race_year"),
        F.col("round"),
        F.col("circuitId").alias("circuit_ref"),
        F.col("name"),
        F.to_timestamp(
            F.concat(
                F.col("date"), F.lit(" "),
                F.coalesce(
                    F.when(F.col("time") == "\\N", None).otherwise(F.col("time")),
                    F.lit("00:00:00"),
                ),
            ),
            "yyyy-MM-dd HH:mm:ss",
        ).alias("race_timestamp"),
        F.lit(v_data_source).alias("data_source"),
        F.lit(v_file_date).alias("file_date"),
        F.current_timestamp().alias("ingestion_date"),
    )

    races_selected_df.write.saveAsTable(
        f"{processed_schema}.races",
        mode="overwrite",
    )
    return races_selected_df


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

    ingest_races(session)
    session.close()
