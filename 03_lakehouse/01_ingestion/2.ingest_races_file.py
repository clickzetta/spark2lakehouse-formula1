# ZettaPark — 对应原始：01_ingestion/2.ingest_races_file.py
#
# 迁移说明：
#   to_timestamp(concat(...)) → F.to_timestamp(F.concat(...))
#   coalesce → F.coalesce
#   partitionBy → write.saveAsTable(partitionBy=[...])

import sys
sys.path.insert(0, "..")

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
        StructField("raceId",    IntegerType(), False),
        StructField("year",      IntegerType(), True),
        StructField("round",     IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name",      StringType(),  True),
        StructField("date",      StringType(),  True),
        StructField("time",      StringType(),  True),
        StructField("url",       StringType(),  True),
    ])

    races_df = session.read.csv(
        f"{raw_folder_path}/races.csv",
        schema=races_schema,
        header=True,
    )

    races_clean = races_df.withColumn(
        "time",
        F.when(F.col("time") == "\\N", None).otherwise(F.col("time"))
    )

    races_with_ts = (
        races_clean
        .withColumn(
            "race_timestamp",
            F.to_timestamp(
                F.concat(F.col("date"), F.lit(" "), F.coalesce(F.col("time"), F.lit("00:00:00"))),
                "yyyy-MM-dd HH:mm:ss",
            )
        )
        .withColumn("data_source", F.lit(v_data_source))
        .withColumn("file_date",   F.lit(v_file_date))
    )

    races_with_ts = add_ingestion_date(races_with_ts)

    races_selected_df = races_with_ts.select(
        F.col("raceId").alias("race_id"),
        F.col("year").alias("race_year"),
        F.col("round"),
        F.col("circuitId").alias("circuit_id"),
        F.col("name"),
        F.col("ingestion_date"),
        F.col("race_timestamp"),
    )

    races_selected_df.write.saveAsTable(
        f"{processed_schema}.races",
        mode="overwrite",
        partitionBy=["race_year"],
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
