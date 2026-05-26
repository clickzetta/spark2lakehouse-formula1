# ZettaPark — 对应原始：01_ingestion/1.ingest_circuits_file.py
#
# 迁移说明：
#   spark.read.csv(path, schema=...) → session.read.csv(path, schema=...)
#   withColumnRenamed → rename(F.col("old"), "new")
#   withColumn → with_column
#   saveAsTable → write.save_as_table(mode="overwrite")
#   dbutils.widgets → Python 变量（无等价物）
#   %run includes → import

import sys
sys.path.insert(0, "..")

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from clickzetta.zettapark.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)
from includes.configuration import raw_folder_path, processed_schema
from includes.common_functions import add_ingestion_date

# 参数（原始通过 dbutils.widgets 传入）
v_data_source = ""
v_file_date   = "2021-03-21"


def ingest_circuits(session: Session):
    circuits_schema = StructType([
        StructField("circuitId",  IntegerType(), False),
        StructField("circuitRef", StringType(),  True),
        StructField("name",       StringType(),  True),
        StructField("location",   StringType(),  True),
        StructField("country",    StringType(),  True),
        StructField("lat",        DoubleType(),  True),
        StructField("lng",        DoubleType(),  True),
        StructField("alt",        IntegerType(), True),
        StructField("url",        StringType(),  True),
    ])

    circuits_df = session.read.csv(
        f"{raw_folder_path}/circuits.csv",
        schema=circuits_schema,
        header=True,
    )

    circuits_selected_df = circuits_df.select(
        "circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt"
    )

    circuits_renamed_df = (
        circuits_selected_df
        .rename(F.col("circuitId"),  "circuit_id")
        .rename(F.col("circuitRef"), "circuit_ref")
        .rename(F.col("lat"),        "latitude")
        .rename(F.col("lng"),        "longitude")
        .rename(F.col("alt"),        "altitude")
        .with_column("data_source", F.lit(v_data_source))
        .with_column("file_date",   F.lit(v_file_date))
    )

    circuits_final_df = add_ingestion_date(circuits_renamed_df)

    circuits_final_df.write.save_as_table(
        f"{processed_schema}.circuits", mode="overwrite"
    )
    return circuits_final_df


if __name__ == "__main__":
    from includes.configuration import SCHEMA_NAME
    # 连接参数从环境变量读取（与 setup.py 保持一致）
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

    ingest_circuits(session)
    session.close()
