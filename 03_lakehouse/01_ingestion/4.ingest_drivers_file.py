# ZettaPark — 对应原始：01_ingestion/4.ingest_drivers_file.py
#
# 迁移说明：
#   原始 drivers.json 有嵌套 name: {forename, surname}，Jolpica 版本已展开为 forename/surname 两列
#   concat_ws(" ", col("name.forename"), col("name.surname")) → F.concat_ws(" ", "forename", "surname")
#   drop('url') → drop("url")

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from clickzetta.zettapark.types import (
    StructType, StructField, IntegerType, StringType
)
from includes.configuration import raw_folder_path, processed_schema
from includes.common_functions import add_ingestion_date

v_data_source = ""
v_file_date   = "2021-03-21"


def ingest_drivers(session: Session):
    drivers_schema = StructType([
        StructField("driverId",    IntegerType(), True),
        StructField("driverRef",   StringType(),  True),
        StructField("number",      IntegerType(), True),
        StructField("code",        StringType(),  True),
        StructField("forename",    StringType(),  True),
        StructField("surname",     StringType(),  True),
        StructField("dob",         StringType(),  True),
        StructField("nationality", StringType(),  True),
        StructField("url",         StringType(),  True),
    ])

    drivers_df = (
        session.read
        .schema(drivers_schema)
        .json(f"{raw_folder_path}/drivers.json")
    )

    drivers_final_df = drivers_df.select(
        F.col("driverId").alias("driver_id"),
        F.col("driverRef").alias("driver_ref"),
        F.col("number"),
        F.col("code"),
        F.concat_ws(F.lit(" "), F.col("forename"), F.col("surname")).alias("name"),
        F.col("dob"),
        F.col("nationality"),
        F.lit(v_data_source).alias("data_source"),
        F.lit(v_file_date).alias("file_date"),
        F.current_timestamp().alias("ingestion_date"),
    )

    drivers_final_df.write.saveAsTable(
        f"{processed_schema}.drivers", mode="overwrite"
    )
    return drivers_final_df


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

    ingest_drivers(session)
    session.close()
