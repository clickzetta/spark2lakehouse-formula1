# ZettaPark — 对应原始：01_ingestion/3.ingest_constructors_file.py
#
# 迁移说明：
#   spark.read.json(path, schema=...) → session.read.json(path, schema=...)
#   drop(col('url')) → drop("url")
#   withColumnRenamed → 直接使用，ZettaPark 兼容 PySpark 同名方法

import sys
sys.path.insert(0, "..")

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from includes.configuration import raw_folder_path, processed_schema
from includes.common_functions import add_ingestion_date

v_data_source = ""
v_file_date   = "2021-03-21"


def ingest_constructors(session: Session):
    constructors_df = session.read.json(
        f"{raw_folder_path}/constructors.json"
    )

    constructor_dropped_df = constructors_df.drop("url")

    constructor_final_df = (
        constructor_dropped_df
        .withColumnRenamed("constructorId", "constructor_id")
        .withColumnRenamed("constructorRef", "constructor_ref")
        .withColumn("data_source", F.lit(v_data_source))
        .withColumn("file_date",   F.lit(v_file_date))
    )

    constructor_final_df = add_ingestion_date(constructor_final_df)

    constructor_final_df.write.saveAsTable(
        f"{processed_schema}.constructors", mode="overwrite"
    )
    return constructor_final_df


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

    ingest_constructors(session)
    session.close()
