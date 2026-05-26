# ZettaPark — 对应原始：02_transformation/2.driver_standings.py
#
# 迁移说明：
#   Window.partitionBy().orderBy() → Window.partition_by().order_by()
#   rank().over(spec) → F.rank().over(spec)
#   groupBy → group_by，agg 相同
#   df_column_to_list → 来自 common_functions

import sys
sys.path.insert(0, "..")

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from clickzetta.zettapark.window import Window
from includes.configuration import presentation_schema
from includes.common_functions import df_column_to_list, merge_delta_data

v_file_date = "2021-03-21"


def produce_driver_standings(session: Session):
    race_results_df = (
        session.table(f"{presentation_schema}.race_results")
        .filter(F.col("file_date") == v_file_date)
    )

    race_year_list = df_column_to_list(race_results_df, "race_year")

    race_results_df = (
        session.table(f"{presentation_schema}.race_results")
        .filter(F.col("race_year").isin(race_year_list))
    )

    driver_standings_df = (
        race_results_df
        .group_by("race_year", "driver_name", "driver_nationality")
        .agg(
            F.sum("points").alias("total_points"),
            F.count(F.when(F.col("position") == 1, True)).alias("wins"),
        )
    )

    driver_rank_spec = (
        Window.partition_by("race_year")
        .order_by(F.col("total_points").desc(), F.col("wins").desc())
    )
    final_df = driver_standings_df.with_column("rank", F.rank().over(driver_rank_spec))

    merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
    merge_delta_data(session, final_df, presentation_schema, "driver_standings",
                     merge_condition, "race_year")
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

    produce_driver_standings(session)
    session.close()
