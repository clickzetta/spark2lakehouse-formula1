# ZettaPark — 对应原始：02_transformation/3.constructor_standings.py
#
# 迁移说明：与 2.driver_standings.py 结构完全相同，按车队分组

import sys
sys.path.insert(0, "..")

from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
from clickzetta.zettapark.window import Window
from includes.configuration import presentation_schema
from includes.common_functions import df_column_to_list, merge_delta_data

v_file_date = "2021-03-28"


def produce_constructor_standings(session: Session):
    race_results_df = (
        session.table(f"{presentation_schema}.race_results")
        .filter(F.col("file_date") == v_file_date)
    )

    race_year_list = df_column_to_list(race_results_df, "race_year")

    race_results_df = (
        session.table(f"{presentation_schema}.race_results")
        .filter(F.col("race_year").isin(race_year_list))
    )

    constructor_standings_df = (
        race_results_df
        .group_by("race_year", "team")
        .agg(
            F.sum("points").alias("total_points"),
            F.count(F.when(F.col("position") == 1, True)).alias("wins"),
        )
    )

    constructor_rank_spec = (
        Window.partition_by("race_year")
        .order_by(F.col("total_points").desc(), F.col("wins").desc())
    )
    final_df = constructor_standings_df.with_column("rank", F.rank().over(constructor_rank_spec))

    merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
    merge_delta_data(session, final_df, presentation_schema, "constructor_standings",
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

    produce_constructor_standings(session)
    session.close()
