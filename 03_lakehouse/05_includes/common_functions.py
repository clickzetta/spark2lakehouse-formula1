# ZettaPark — 对应原始：05_includes/common_functions.py
#
# 迁移说明：
#   add_ingestion_date: withColumn → with_column，current_timestamp 来自 zettapark.functions
#   re_arrange_partition_column: schema.names → [f.name for f in df.schema.fields]
#   overwrite_partition: saveAsTable mode="overwrite" + partition_by
#   merge_delta_data: session.sql("MERGE INTO ...") — ZettaPark 支持标准 MERGE INTO 语法
#   df_column_to_list: select + distinct + collect，Row 访问方式相同

from clickzetta.zettapark import functions as F


def add_ingestion_date(df):
    return df.with_column("ingestion_date", F.current_timestamp())


def re_arrange_partition_column(input_df, partition_column):
    column_list = [f.name for f in input_df.schema.fields if f.name != partition_column]
    column_list.append(partition_column)
    return input_df.select(column_list)


def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    output_df.write.save_as_table(
        f"{db_name}.{table_name}",
        mode="overwrite",
        partition_by=[partition_column],
    )


def df_column_to_list(input_df, column_name):
    rows = input_df.select(column_name).distinct().collect()
    return [row[column_name] for row in rows]


def merge_delta_data(session, input_df, db_name, table_name, merge_condition, partition_column):
    """
    先建临时视图，再用 session.sql 执行 MERGE INTO。
    ZettaPark 不支持 DeltaTable.forPath，改用标准 SQL MERGE INTO。
    """
    input_df.create_or_replace_temp_view("_merge_src")

    table_exists = False
    try:
        session.sql(f"DESCRIBE TABLE {db_name}.{table_name}").collect()
        table_exists = True
    except Exception:
        pass

    if table_exists:
        session.sql(f"""
            MERGE INTO {db_name}.{table_name} tgt
            USING _merge_src src
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        input_df.write.save_as_table(
            f"{db_name}.{table_name}",
            mode="overwrite",
            partition_by=[partition_column],
        )
