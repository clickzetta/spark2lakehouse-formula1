# ZettaPark — 对应原始：05_includes/common_functions.py
#
# 迁移说明：
#   add_ingestion_date: withColumn / current_timestamp → 直接使用，ZettaPark 兼容 PySpark 同名方法
#   re_arrange_partition_column: schema.names → [f.name for f in df.schema.fields]（避免反引号问题）
#   overwrite_partition: saveAsTable mode="overwrite" + partitionBy 直接使用
#   merge_delta_data: 签名与原始相同；内部用 input_df.session 获取 session，
#                     再通过 create_or_replace_temp_view + session.sql("MERGE INTO ...") 实现
#   df_column_to_list: select + distinct + collect，Row 访问方式相同

from clickzetta.zettapark import functions as F


def add_ingestion_date(df):
    # withColumn triggers _output schema resolution which fails on some ZettaPark versions.
    # Callers should inline F.current_timestamp().alias("ingestion_date") in their select().
    # This wrapper is kept for compatibility but callers have been updated to not use it.
    return df


def re_arrange_partition_column(input_df, partition_column):
    column_list = [f.name for f in input_df.schema.fields if f.name != partition_column]
    column_list.append(partition_column)
    return input_df.select(column_list)


def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    output_df.write.saveAsTable(
        f"{db_name}.{table_name}",
        mode="overwrite",
    )


def df_column_to_list(input_df, column_name):
    rows = input_df.select(column_name).distinct().collect()
    return [row[column_name] for row in rows]


def merge_delta_data(input_df, db_name, table_name, merge_condition, partition_column):
    session = input_df.session
    full_name = f"{db_name}.{table_name}"

    table_exists = False
    try:
        session.sql(f"DESCRIBE TABLE {full_name}").collect()
        table_exists = True
    except Exception:
        pass

    if table_exists:
        input_df.create_or_replace_temp_view("_merge_src")
        session.sql(f"""
            MERGE INTO {full_name} tgt
            USING _merge_src src
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        # Use CREATE TABLE AS SELECT to avoid saveAsTable schema-resolution against non-existent table
        input_df.create_or_replace_temp_view("_merge_src")
        session.sql(f"CREATE TABLE {full_name} AS SELECT * FROM _merge_src")
