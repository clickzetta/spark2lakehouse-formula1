# ZettaPark — 对应原始：05_includes/common_functions.py
#
# 迁移说明：
#   add_ingestion_date: withColumn / current_timestamp → 直接使用，ZettaPark 兼容 PySpark 同名方法
#   re_arrange_partition_column: 签名与原始相同，分区列移到最后（PARTITIONED BY 要求）
#   overwrite_partition: 新表用 CREATE TABLE ... PARTITIONED BY + INSERT INTO，已有表用 INSERT OVERWRITE
#   merge_delta_data: 新表用 CREATE TABLE ... PARTITIONED BY + INSERT INTO，已有表用 MERGE INTO
#   df_column_to_list: select + distinct + collect，Row 访问方式相同

from clickzetta.zettapark import functions as F

_TYPE_MAP = {
    "LongType": "BIGINT", "IntegerType": "INT", "ShortType": "SMALLINT",
    "ByteType": "TINYINT", "DoubleType": "DOUBLE", "FloatType": "FLOAT",
    "DecimalType": "DECIMAL", "StringType": "STRING", "BooleanType": "BOOLEAN",
    "DateType": "DATE", "TimestampType": "TIMESTAMP", "BinaryType": "BINARY",
}


def _sql_type(datatype):
    return _TYPE_MAP.get(type(datatype).__name__, "STRING")


def _field_name(f):
    return f.name.strip("`")


def add_ingestion_date(df):
    return df.withColumn("ingestion_date", F.current_timestamp())


def re_arrange_partition_column(input_df, partition_column):
    column_list = [_field_name(f) for f in input_df.schema.fields if _field_name(f) != partition_column]
    column_list.append(partition_column)
    return input_df.select(column_list)


def _create_partitioned_table(session, full_name, input_df, partition_column):
    """CREATE TABLE ... PARTITIONED BY (...) then INSERT INTO."""
    fields = input_df.schema.fields
    col_defs = ", ".join(
        f"{_field_name(f)} {_sql_type(f.datatype)}"
        for f in fields
        if _field_name(f) != partition_column
    )
    part_field = next(f for f in fields if _field_name(f) == partition_column)
    part_def = f"{_field_name(part_field)} {_sql_type(part_field.datatype)}"

    session.sql(f"CREATE TABLE {full_name} ({col_defs}) PARTITIONED BY ({part_def})").collect()
    all_cols = ", ".join(_field_name(f) for f in input_df.schema.fields)
    input_df.create_or_replace_temp_view("_insert_src")
    session.sql(f"INSERT INTO {full_name} ({all_cols}) SELECT {all_cols} FROM _insert_src").collect()


def overwrite_partition(input_df, db_name, table_name, partition_column):
    session = input_df.session
    full_name = f"{db_name}.{table_name}"
    output_df = re_arrange_partition_column(input_df, partition_column)

    table_exists = False
    try:
        session.sql(f"DESCRIBE TABLE {full_name}").collect()
        table_exists = True
    except Exception:
        pass

    if table_exists:
        output_df.create_or_replace_temp_view("_insert_src")
        session.sql(f"INSERT OVERWRITE {full_name} SELECT * FROM _insert_src").collect()
    else:
        _create_partitioned_table(session, full_name, output_df, partition_column)


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
        """).collect()
    else:
        output_df = re_arrange_partition_column(input_df, partition_column)
        _create_partitioned_table(session, full_name, output_df, partition_column)
