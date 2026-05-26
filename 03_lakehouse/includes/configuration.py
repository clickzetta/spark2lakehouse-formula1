# ZettaPark — 对应原始：05_includes/configuration.py
#
# 迁移说明：
#   原始使用 DBFS 挂载路径 /mnt/formula1dltr/...
#   Lakehouse 使用 Named Volume，路径格式为 vol://schema.volume_name/
#   session.read.csv/json 使用 vol:// 协议；COPY INTO 使用 FROM VOLUME 关键字语法

import os

SCHEMA_NAME   = os.environ.get("CLICKZETTA_SCHEMA", "mcp_demo")
VOLUME_NAME   = os.environ.get("CLICKZETTA_VOLUME", "formula1_vol")
VOLUME_PATH   = f"vol://{SCHEMA_NAME}.{VOLUME_NAME}"   # ZettaPark session.read 使用

raw_folder_path          = VOLUME_PATH
processed_schema         = "f1_processed"
presentation_schema      = "f1_presentation"
