# ZettaPark — 对应原始：05_includes/configuration.py
#
# 迁移说明：
#   原始使用 DBFS 挂载路径 /mnt/formula1dltr/...
#   Lakehouse 使用 Named Volume，路径格式为 vol://schema.volume_name/
#   session.read.csv/json 使用 vol:// 协议；COPY INTO 使用 FROM VOLUME 关键字语法

import os

VOLUME_PATH   = "vol://mcp_demo.formula1_vol"   # ZettaPark session.read 使用
SCHEMA_NAME   = "mcp_demo"                       # Volume 所在 schema（用于 USE SCHEMA）

raw_folder_path          = VOLUME_PATH
processed_schema         = "f1_processed"
presentation_schema      = "f1_presentation"
