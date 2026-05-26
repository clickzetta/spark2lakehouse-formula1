# ZettaPark — 对应原始：01_ingestion/0.ingest_all_files.py
#
# 迁移说明：
#   原始使用 dbutils.notebook.run() 串行调用各 notebook
#   ZettaPark 版本直接 import 并调用各模块的 ingest 函数，共享同一个 session

import sys
sys.path.insert(0, "..")

import os
from dotenv import load_dotenv
from clickzetta.zettapark.session import Session
from includes.configuration import SCHEMA_NAME

from ingest_circuits_file    import ingest_circuits
from ingest_races_file       import ingest_races
from ingest_constructors_file import ingest_constructors
from ingest_drivers_file     import ingest_drivers
from ingest_results_file     import ingest_results
from ingest_pit_stops_file   import ingest_pit_stops
from ingest_lap_times_file   import ingest_lap_times
from ingest_qualifying_file  import ingest_qualifying

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

try:
    ingest_circuits(session)
    ingest_races(session)
    ingest_constructors(session)
    ingest_drivers(session)
    ingest_results(session)
    ingest_pit_stops(session)
    ingest_lap_times(session)
    ingest_qualifying(session)
finally:
    session.close()
