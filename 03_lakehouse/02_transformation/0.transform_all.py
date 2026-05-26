# ZettaPark — 对应原始：02_transformation/0.transform_all.py
#
# 迁移说明：
#   原始使用 dbutils.notebook.run() 串行调用各 notebook
#   ZettaPark 版本用 importlib 按路径加载各模块

import sys
import importlib.util
from pathlib import Path

_here = Path(__file__).parent
sys.path.insert(0, str(_here.parent))   # 03_lakehouse/ — for includes.*

import os
from dotenv import load_dotenv
load_dotenv(_here.parent.parent / ".env")

from clickzetta.zettapark.session import Session
from includes.configuration import SCHEMA_NAME


def _load(filename):
    path = _here / filename
    spec = importlib.util.spec_from_file_location(filename.stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


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
    _load(Path("1.race_results.py")).produce_race_results(session)
    _load(Path("2.driver_standings.py")).produce_driver_standings(session)
    _load(Path("3.constructor_standings.py")).produce_constructor_standings(session)
    _load(Path("4.calculated_race_results.py")).produce_calculated_race_results(session)
finally:
    session.close()
