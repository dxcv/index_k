import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundAssetScale
from utils.database.models.base_public import IdMatch, FundInfo, FundAssetScale
from sqlalchemy import create_engine

# _engine_wt = cfg.load_engine()["4Gpp"]
DB = "c"
_engine_wt = create_engine("mysql+pymysql://root:smyt0317@182.254.128.241:4171/crawl_private")

tables = pd.read_sql("SHOW TABLES", _engine_wt)["Tables_in_{db}".format(db=DB)].tolist()
for tb in tables:
    try:
        print(tb)
        _engine_wt.execute("ALTER TABLE {tb} ADD INDEX `idx_update_time`(update_time) USING BTREE".format(
            tb=tb
        ))
    except Exception as e:
        print(tb)
