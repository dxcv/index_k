from utils.database import config as cfg
from utils.etlkit.reader.mysqlreader import MysqlInput
from sqlalchemy import and_, distinct
from sqlalchemy.orm import sessionmaker
from utils.etlkit.core.base import Stream
from utils.etlkit.core import transform
from utils.database.models.config_private import SourceInfo
from utils.database.models.crawl_private import DFundNv, TFundNv, YFundNv, GFundNv
from utils.database.models.base_private import FundNvDataSource, IdMatch
from utils.database import io
import datetime as dt
from multiprocessing import Pool
import pandas as pd


engine_w = cfg.load_engine()["etl_crawl_private"]
engine_r = cfg.load_engine()["etl_crawl_private_old"]

dbsession = sessionmaker()


def dfundnv(source_id):
    SOURCES = {
        1: "020008",
        2: "020004",
        3: "020001",
        4: "020002",
        5: "020003",
        6: "020005",
    }

    mapped_source_id = SOURCES[source_id]

    sql_ids = "SELECT DISTINCT fund_id FROM d_fund_nv_data WHERE data_source = '{sid}'".format(
        sid=source_id
    )
    fids = pd.read_sql(sql_ids, engine_r)["fund_id"].tolist()[98662:]

    for fid in fids:
        sql = "SELECT fund_id, fund_name, statistic_date, nav, sanav as added_nav \
        FROM d_fund_nv_data WHERE data_source = '{sid}' AND fund_id = '{fid}'".format(
            sid=source_id, fid=fid
        )
        df_nv = pd.read_sql(sql, engine_r)
        df_nv["source_id"] = mapped_source_id
        print(df_nv)
        io.to_sql("d_fund_nv", engine_w, df_nv, type="ignore")


def tfundnv():
    mids = pd.read_sql("SELECT source_id, source FROM base.id_match WHERE source LIKE '04%%' AND source <> '04'", engine_r)
    SOURCES = dict(mids.as_matrix())

    fids = pd.read_sql("SELECT DISTINCT fund_id FROM t_fund_nv_data", engine_r)
    fids["source_id"] = fids["fund_id"].apply(lambda x: SOURCES.get(x))
    fids = dict(fids.dropna().as_matrix())

    for fid in sorted(fids.keys()):
        sql = "SELECT fund_id, fund_name, statistic_date, nav, added_nav as added_nav \
        FROM t_fund_nv_data WHERE fund_id = '{fid}'".format(
            fid=fid
        )
        df_nv = pd.read_sql(sql, engine_r)
        df_nv["source_id"] = fids[fid]
        print(df_nv)
        io.to_sql("t_fund_nv", engine_w, df_nv, type="ignore")


def tmp_sync():
    # dfundnv(4)
    tfundnv()

if __name__ == "__main__":
    tmp_sync()
