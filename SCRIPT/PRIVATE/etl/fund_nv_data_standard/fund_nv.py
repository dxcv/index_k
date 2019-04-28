from utils.database import config as cfg
from utils.database import io
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.database.models.config_private import SyncSource, SourceInfo
from utils.database.models.base_private import FundNvDataSource, FundNvDataStandard
from utils.etlkit.reader.mysqlreader import MysqlInput
from utils.etlkit.core.base import Stream
from utils.etlkit.core import transform
from multiprocessing import Pool
import pandas as pd
from functools import partial
from collections import OrderedDict
import datetime as dt
from dateutil.relativedelta import relativedelta


dbsession = sessionmaker()
ENGINE_R1 = cfg.load_engine()["2Gb"]
ENGINE_R2 = create_engine("mysql+pymysql://root:smyt0317@58cb57c164977.sh.cdb.myqcloud.com:4171/config_private?charset=utf8")
SESSION_BASE = dbsession(bind=ENGINE_R1)
SESSION_CFG = dbsession(bind=ENGINE_R2)


def encap_to_old(df):
    SRC = {"私募云通": 0, "第三方": 3, "托管机构": 1, "投顾公开": 2, "数据授权": 4}
    try:
        df["source_code"] = df["source"].apply(lambda x: SRC[x])
    except:
        pass


# JR003099
def init_const():
    SOURCE_MAP = {
        "00": "私募云通",
        "02": "第三方",
        "03": "托管机构",
        "04": "托管机构",
        "05": "投顾公开"
    }
    stmt = SESSION_CFG.query(SourceInfo).filter(
        SourceInfo.is_granted == 1
    ).with_entities(
        SourceInfo.source_id
    )
    GRANTED_SOURCES = {x[0] for x in stmt.all()}
    return SOURCE_MAP, GRANTED_SOURCES


def WRONG():
    sql = "SELECT t1.fund_id, t1.source_id FROM `fund_nv_data_source_copy2` t1 \
    JOIN (SELECT fund_id, source_id, MIN(statistic_date) msd FROM fund_nv_data_source_copy2 \
    WHERE nav > 50 AND is_used = 1 GROUP BY fund_id, source_id) t2 \
    ON t1.fund_id = t2.fund_id AND t1.source_id = t2.source_id AND t1.statistic_date = t2.msd"
    tmp = pd.read_sql(sql, ENGINE_R1)
    return set(tmp.fund_id.drop_duplicates().tolist())


def init_firstnv():
    sql = "SELECT t1.fund_id, t1.source_id, t1.nav FROM `fund_nv_data_source_copy2` t1 \
    JOIN (SELECT fund_id, source_id, MIN(statistic_date) msd FROM fund_nv_data_source_copy2 \
    WHERE is_used = 1 GROUP BY fund_id, source_id) t2 \
    ON t1.fund_id = t2.fund_id AND t1.source_id = t2.source_id AND t1.statistic_date = t2.msd \
    WHERE t1.nav > 50"
    tmp = pd.read_sql(sql, ENGINE_R1)
    keys = tuple(zip(tmp["fund_id"], tmp["source_id"]))
    vals = tmp["nav"].tolist()
    d = dict(zip(keys, vals))
    return d


SOURCE_MAP, GRANTED_SOURCES = init_const()

NOT_MOD = {"JR116914", "JR113397", "JR113763", "JR002004", "JR106604", "JR117424"}


def mod100(fund_id, src_id, nav, mod_list):
    if (fund_id in NOT_MOD) or src_id == "000001":  # 云通源不做处理
        return nav
    k = (fund_id, src_id)
    val_t0 = mod_list.get(k)
    if val_t0 is not None:  # 如果基金初始净值大于50
        if nav > 0.2 * val_t0:  # 如果基金当期净值仍然大于初始净值的20%
            nav /= 100  # 对净值作单位转化

    else:  # 如果基金初始净值小于50
        if nav >= 80:  # 如果当期净值大于等于80
            nav /= 100  # 对净值作单位转化
    return nav


def tag_source(source):
    return "数据授权" if source in GRANTED_SOURCES else SOURCE_MAP[source[:2]]


def fund_src_nv(task, src_priority=None, mod_list=None):
    """

    Args:
        task: (fund_id, statistic_date)

    Returns:

    """

    fund_id, statistic_date = task
    stmt = SESSION_BASE.query(FundNvDataSource).with_entities(
        FundNvDataSource.fund_id, FundNvDataSource.statistic_date, FundNvDataSource.source_id,
        FundNvDataSource.nav, FundNvDataSource.added_nav,
    ).filter(
        FundNvDataSource.is_used == 1, FundNvDataSource.fund_id == fund_id, FundNvDataSource.statistic_date == statistic_date,
        FundNvDataSource.source_id.in_(src_priority.keys()), FundNvDataSource.added_nav != 0, FundNvDataSource.nav != 0,
        # FundNvDataSource.update_time <= dt.datetime(2018, 4, 19, 13, 20, 0)  # TEST
    )

    inp = MysqlInput(SESSION_BASE.bind, stmt)

    if mod_list is None:
        mod_list = init_firstnv()

    vm = transform.ValueMap(
        {
            FundNvDataSource.nav.name: (lambda nv, fid, src: mod100(fid, src, nv, mod_list), FundNvDataSource.nav.name, FundNvDataSource.fund_id.name, FundNvDataSource.source_id.name),
            FundNvDataSource.added_nav.name: (lambda nv, fid, src: mod100(fid, src, nv, mod_list), FundNvDataSource.added_nav.name, FundNvDataSource.fund_id.name, FundNvDataSource.source_id.name)
        }
    )

    sfd = transform.SortFillnaDropduplicate(
        [FundNvDataSource.source_id.name], ascending=[False], subset=[FundNvDataSource.fund_id.name], key={FundNvDataSource.source_id.name: src_priority},
    )

    vm2 = transform.ValueMap(
        OrderedDict([
            (FundNvDataStandard.source.name, (lambda x: tag_source(x), FundNvDataSource.source_id.name)),
        ])
    )

    sk = transform.MapSelectKeys(
        {
            FundNvDataSource.fund_id.name: FundNvDataStandard.fund_id.name,
            FundNvDataSource.statistic_date.name: FundNvDataStandard.statistic_date.name,
            FundNvDataStandard.source.name: None,
            FundNvDataSource.nav.name: FundNvDataStandard.nav.name,
            FundNvDataSource.added_nav.name: FundNvDataStandard.added_nav.name,
            FundNvDataSource.adjusted_nav.name: FundNvDataStandard.swanav.name,

        }
    )
    s = Stream(inp.dataframe, transform=[vm, sfd, vm2, sk])
    s.flow()
    return s.dataframe


def sync_by_fund(fund_id, mod_list=None):
    sql = "SELECT DISTINCT fund_id, statistic_date FROM fund_nv_data_source_copy2 WHERE fund_id = '%s'" % fund_id
    df = pd.read_sql(sql, ENGINE_R1)
    df_result = pd.DataFrame()

    stmt2 = SESSION_CFG.query(SyncSource).filter(
        SyncSource.is_used == 1, SyncSource.target_table == "fund_nv_data_standard", SyncSource.pk == fund_id,
        SyncSource.source_id.notin_({"03", "04", "05"})
    ).with_entities(
        SyncSource.source_id, SyncSource.priority
    )

    src_priority = dict(stmt2.all())

    for fid, sd in df.values:
        task = (fid, sd)
        tmp = fund_src_nv(task, src_priority, mod_list)

        df_result = df_result.append(tmp)
    io.to_sql("fund_nv_data_standard_copy2", ENGINE_R1, df_result)
    ######## maintain the F**KING old table
    encap_to_old(df_result)
    io.to_sql("fund_nv_data_standard", ENGINE_R1, df_result)
    # io.to_sql("fund_nv_data_standard", ENGINE_R1, df_result)  # 对于有mod的净值, 旧表做一次性覆盖更新;
    ########################################


def main():
    p = Pool(6)
    UPDATE_SINCE = (dt.datetime.now() - relativedelta(minutes=12)).strftime("%Y%m%d%H%M%S")
    sql_fids_from_syncsrc = "SELECT DISTINCT pk FROM sync_source WHERE `target_table` = 'fund_nv_data_standard' AND source_id NOT IN ('03', '04', '05') AND update_time >= '{us}'".format(us=UPDATE_SINCE)
    sql_fids_from_nvsrc = "SELECT DISTINCT fund_id FROM fund_nv_data_source_copy2 WHERE source_id NOT IN ('03', '04', '05') AND update_time >= '{us}'".format(us=UPDATE_SINCE)

    fids_from_syncsrc = [x[0] for x in ENGINE_R2.execute(sql_fids_from_syncsrc).fetchall()]
    fids_from_nvsrc = [x[0] for x in ENGINE_R1.execute(sql_fids_from_nvsrc).fetchall()]
    fids = sorted(set([*fids_from_nvsrc, *fids_from_syncsrc]))

    print("TIME: {nw}, NUM: {n}".format(nw=dt.datetime.now(), n=len(fids)))
    print(fids)
    mod_list = init_firstnv()
    sync_by_fund_with_mod = partial(sync_by_fund, mod_list=mod_list)
    p.map(sync_by_fund_with_mod, fids)
    print("Done.")


def test():
    fids = ["JR090613"]

    # JR090613 2018 三方源乱入
    q = init_firstnv()
    for fid in fids:
        fund_id = fid
        sync_by_fund(fund_id, q)


if __name__ == "__main__":
    main()
