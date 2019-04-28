from utils.database import io, config as cfg
import pandas as pd
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
import datetime as dt
# 净值分离脚本, 供服务器测试用;

sources = {
    0: "000001",
    1: "020008",
    2: "020004",
    3: "020001",
    4: "020002",
    5: "020003",
    6: "04",
    7: "03",
    8: "05",
    9: "05",
    10: "05",
    11: None,
    12: "020005",
    13: "020007",
}

sources_priority = {
    "000001": 10,
    "020008": 8,
    "020004": 8,
    "020001": 8,
    "020002": 8,
    "020003": 8,
    "04": 8,
    "03": 8,
    "05": 8,
    "05": 8,
    "05": 8,
    "020005": 8,
    "020007": 8,
}

eg = cfg.load_engine()["2Gb"]
eg_cfg = create_engine("mysql+pymysql://root:smyt0317@182.254.128.241:4171/config_private?charset=utf8")
eg_crawl = create_engine("mysql+pymysql://root:smyt0317@182.254.128.241:4171/crawl_private?charset=utf8")


def check_need_mod(x):
    return int(x > 30) if x is not None else 0


def update_nv_data_source(fids):
    for fid in fids:
        print(fid)
        sql_nvsrc = "SELECT data_source as source_id, statistic_date, nav, added_nav, IFNULL(swanav, swanav1) as swanav FROM fund_nv_data_source WHERE fund_id='{fid}'".format(
            fid=fid
        )
        sql_nv = "SELECT statistic_date, nav, added_nav, swanav FROM fund_nv_data_standard WHERE fund_id='{fid}'".format(
            fid=fid
        )
        df_nvsrc = pd.read_sql(sql_nvsrc, eg)
        df_nv = pd.read_sql(sql_nv, eg)

        a = df_nv.groupby(["statistic_date"]).last()  # 标准表净值数据
        b = df_nvsrc.groupby(["statistic_date", "source_id"]).last()  # 多源表净值数据

        cols_cp = ["nav", "added_nav"]

        # 去除只存在于nv_std中, 但不存在于nv_src表中的记录
        if len(a) > 0 and len(b) != 0:
            idx_dates = list(set(a.index).intersection(b.index.levels[0]))
            a = a.loc[idx_dates]

            # Cond.1 一个日期下所有源 与 标准净值表 全部不相等,
            # 需要去除只存在于fund_nv_data_source表而不存在于fund_nv_data_standard表中的数据
            all_date_not_eql = (~a[cols_cp].eq(b.loc[idx_dates][cols_cp]).all(axis=1)).all(level=0)

            # Cond.2 一个日期下存在某个源的净值 与 标准净值表不相等
            all_date_src_eql = a[cols_cp].eq(b[cols_cp]).all(level=[0, 1]).all(axis=1)

            # Cond.1
            # 将符合Cond.1的标准净值表数据存入 y_fund_nv(采集库.云通源净值表)
            result_all_not_eql = a[all_date_not_eql].reset_index()
            result_all_not_eql["fund_id"] = fid
            result_all_not_eql["source_id"] = 0
            result_all_not_eql["is_used"] = 1
            # result_all_not_eql.columns = ["statistic_date", "nav", "added_nav", "swanav", "fund_id", "source_id", "is_used"]

            result_smyt = result_all_not_eql[["statistic_date", "nav", "added_nav", "swanav", "fund_id"]]
            result_smyt.columns = ["statistic_date", "nav", "added_nav", "adjusted_nav", "fund_id"]

            # Cond.2
            result_nv_src = df_nvsrc.copy()
            result_nv_src["is_used"] = all_date_src_eql.apply(lambda x: int(x)).tolist()
            result_nv_src["fund_id"] = fid

            result = result_nv_src.append(result_all_not_eql)
            result["source_id"] = result["source_id"].apply(lambda x: sources.get(x))
            result.dropna(subset=["source_id"])
            # 标识可能需要除100的记录(以nav > 30界定)
            result["need_mod"] = result["nav"].apply(check_need_mod)
            result.columns = [
                'added_nav', 'fund_id', 'is_used', 'nav', 'source_id', 'statistic_date', 'adjusted_nav', 'need_mod']

            result_sync = result[["fund_id", "source_id"]].drop_duplicates()
            result_sync.columns = ["pk", "source_id"]
            result_sync["target_table"] = "fund_nv_data_standard"
            result_sync["priority"] = result_sync["source_id"].apply(lambda x: sources_priority.get(x, 0))

            io.to_sql("y_fund_nv", eg_crawl, result_smyt)
            io.to_sql("fund_nv_data_source_copy2", eg, result)
            io.to_sql("sync_source", eg_cfg, result_sync)

        elif len(a) > 0 and len(b) == 0:  # 处理只存在fund_nv_data_standard但不存在于fund_nv_data_source的数据
            print("NO SOURCE DATA FOUND: {fid}".format(fid=fid))
            result_smyt = a.reset_index()
            result_smyt.columns = ["statistic_date", "nav", "added_nav", "adjusted_nav"]
            result_smyt["fund_id"] = fid

            result_sync = result_smyt[["fund_id"]].drop_duplicates()
            result_sync.columns = ["pk"]
            result_sync["target_table"] = "fund_nv_data_standard"
            result_sync["source_id"] = "000001"
            result_sync["priority"] = 10
            result_sync["is_used"] = 1

            result = result_smyt.copy()
            result["is_used"] = 1
            result["need_mod"] = result["nav"].apply(check_need_mod)

            io.to_sql("y_fund_nv", eg_crawl, result_smyt)
            io.to_sql("fund_nv_data_source_copy2", eg, result)
            io.to_sql("sync_source", eg_cfg, result_sync)


def main():
    now = dt.datetime.now()
    t1 = now.strftime("%Y%m%d%H%M%S")
    t0 = (now - relativedelta(hours=1, minutes=10)).strftime("%Y%m%d%H%M%S")
    fids = eg.execute("SELECT DISTINCT fund_id FROM fund_nv_data_standard WHERE update_time BETWEEN '{t0}' AND '{t1}'".format(t0=t0, t1=t1)).fetchall()
    fids = [x[0] for x in fids]
    update_nv_data_source(fids)


def test():
    import sys
    start, end = sys.argv[1], sys.argv[2]

    fids = eg.execute("SELECT DISTINCT fund_id FROM fund_nv_data_standard").fetchall()
    fids = [x[0] for x in fids]
    tmp = fids[int(start):int(end)]
    update_nv_data_source(tmp)


if __name__ == "__main__":
    test()
