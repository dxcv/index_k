import datetime as dt
import pandas as pd
from utils.algorithm import preprocess as pre, calculation as cal
from utils.database import config as cfg, io, sqlfactory as sf
from utils.dev import calargs_org4r
from dateutil.relativedelta import relativedelta
from multiprocessing import Pool

# SET UP
sf.SQL.Org4R.INDEXID = "OI01"  # 使用OI01作为输入计算
pre.SQL_USED = sf.SQL.Org4R
cal.calargs = calargs_org4r  # 设置对应计算参数
engines = cfg.load_engine()

engine_rd = engines["2Gb"]

today = dt.date.today()
last_month = today - dt.timedelta(today.day)

_freq = "m"
_intervals = calargs_org4r.intervals
_funcs_return = [
    "accumulative_return", "return_a", "excess_return_a", "sharpe_a", "calmar_a", "sortino_a", "info_a",
    "jensen_a", "treynor_a"
]
_funcs_risk = [
    "stdev_a", "downside_deviation_a", "max_drawdown", "beta", "corr", "mdd_time", "VaR"
]
_funcs_sub = [
    "odds", "ability_timing", "ability_security", "persistence"
]


_bms_used = ["hs300", "csi500", "sse50", "cbi", "FI01", "nfi"]


def generate_tasks(update_time_l, update_time_r):
    try:
        tasks = pre.generate_tasks(update_time_l, update_time_r, freq="w", processes=7, conn=engine_rd)
        tasks = {k: v for k, v in tasks.items() if k >= dt.date(2015, 1, 1)}
        print("{l}->{r}".format(l=update_time_l, r=update_time_r), len(tasks))

    except ValueError as e:
        print("{l}->{r}".format(l=update_time_l, r=update_time_r), e)
    return tasks


def calculate(statistic_date_with_ids_used):
    """

    Args:
        tasks<date: {oid1, oid2, ..., }>:

    Returns:

    """
    statistic_date, ids_used = statistic_date_with_ids_used
    data = pre.ProcessedData(statistic_date, sorted(ids_used), _freq)
    bms = {index_name: cal.Benchmark(attr_dict, index_name) for index_name, attr_dict in data.index.items()}
    tbond = cal.Tbond(data.index["y1_treasury_rate"], "y1_treasury_rate")
    for fid, attrs in data.funds.items():
        fund = cal.Fund(attrs)

        result_return = []
        result_risk = []
        result_sub = []

        res_return, cols_return_sorted = cal.calculate(_funcs_return, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
        res_risk, cols_risk_sorted = cal.calculate(_funcs_risk, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
        res_sub, cols_sub_sorted = cal.calculate(_funcs_sub, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
        result_return.extend(res_return)
        result_risk.extend(res_risk)
        result_sub.extend(res_sub)

        df_return = pd.DataFrame(result_return)
        df_risk = pd.DataFrame(result_risk)
        df_sub = pd.DataFrame(result_sub)

        cols_return = cal.format_cols_org4r(
            cols_return_sorted, _freq, prefix=["org_id", "org_name", "statistic_date", "benchmark"]
        )
        cols_risk = cal.format_cols_org4r(
            cols_risk_sorted, _freq, prefix=["org_id", "org_name", "statistic_date", "benchmark"]
        )
        cols_sub = cal.format_cols_org4r(
            cols_sub_sorted, _freq, prefix=["org_id", "org_name", "statistic_date", "benchmark"]
        )

        df_return.columns = cols_return
        df_risk.columns = cols_risk
        df_sub.columns = cols_sub

        df_return["index_id"] = sf.SQL.Org4R.INDEXID
        df_risk["index_id"] = sf.SQL.Org4R.INDEXID
        df_sub["index_id"] = sf.SQL.Org4R.INDEXID

        io.to_sql("org_monthly_return", conn=engine_rd, dataframe=df_return, chunksize=5000)
        io.to_sql("org_monthly_risk", conn=engine_rd, dataframe=df_risk, chunksize=5000)
        io.to_sql("org_monthly_research", conn=engine_rd, dataframe=df_sub, chunksize=5000)


def main():
    UPDATE_TIME = dt.datetime.now()
    l, r = UPDATE_TIME - relativedelta(hours=1, minutes=5), UPDATE_TIME
    print(l, r)
    p = Pool(4)

    tasks = generate_tasks(l, r)
    tasks = sorted(tasks.items(), key=lambda x: x[0])
    # tasks = [(dt.date(2017, 10, 31), {"P1000445"})]
    if len(tasks) > 0:
        p.map(calculate, tasks)
        p.close()
        p.join()


if __name__ == "__main__":
    main()
