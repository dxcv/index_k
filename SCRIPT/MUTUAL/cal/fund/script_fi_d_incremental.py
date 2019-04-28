import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
from utils.algorithm import preprocess as pre, calculation as cal
from utils.database import config as cfg, io, sqlfactory as sf
from utils.dev import calargs_mutual
import time
from sqlalchemy import create_engine

# 设置preprocess, calculation类的计算参数为公募的计算参数;
pre.SQL_USED = sf.SQL.Mutual
cal.calargs = calargs_mutual


UPDATE_TIME = dt.datetime.now()

engines = cfg.load_engine()
# engine_rd = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@10.66.175.186:3306/base",
#                           connect_args={"charset": "utf8"})
engine_rd = engines["2Gbp"]
engine_mkt = engines["2Gb"]
engine_wt = engines["2Gbp"]

_freq = "d"
_intervals = calargs_mutual.intervals
_funcs_return = [
    "accumulative_return", "return_a", "excess_return_a", "persistence", "odds", "sharpe_a", "calmar_a", "sortino_a", "info_a",
    "jensen_a", "treynor_a"
]
_funcs_risk = [
    "stdev", "stdev_a", "downside_deviation_a", "unsystematic_risk", "max_drawdown", "mdd_time", "mdd_repair_time", "beta", "corr", "VaR", "ERVaR"
]
_funcs_sub = [
    "con_rise_periods", "con_fall_periods", "ability_timing", "ability_security", "tracking_error_a", "p_earning_periods", "n_earning_periods", "min_return", "max_return",
    "skewness", "kurtosis",
]

_bms_used = ["hs300", "csi500", "sse50", "cbi", "nfi"]
for update_time in [UPDATE_TIME]:
    try:
        tasks = pre.generate_tasks(update_time - relativedelta(hours=35, minutes=5), update_time, freq=_freq, processes=7, conn=engine_rd)
        tasks = {k: v for k, v in tasks.items() if k >= dt.date(2015, 1, 1)}
        print(update_time, len(tasks))

    except ValueError as e:
        print(update_time, e)
        continue
    for statistic_date, ids_used in sorted(tasks.items(), key=lambda x: x[0]):
        if statistic_date != dt.date(2017, 9 ,21): continue
        print("UPDATE TIME:{ut}: STATISTIC DATE:{sd}, LENGTH:{l}".format(ut=update_time, sd=statistic_date, l=len(ids_used)))
        result_return = []
        result_risk = []
        result_sub = []
        data = pre.ProcessedData(statistic_date, list(ids_used), _freq, pe=[], conn=engine_rd, conn_mkt=engine_mkt)
        bms = {index_name: cal.Benchmark(attr_dict, index_name) for index_name, attr_dict in data.index.items()}
        tbond = cal.Tbond(data.index["y1_treasury_rate"], "y1_treasury_rate")
        for fid, attrs in data.funds.items():
            if fid != "000127": continue
            fund = cal.Fund(attrs)
            res_return, _funcs_return_sorted = cal.calculate(_funcs_return, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
            res_risk, _funcs_risk_sorted = cal.calculate(_funcs_risk, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
            res_sub, _funcs_sub_sorted = cal.calculate(_funcs_sub, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
            result_return.extend(res_return)
            result_risk.extend(res_risk)
            result_sub.extend(res_sub)
        df_return = pd.DataFrame(result_return)
        df_risk = pd.DataFrame(result_risk)
        df_sub = pd.DataFrame(result_sub)

        cols_return = cal.format_cols_mutual(_funcs_return_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
        cols_risk = cal.format_cols_mutual(_funcs_risk_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
        cols_sub = cal.format_cols_mutual(_funcs_sub_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])

        df_return.columns = cols_return
        df_risk.columns = cols_risk
        df_sub.columns = cols_sub
        try:
            io.to_sql("fund_daily_return", conn=engine_wt, dataframe=df_return, chunksize=5000)
            io.to_sql("fund_daily_risk", conn=engine_wt, dataframe=df_risk, chunksize=5000)
            io.to_sql("fund_daily_subsidiary", conn=engine_wt, dataframe=df_sub, chunksize=5000)
        except Exception as e:
            time.sleep(10)
            io.to_sql("fund_daily_return", conn=engine_wt, dataframe=df_return, chunksize=5000)
            io.to_sql("fund_daily_risk", conn=engine_wt, dataframe=df_risk, chunksize=5000)
            io.to_sql("fund_daily_subsidiary", conn=engine_wt, dataframe=df_sub, chunksize=5000)

    print("TASK DONE: {ut}".format(ut=update_time))
