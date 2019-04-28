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

UPDATE_TIME = dt.datetime.fromtimestamp(time.mktime((dt.date.today() - dt.timedelta(1)).timetuple()))
last_month = UPDATE_TIME - dt.timedelta(UPDATE_TIME.day)


engines = cfg.load_engine()
engine_rd = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@10.66.175.186:3306/base_public", connect_args={"charset": "utf8"})
engine_mkt = engines["2Gb"]
engine_wt = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@10.66.175.186:3306/base_public", connect_args={"charset": "utf8"})
engine_rd = engines["2Gbp"]
engine_wt = engines["2Gbp"]

_freq = "m"
_intervals = calargs_mutual.intervals
_funcs_1 = [
    "ddr3_a", "pain_index", "CVaR", "average_drawdown"
]
_funcs_2 = [
    "upsidecap", "downsidecap", "pain_ratio", "ERCVaR", "return_Msqr", "adjusted_jensen_a", "assess_ratio", "sterling_a", "excess_pl", "beta_timing_camp", "corr_spearman"
]
_funcs_3 = [
    "hurst",  "burke_a", "kappa_a", "omega", "stock_hm", "timing_hm", "upbeta_cl", "downbeta_cl"
]

_bms_used = ["hs300", "csi500", "sse50", "cbi", "nfi"]
try:
    tasks = pre.generate_tasks(last_month - dt.timedelta(last_month.day -1), last_month + dt.timedelta(0, 86399), freq=_freq, processes=7, conn=engine_rd)
    tasks = {k: v for k, v in tasks.items() if (k >= dt.date(2015, 1, 1) and k < dt.date(UPDATE_TIME.year, UPDATE_TIME.month, 1))}
    print(UPDATE_TIME, len(tasks))

except ValueError as e:
    print(UPDATE_TIME, e)

for statistic_date, ids_used in sorted(tasks.items(), key=lambda x: x[0]):
    print("UPDATE TIME:{ut}: STATISTIC DATE:{sd}, LENGTH:{l}".format(ut=UPDATE_TIME, sd=statistic_date, l=len(ids_used)))
    result_1 = []
    result_2 = []
    result_3 = []
    data = pre.ProcessedData(statistic_date, list(ids_used), _freq, pe=[], conn=engine_rd, conn_mkt=engine_mkt)
    bms = {index_name: cal.Benchmark(attr_dict, index_name) for index_name, attr_dict in data.index.items()}
    tbond = cal.Tbond(data.index["y1_treasury_rate"], "y1_treasury_rate")
    for fid, attrs in data.funds.items():
        fund = cal.Fund(attrs)
        res_return, _funcs_1_sorted = cal.calculate(_funcs_1, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
        res_risk, _funcs_2_sorted = cal.calculate(_funcs_2, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
        res_sub, _funcs_3_sorted = cal.calculate(_funcs_3, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
        result_1.extend(res_return)
        result_2.extend(res_risk)
        result_3.extend(res_sub)
    df_1 = pd.DataFrame(result_1)
    df_2 = pd.DataFrame(result_2)
    df_3 = pd.DataFrame(result_3)

    cols_1 = cal.format_cols_mutual(_funcs_1_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
    cols_2 = cal.format_cols_mutual(_funcs_2_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
    cols_3 = cal.format_cols_mutual(_funcs_3_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])

    df_1.columns = cols_1
    df_2.columns = cols_2
    df_3.columns = cols_3
    try:
        io.to_sql("fund_monthly_risk2", conn=engine_wt, dataframe=df_1, chunksize=500)
        io.to_sql("fund_monthly_subsidiary2", conn=engine_wt, dataframe=df_2, chunksize=500)
        io.to_sql("fund_monthly_subsidiary3", conn=engine_wt, dataframe=df_3, chunksize=500)
    except Exception as e:
        time.sleep(10)
        io.to_sql("fund_monthly_risk2", conn=engine_wt, dataframe=df_1, chunksize=500)
        io.to_sql("fund_monthly_subsidiary2", conn=engine_wt, dataframe=df_2, chunksize=500)
        io.to_sql("fund_monthly_subsidiary3", conn=engine_wt, dataframe=df_3, chunksize=500)

    print("TASK DONE: {ut}".format(ut=UPDATE_TIME))
