import datetime as dt, time
import pandas as pd
from utils.algorithm import preprocess as pre, calculation as cal
from utils.database import config as cfg, io
from utils.dev import calargs

UPDATE_TIME = dt.datetime.fromtimestamp(time.mktime((dt.date.today() - dt.timedelta(1)).timetuple()))
last_month = UPDATE_TIME - dt.timedelta(UPDATE_TIME.day)

engines = cfg.load_engine()
engine_rd = engines["2Gb"]

_freq = "m"
_intervals = calargs.intervals
_funcs_1 = [
    "ddr3_a", "pain_index", "CVaR", "average_drawdown"
]
_funcs_2 = [
    "upsidecap", "downsidecap", "pain_ratio", "ERCVaR", "return_Msqr", "adjusted_jensen_a", "assess_ratio", "sterling_a", "excess_pl", "beta_timing_camp", "corr_spearman"
]
_funcs_3 = [
    "hurst",  "burke_a", "kappa_a", "omega", "stock_hm", "timing_hm", "upbeta_cl", "downbeta_cl"
]

_bms_used = ["hs300", "csi500", "sse50", "cbi", "strategy", "FI01", "nfi"]

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
    data = pre.ProcessedData(statistic_date, list(ids_used), _freq)
    bms = {index_name: cal.Benchmark(attr_dict, index_name) for index_name, attr_dict in data.index.items()}
    tbond = cal.Tbond(data.index["y1_treasury_rate"], "y1_treasury_rate")
    for fid, attrs in data.funds.items():
        fund = cal.Fund(attrs)
        res_1, _funcs_1_sorted = cal.calculate(_funcs_1, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
        res_2, _funcs_2_sorted = cal.calculate(_funcs_2, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
        res_3, _funcs_3_sorted = cal.calculate(_funcs_3, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
        result_1.extend(res_1)
        result_2.extend(res_2)
        result_3.extend(res_3)
    df_return = pd.DataFrame(result_1)
    df_risk = pd.DataFrame(result_2)
    df_sub = pd.DataFrame(result_3)

    cols_return = cal.format_cols_private(_funcs_1_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
    cols_risk = cal.format_cols_private(_funcs_2_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
    cols_sub = cal.format_cols_private(_funcs_3_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])

    df_return.columns = cols_return
    df_risk.columns = cols_risk
    df_sub.columns = cols_sub
    try:
        io.to_sql("fund_month_risk2", conn=engine_rd, dataframe=df_return, chunksize=500)
        io.to_sql("fund_subsidiary_month_index2", conn=engine_rd, dataframe=df_risk, chunksize=500)
        io.to_sql("fund_subsidiary_month_index3", conn=engine_rd, dataframe=df_sub, chunksize=500)
    except Exception as e:
        time.sleep(10)
        io.to_sql("fund_month_risk2", conn=engine_rd, dataframe=df_return, chunksize=500)
        io.to_sql("fund_subsidiary_month_index2", conn=engine_rd, dataframe=df_risk, chunksize=500)
        io.to_sql("fund_subsidiary_month_index3", conn=engine_rd, dataframe=df_sub, chunksize=500)

    print("TASK DONE: {ut}".format(ut=UPDATE_TIME))
