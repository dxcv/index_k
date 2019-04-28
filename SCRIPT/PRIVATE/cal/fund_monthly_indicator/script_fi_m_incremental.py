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
_funcs_return = [
    "accumulative_return", "return_a", "excess_return_a", "odds", "sharpe_a", "calmar_a", "sortino_a", "info_a",
    "jensen_a", "treynor_a"
]
_funcs_risk = [
    "stdev", "stdev_a", "downside_deviation_a", "max_drawdown", "beta", "corr"
]
_funcs_sub = [
    "con_rise_periods", "con_fall_periods", "ability_timing", "ability_security", "persistence", "unsystematic_risk",
    "tracking_error_a", "VaR", "p_earning_periods", "n_earning_periods", "min_return", "max_return", "mdd_repair_time",
    "mdd_time", "skewness", "kurtosis", "ERVaR"
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
    result_return = []
    result_risk = []
    result_sub = []
    data = pre.ProcessedData(statistic_date, list(ids_used), _freq)
    bms = {index_name: cal.Benchmark(attr_dict, index_name) for index_name, attr_dict in data.index.items()}
    tbond = cal.Tbond(data.index["y1_treasury_rate"], "y1_treasury_rate")
    for fid, attrs in data.funds.items():
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

    cols_return = cal.format_cols_private(_funcs_return_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
    cols_risk = cal.format_cols_private(_funcs_risk_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
    cols_sub = cal.format_cols_private(_funcs_sub_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])

    df_return.columns = cols_return
    df_risk.columns = cols_risk
    df_sub.columns = cols_sub
    try:
        io.to_sql("fund_month_return", conn=engine_rd, dataframe=df_return, chunksize=500)
        io.to_sql("fund_month_risk", conn=engine_rd, dataframe=df_risk, chunksize=500)
        io.to_sql("fund_subsidiary_month_index", conn=engine_rd, dataframe=df_sub, chunksize=500)
    except Exception as e:
        time.sleep(10)
        io.to_sql("fund_month_return", conn=engine_rd, dataframe=df_return, chunksize=500)
        io.to_sql("fund_month_risk", conn=engine_rd, dataframe=df_risk, chunksize=500)
        io.to_sql("fund_subsidiary_month_index", conn=engine_rd, dataframe=df_sub, chunksize=500)

    print("TASK DONE: {ut}".format(ut=UPDATE_TIME))
