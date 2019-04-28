import datetime as dt, time
import pandas as pd
from utils.algorithm import preprocess as pre, calculation as cal, timeutils as tu
from utils.database import config as cfg, io
from utils.dev import calargs

UPDATE_TIME = dt.datetime.fromtimestamp(time.mktime((dt.date.today() - dt.timedelta(1)).timetuple()))

engines = cfg.load_engine()
engine_rd = engines["2Gb"]

_freq = "d"
# _freq = "w"
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

cols_return = cal.format_cols(_funcs_return, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
cols_risk = cal.format_cols(_funcs_risk, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
cols_sub = cal.format_cols(_funcs_sub, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])


_bms_used = ["hs300", "csi500", "sse50", "cbi", "strategy", "FI01", "nfi"]


# for update_time in [UPDATE_TIME]:
#     try:
#         tasks = pre.generate_tasks(update_time, update_time + dt.timedelta(1) - dt.timedelta(0, 0, 1), freq="w", processes=7)
#         tasks = {k: v for k, v in tasks.items() if k >= dt.date(2015, 1, 1)}
#         print(update_time, len(tasks))
#
#     except ValueError as e:
#         print(update_time, e)
#         continue

# for statistic_date, ids_used in sorted(tasks.items(), key=lambda x: x[0]):
statistic_date = dt.date(2017, 8, 25)
ids_used = ["JR000001"]

result_return = []
result_risk = []
result_sub = []
data = pre.ProcessedData(statistic_date, list(ids_used), _freq, pe=[], weekday=True)


bms = {index_name: cal.Benchmark(attr_dict, index_name) for index_name, attr_dict in data.index.items()}
tbond = cal.Tbond(data.index["y1_treasury_rate"], "y1_treasury_rate")
for fid, attrs in data.funds.items():
    fund = cal.Fund(attrs)
    res_return = cal.calculate(_funcs_return, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond)
    res_risk = cal.calculate(_funcs_risk, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond)
    res_sub = cal.calculate(_funcs_sub, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond)
    result_return.extend(res_return)
    result_risk.extend(res_risk)
    result_sub.extend(res_sub)

df_return = pd.DataFrame(result_return)
df_risk = pd.DataFrame(result_risk)
df_sub = pd.DataFrame(result_sub)

df_return.columns = cols_return
df_risk.columns = cols_risk
df_sub.columns = cols_sub
#
# io.to_sql("fund_weekly_return", conn=engine_rd, dataframe=df_return, chunksize=5000)
# io.to_sql("fund_weekly_risk", conn=engine_rd, dataframe=df_risk, chunksize=5000)
# io.to_sql("fund_subsidiary_weekly_index", conn=engine_rd, dataframe=df_sub, chunksize=5000)
