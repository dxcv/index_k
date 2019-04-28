import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
from utils.algorithm import preprocess as pre, calculation as cal
from utils.database import config as cfg, io, sqlfactory as sf
from utils.dev import calargs_mutual
import calendar as cld
from sqlalchemy import create_engine

# 设置preprocess, calculation类的计算参数为公募的计算参数;
pre.SQL_USED = sf.SQL.Mutual
cal.calargs = calargs_mutual


engines = cfg.load_engine()
engine_rd = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@10.66.175.186:3306/base_public", connect_args={"charset": "utf8"})
engine_mkt = engines["2Gb"]
engine_wt = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@10.66.175.186:3306/base_public", connect_args={"charset": "utf8"})
# engine_rd = engines["2Gbp"]
# engine_wt = engines["2Gbp"]

_freq = "m"
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

for year in range(2017, 2017 + 1):
    for month in range(9, 9 + 1):
        month_range = cld.monthrange(year, month)[1]
        for day in range(month_range, month_range + 1):

            result_return = []
            result_risk = []
            result_sub = []

            statistic_date = dt.date(year, month, day)
            ids_used = pre.fetch_fids_used(statistic_date=statistic_date, freq=_freq, conn=engine_rd)
            if len(ids_used) == 0: continue
            print(statistic_date, len(ids_used))

            data = pre.ProcessedData(statistic_date, list(ids_used), _freq, pe=[], conn=engine_rd, conn_mkt=engine_mkt)
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
                break
            df_return = pd.DataFrame(result_return)
            df_risk = pd.DataFrame(result_risk)
            df_sub = pd.DataFrame(result_sub)

            cols_return = cal.format_cols_mutual(_funcs_return_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
            cols_risk = cal.format_cols_mutual(_funcs_risk_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
            cols_sub = cal.format_cols_mutual(_funcs_sub_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])

            df_return.columns = cols_return
            df_risk.columns = cols_risk
            df_sub.columns = cols_sub
            break
            try:
                io.to_sql("fund_monthly_return", conn=engine_wt, dataframe=df_return, chunksize=500)
                io.to_sql("fund_monthly_risk", conn=engine_wt, dataframe=df_risk, chunksize=500)
                io.to_sql("fund_monthly_subsidiary", conn=engine_wt, dataframe=df_sub, chunksize=500)
            except Exception as e:
                io.to_sql("fund_monthly_return", conn=engine_wt, dataframe=df_return, chunksize=500)
                io.to_sql("fund_monthly_risk", conn=engine_wt, dataframe=df_risk, chunksize=500)
                io.to_sql("fund_monthly_subsidiary", conn=engine_wt, dataframe=df_sub, chunksize=500)


