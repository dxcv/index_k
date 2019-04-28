import calendar as cld, datetime as dt
import pandas as pd
from utils.algorithm import preprocess as pre, calculation as cal, timeutils as tu
from utils.database import config as cfg, io
from utils.dev import calargs

engines = cfg.load_engine()
engine_rd = engines["2Gb"]
today = dt.date.today()

_freq = "w"
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


for year in range(2017, 2018):
    for month in range(12, 13):
        month_range = cld.monthrange(year, month)[1]
        for day in range(31, 31 + 1):
            result_return = []
            result_risk = []
            result_sub = []

            statistic_date = dt.date(year, month, day)
            # ids_used = pre.fetch_fids_used(statistic_date=statistic_date, freq=_freq, conn=engine_rd)
            ids_used = ["JR113432"]

            data = pre.ProcessedData(statistic_date, ids_used, _freq)
            bms = {index_name: cal.Benchmark(attr_dict, index_name) for index_name, attr_dict in data.index.items()}
            tbond = cal.Tbond(data.index["y1_treasury_rate"], "y1_treasury_rate")
            for fid, attrs in data.funds.items():
                fund = cal.Fund(attrs)
                res_return, funcs_return_sorted = cal.calculate(_funcs_return, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
                res_risk, funcs_risk_sorted = cal.calculate(_funcs_risk, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
                res_sub, funcs_sub_sorted = cal.calculate(_funcs_sub, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
                result_return.extend(res_return)
                result_risk.extend(res_risk)
                result_sub.extend(res_sub)

            df_return = pd.DataFrame(result_return)
            df_risk = pd.DataFrame(result_risk)
            df_sub = pd.DataFrame(result_sub)

            cols_return = cal.format_cols_private(
                funcs_return_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"]
            )
            cols_risk = cal.format_cols(
                funcs_risk_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"]
            )
            cols_sub = cal.format_cols(
                funcs_sub_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"]
            )

            df_return.columns = cols_return
            df_risk.columns = cols_risk
            df_sub.columns = cols_sub

            # io.to_sql("fund_weekly_return", conn=engine_rd, dataframe=df_return, chunksize=5000)
            # io.to_sql("fund_weekly_risk", conn=engine_rd, dataframe=df_risk, chunksize=5000)
            # io.to_sql("fund_subsidiary_weekly_index", conn=engine_rd, dataframe=df_sub, chunksize=5000)
