import calendar as cld, datetime as dt
import pandas as pd
from utils.algorithm import preprocess as pre, calculation as cal
from utils.database import config as cfg, io
from utils.dev import calargs

today = dt.date.today()
last_month = today - dt.timedelta(today.day)

engines = cfg.load_engine()
engine_rd = engines["2Gb"]
today = dt.date.today()

_freq = "m"
_intervals = calargs.intervals

_funcs_return = [
    "accumulative_return", "return_a", "sharpe_a", "calmar_a", "sortino_a"
]

_funcs_risk = [
    "stdev", "stdev_a", "downside_deviation_a", "max_drawdown"
]

_funcs_sub = [
    "con_rise_periods", "con_fall_periods", "VaR", "p_earning_periods", "n_earning_periods", "min_return", "max_return", "mdd_repair_time",
    "mdd_time", "skewness", "kurtosis", "ERVaR"
]


for year in range(last_month.year, last_month.year + 1):
    for month in range(last_month.month, last_month.month + 1):
        month_range = cld.monthrange(year, month)[1]
        for day in range(month_range, month_range + 1):
            result_return = []
            result_risk = []
            result_sub = []

            statistic_date = dt.date(year, month, day)
            ids_used = pre.fetch_fids_used(statistic_date=statistic_date, freq=_freq, conn=engine_rd)

            data = pre.ProcessedData(statistic_date, [], _freq)
            bms = {index_id: cal.Benchmark(attr_dict, index_id) for index_id, attr_dict in data.index.items()}
            tbond = cal.Tbond(data.index["y1_treasury_rate"], "y1_treasury_rate")

            for bm_name, bm in bms.items():
                if bm.id == "y1_treasury_rate":
                    continue

                res_return, funcs_return_sorted = cal.calculate(_funcs_return, _intervals, None, _freq, statistic_date, bm, None, tbond, with_func_names=True)
                res_risk, funcs_risk_sorted = cal.calculate(_funcs_risk, _intervals, None, _freq, statistic_date, bm, None, tbond, with_func_names=True)
                res_sub, funcs_sub_sorted = cal.calculate(_funcs_sub, _intervals, None, _freq, statistic_date, bm, None, tbond, with_func_names=True)

                result_return.extend(res_return)
                result_risk.extend(res_risk)
                result_sub.extend(res_sub)

            df_return = pd.DataFrame(result_return)
            df_risk = pd.DataFrame(result_risk)
            df_sub = pd.DataFrame(result_sub)

            cols_return = cal.format_cols(funcs_return_sorted, _freq, prefix=["index_id", "index_name", "statistic_date"])
            cols_risk = cal.format_cols(funcs_risk_sorted, _freq, prefix=["index_id", "index_name", "statistic_date"])
            cols_sub = cal.format_cols(funcs_sub_sorted, _freq, prefix=["index_id", "index_name", "statistic_date"])

            df_return.columns = cols_return
            df_risk.columns = cols_risk
            df_sub.columns = cols_sub
            df_return["index_id"] = df_return["index_id"].apply(str.upper)
            df_risk["index_id"] = df_risk["index_id"].apply(str.upper)
            df_sub["index_id"] = df_sub["index_id"].apply(str.upper)

            io.to_sql("index_monthly_return", conn=engine_rd, dataframe=df_return, chunksize=5000)
            io.to_sql("index_monthly_risk", conn=engine_rd, dataframe=df_risk, chunksize=5000)
            io.to_sql("index_monthly_subsidiary", conn=engine_rd, dataframe=df_sub, chunksize=5000)

# res_return = cal.calculate(_funcs_return, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond)
# res_risk = cal.calculate(_funcs_risk, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond)
# res_sub = cal.calculate(_funcs_sub, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond)
# result_return.extend(res_return)
# result_risk.extend(res_risk)
# result_sub.extend(res_sub)

# df_return = pd.DataFrame(result_return)
# df_risk = pd.DataFrame(result_risk)
# df_sub = pd.DataFrame(result_sub)
#
# df_return.columns = cols_return
# df_risk.columns = cols_risk
# df_sub.columns = cols_sub