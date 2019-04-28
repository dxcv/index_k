import calendar as cld, datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
from utils.algorithm import preprocess as pre, calculation as cal
from utils.database import config as cfg, io
from utils.dev import calargs

today = dt.date.today() - relativedelta(days=1)

engines = cfg.load_engine()
engine_rd = engines["2Gb"]

_freq = "w"
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


date_rng = pd.date_range(today - relativedelta(weeks=1), today)
for statistic_date in date_rng:
    result_return = []
    result_risk = []
    result_sub = []

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

        cols_return = cal.format_cols(funcs_return_sorted, _freq, prefix=["index_id", "index_name", "statistic_date"])
        cols_risk = cal.format_cols(funcs_risk_sorted, _freq, prefix=["index_id", "index_name", "statistic_date"])
        cols_sub = cal.format_cols(funcs_sub_sorted, _freq, prefix=["index_id", "index_name", "statistic_date"])

    df_return = pd.DataFrame(result_return)
    df_risk = pd.DataFrame(result_risk)
    df_sub = pd.DataFrame(result_sub)

    df_return.columns = cols_return
    df_risk.columns = cols_risk
    df_sub.columns = cols_sub

    df_return["index_id"] = df_return["index_id"].apply(str.upper)
    df_risk["index_id"] = df_risk["index_id"].apply(str.upper)
    df_sub["index_id"] = df_sub["index_id"].apply(str.upper)

    io.to_sql("index_weekly_return", conn=engine_rd, dataframe=df_return, chunksize=5000)
    io.to_sql("index_weekly_risk", conn=engine_rd, dataframe=df_risk, chunksize=5000)
    io.to_sql("index_weekly_subsidiary", conn=engine_rd, dataframe=df_sub, chunksize=5000)
