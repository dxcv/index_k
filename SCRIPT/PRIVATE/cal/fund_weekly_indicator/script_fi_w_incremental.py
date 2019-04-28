import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
from utils.algorithm import preprocess as pre, calculation as cal
from utils.database import config as cfg, io
from utils.dev import calargs
from sqlalchemy import create_engine
import time
import sys

UPDATE_TIME = dt.datetime.now()

engines = cfg.load_engine()
engine_rd = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@10.66.175.186:3306/base", connect_args={"charset": "utf8"})
# engine_rd = cfg.load_engine()["2Gb"]

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


def calculate(time_s, time_e):
    try:
        tasks = pre.generate_tasks(time_s, time_e, freq="w", processes=7, conn=engine_rd)
        tasks = {k: v for k, v in tasks.items() if k >= dt.date(2015, 1, 1)}
        print(time_e, len(tasks))

    except ValueError as e:
        print(time_e, e)
    for statistic_date, ids_used in sorted(tasks.items(), key=lambda x: x[0]):
        print("UPDATE TIME:{ut}: STATISTIC DATE:{sd}, LENGTH:{l}".format(ut=time_e, sd=statistic_date, l=len(ids_used)))
        result_return = []
        result_risk = []
        result_sub = []
        data = pre.ProcessedData(statistic_date, list(ids_used), _freq)
        bms = {index_name: cal.Benchmark(attr_dict, index_name) for index_name, attr_dict in data.index.items()}
        tbond = cal.Tbond(data.index["y1_treasury_rate"], "y1_treasury_rate")
        for fid, attrs in data.funds.items():
            fund = cal.Fund(attrs)
            res_return, cols_return_sorted = cal.calculate(_funcs_return, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
            res_risk, cols_risk_sorted = cal.calculate(_funcs_risk, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
            res_sub, cols_sub_sorted = cal.calculate(_funcs_sub, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
            result_return.extend(res_return)
            result_risk.extend(res_risk)
            result_sub.extend(res_sub)

        df_return = pd.DataFrame(result_return)
        df_risk = pd.DataFrame(result_risk)
        df_sub = pd.DataFrame(result_sub)

        cols_return = cal.format_cols(
            cols_return_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"]
        )
        cols_risk = cal.format_cols(
            cols_risk_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"]
        )
        cols_sub = cal.format_cols(
            cols_sub_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"]
        )

        df_return.columns = cols_return
        df_risk.columns = cols_risk
        df_sub.columns = cols_sub

        try:
            io.to_sql("fund_weekly_return", conn=engine_rd, dataframe=df_return, chunksize=5000)
            io.to_sql("fund_weekly_risk", conn=engine_rd, dataframe=df_risk, chunksize=5000)
            io.to_sql("fund_subsidiary_weekly_index", conn=engine_rd, dataframe=df_sub, chunksize=5000)
        except Exception as e:
            time.sleep(10)
            io.to_sql("fund_weekly_return", conn=engine_rd, dataframe=df_return, chunksize=5000)
            io.to_sql("fund_weekly_risk", conn=engine_rd, dataframe=df_risk, chunksize=5000)
            io.to_sql("fund_subsidiary_weekly_index", conn=engine_rd, dataframe=df_sub, chunksize=5000)

    print("TASK DONE: {ut}".format(ut=time_e))


def main():
    mod = int(sys.argv[1])
    time_s, time_e = UPDATE_TIME - relativedelta(minutes=60 * mod + 5), UPDATE_TIME - relativedelta(minutes=60 * (mod-1))
    calculate(time_s, time_e)


if __name__ == "__main__":
    main()
