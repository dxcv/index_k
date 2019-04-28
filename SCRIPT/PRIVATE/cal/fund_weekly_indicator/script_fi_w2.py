import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
from utils.algorithm import preprocess as pre, calculation as cal
from utils.database import config as cfg, io
from utils.dev import calargs
from sqlalchemy import create_engine
import calendar as cld

UPDATE_TIME = dt.datetime.now()

engines = cfg.load_engine()
# engine_rd = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@10.66.175.186:3306/base", connect_args={"charset": "utf8"})
engine_rd = engines["2Gb"]
_freq = "w"
_intervals = calargs.intervals
_funcs_risk2 = [
    "ddr3_a", "pain_index", "CVaR", "average_drawdown"
]
_funcs_sub2 = [
    "upsidecap", "downsidecap", "pain_ratio", "ERCVaR", "return_Msqr", "adjusted_jensen_a", "assess_ratio", "sterling_a", "excess_pl", "beta_timing_camp", "corr_spearman"
]
_funcs_sub3 = [
    "hurst",  "burke_a", "kappa_a", "omega", "stock_hm", "timing_hm", "upbeta_cl", "downbeta_cl"
]


_bms_used = ["hs300", "csi500", "sse50", "cbi", "strategy", "FI01", "nfi"]


for year in range(2015, 2016):
    for month in range(1, 2):
        month_range = cld.monthrange(year, month)[1]
        for day in range(19, month_range + 1):
            result_risk2 = []
            result_sub2 = []
            result_sub3 = []

            statistic_date = dt.date(year, month, day)
            ids_used = pre.fetch_fids_used(statistic_date=statistic_date, freq=_freq, conn=engine_rd)
            print(statistic_date, len(ids_used))

            data = pre.ProcessedData(statistic_date, ids_used, _freq)
            bms = {index_name: cal.Benchmark(attr_dict, index_name) for index_name, attr_dict in data.index.items()}
            tbond = cal.Tbond(data.index["y1_treasury_rate"], "y1_treasury_rate")
            for fid, attrs in data.funds.items():
                fund = cal.Fund(attrs)
                res_risk2, funcs_risk2_sorted = cal.calculate(_funcs_risk2, _intervals, _bms_used, _freq, statistic_date, fund,
                                                              bms, tbond, with_func_names=True)
                res_sub2, funcs_sub2_sorted = cal.calculate(_funcs_sub2, _intervals, _bms_used, _freq, statistic_date, fund,
                                                            bms, tbond, with_func_names=True)
                res_sub3, funcs_sub3_sorted = cal.calculate(_funcs_sub3, _intervals, _bms_used, _freq, statistic_date, fund,
                                                            bms, tbond, with_func_names=True)
                result_risk2.extend(res_risk2)
                result_sub2.extend(res_sub2)
                result_sub3.extend(res_sub3)

            df_risk2 = pd.DataFrame(result_risk2)
            df_sub2 = pd.DataFrame(result_sub2)
            df_sub3 = pd.DataFrame(result_sub3)

            cols_risk2 = cal.format_cols_private(
                funcs_risk2_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"]
            )
            cols_sub2 = cal.format_cols_private(
                funcs_sub2_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"]
            )
            cols_sub3 = cal.format_cols_private(
                funcs_sub3_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"]
            )

            df_risk2.columns = cols_risk2
            df_sub2.columns = cols_sub2
            df_sub3.columns = cols_sub3
            try:
                io.to_sql("fund_weekly_risk2", conn=engine_rd, dataframe=df_risk2, chunksize=500)
                io.to_sql("fund_weekly_subsidiary2", conn=engine_rd, dataframe=df_sub2, chunksize=500)
                io.to_sql("fund_weekly_subsidiary3", conn=engine_rd, dataframe=df_sub3, chunksize=500)
            except Exception as e:
                engine_rd = engines["2Gb"]
                c = engine_rd.connect()
                c.close()

                io.to_sql("fund_weekly_risk2", conn=engine_rd, dataframe=df_risk2, chunksize=500)
                io.to_sql("fund_weekly_subsidiary2", conn=engine_rd, dataframe=df_sub2, chunksize=500)
                io.to_sql("fund_weekly_subsidiary3", conn=engine_rd, dataframe=df_sub3, chunksize=500)
