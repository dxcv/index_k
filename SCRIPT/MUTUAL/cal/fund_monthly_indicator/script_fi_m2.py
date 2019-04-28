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
_funcs_risk2 = [
    "ddr3_a", "pain_index", "CVaR", "average_drawdown"
]
_funcs_sub2 = [
    "upsidecap", "downsidecap", "pain_ratio", "ERCVaR", "return_Msqr", "adjusted_jensen_a", "assess_ratio", "sterling_a", "excess_pl", "beta_timing_camp", "corr_spearman"
]
_funcs_sub3 = [
    "hurst",  "burke_a", "kappa_a", "omega", "stock_hm", "timing_hm", "upbeta_cl", "downbeta_cl"
]

_bms_used = ["hs300", "csi500", "sse50", "cbi", "nfi"]

for year in range(2017, 2017 + 1):
    for month in range(9, 9 + 1):
        month_range = cld.monthrange(year, month)[1]

        for day in range(month_range, month_range + 1):
            result_risk2 = []
            result_sub2 = []
            result_sub3 = []

            statistic_date = dt.date(year, month, day)
            ids_used = pre.fetch_fids_used(statistic_date=statistic_date, freq=_freq, conn=engine_rd)
            if len(ids_used) == 0: continue
            print(statistic_date, len(ids_used))
            data = pre.ProcessedData(statistic_date, list(ids_used), _freq, pe=[], conn=engine_rd, conn_mkt=engine_mkt)
            bms = {index_name: cal.Benchmark(attr_dict, index_name) for index_name, attr_dict in data.index.items()}
            tbond = cal.Tbond(data.index["y1_treasury_rate"], "y1_treasury_rate")
            for fid, attrs in data.funds.items():
                fund = cal.Fund(attrs)
                res_risk2, _funcs_return_sorted = cal.calculate(_funcs_risk2, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
                res_sub2, _funcs_risk_sorted = cal.calculate(_funcs_sub2, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
                res_sub3, _funcs_sub_sorted = cal.calculate(_funcs_sub3, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond, with_func_names=True)
                result_risk2.extend(res_risk2)
                result_sub2.extend(res_sub2)
                result_sub3.extend(res_sub3)
            df_risk2 = pd.DataFrame(result_risk2)
            df_sub2 = pd.DataFrame(result_sub2)
            df_sub3 = pd.DataFrame(result_sub3)

            cols_risk2 = cal.format_cols_mutual(_funcs_return_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
            cols_sub2 = cal.format_cols_mutual(_funcs_risk_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
            cols_sub3 = cal.format_cols_mutual(_funcs_sub_sorted, _freq, prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])

            df_risk2.columns = cols_risk2
            df_sub2.columns = cols_sub2
            df_sub3.columns = cols_sub3
            try:
                io.to_sql("fund_monthly_risk2", conn=engine_wt, dataframe=df_risk2, chunksize=500)
                io.to_sql("fund_monthly_subsidiary2", conn=engine_wt, dataframe=df_sub2, chunksize=500)
                io.to_sql("fund_monthly_subsidiary3", conn=engine_wt, dataframe=df_sub3, chunksize=500)
            except Exception as e:
                io.to_sql("fund_monthly_risk2", conn=engine_wt, dataframe=df_risk2, chunksize=500)
                io.to_sql("fund_monthly_subsidiary2", conn=engine_wt, dataframe=df_sub2, chunksize=500)
                io.to_sql("fund_monthly_subsidiary3", conn=engine_wt, dataframe=df_sub3, chunksize=500)

