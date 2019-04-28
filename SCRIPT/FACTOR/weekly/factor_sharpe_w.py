import calendar as cld
import datetime as dt
from importlib import reload

import pandas as pd

from utils.algorithm.sharpe import factors_sharpe_pri as fs
from utils.database import config as cfg, io

reload(fs)
from multiprocessing import Pool

ENGINE = cfg.load_engine()["2Gb"]


def fetch_fund(date, factor_type):
    y, m = date.year, date.month
    start, end = dt.date(y, m, 1), dt.date(y, m, cld.monthrange(y, m)[1])
    if factor_type == "style":
        sql = "SELECT DISTINCT fund_id " \
              "FROM fund_nv_data_standard " \
              "WHERE statistic_date BETWEEN '{s}' AND '{e}' " \
              "AND swanav IS NOT NULL " \
              "AND statistic_date IS NOT NULL".format(s=start, e=end)
    elif factor_type == "industry":
        sql = "SELECT DISTINCT ftm.fund_id " \
              "FROM (SELECT fund_id FROM base.fund_type_mapping WHERE stype_code IN ('6010101', '6010102', '6010103', '6010401', '6010402', '6010403', '6010404') AND flag = 1) ftm " \
              "JOIN fund_nv_data_standard fnds ON ftm.fund_id = fnds.fund_id " \
              "WHERE statistic_date BETWEEN '{s}' AND '{e}' " \
              "AND swanav IS NOT NULL " \
              "AND statistic_date IS NOT NULL".format(s=start, e=end)

    df = pd.read_sql(sql, ENGINE)
    return df["fund_id"].tolist()


def calculate(fid, date, factor_type):
    try:
        print(fid)
        res = fs.PriSharpeFactor(fid, start=None, end=date, freq="w", factor_type=factor_type)
        rp = fs.ResultProxy(res)

        return factor_type, rp.frame_table1, rp.frame_table2

    except fs.DataError as err:
        print(err)
        return None


def save_to_db(frames):
    if frames is not None:
        ft = frames[0]
        if ft == "style":
            io.to_sql("base.fund_sharpe_regr_w", ENGINE, frames[1])
            io.to_sql("base.fund_sharpe_contri_w", ENGINE, frames[2])
        elif ft == "industry":
            io.to_sql("base.sharpe_industry_regr_w", ENGINE, frames[1])
            io.to_sql("base.sharpe_industry_contri_w", ENGINE, frames[2])


def main():
    p = Pool(6)
    factor_type = "style"
    dr = pd.date_range(dt.date(2007, 1, 1), dt.date(2015, 12, 31), freq="M")[::-1]
    for d in dr:
        print(d)
        fids = fetch_fund(d, factor_type)
        [p.apply_async(calculate, args=(fid, d.date(), factor_type), callback=save_to_db) for fid in fids]
    p.close()
    p.join()


if __name__ == "__main__":
    main()
