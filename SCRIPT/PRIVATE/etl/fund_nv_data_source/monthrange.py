import pandas as pd
from utils.database import config as cfg, io
import calendar as cld
import datetime as dt

engines = cfg.load_engine()
engine_rd = engines["2Gb"]
engine_wt = engines["2Gb"]


def get_result():
    sql_sd = "SELECT fund_id, statistic_date, statistic_date_m FROM fund_nv_data_standard"
    df = pd.read_sql(sql_sd, engine_rd)

    df_original = df.copy()
    df["ym"] = df["statistic_date"].apply(lambda x: (x.year, x.month))

    df = df.sort_values(by=["fund_id", "statistic_date"], ascending=[True, False])

    tmp = df.drop_duplicates(subset=["fund_id", "ym"])
    tmp["statistic_date_m"] = tmp["statistic_date"].apply(
        lambda x: dt.date(x.year, x.month, cld.monthrange(x.year, x.month)[1]))
    tmp = tmp[["fund_id", "statistic_date", "statistic_date_m"]]

    q = df_original.merge(tmp, how="left", on=["fund_id", "statistic_date"], suffixes=["ori", ""])
    q = q.dropna(axis=0, subset=q.columns[-2:], how="all")
    result = q[["fund_id", "statistic_date", "statistic_date_m"]]
    return result


def main():
    result = get_result()
    io.to_sql("fund_nv_data_standard", engine_wt, result, "update")


if __name__ == "__main__":
    main()
