import calendar as cld
import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
from utils.database import config as cfg, io, sqlfactory as sf


ENGINE = cfg.load_engine()["2Gb"]


def main():

    TODAY = dt.date.today() - dt.timedelta(10)
    FDATE_F = TODAY - relativedelta(months=1)
    FDATE_O = TODAY - relativedelta(months=3)

    # 取出所有有净值更新的产品
    sql_fid_upt = "\
    SELECT DISTINCT fund_id FROM fund_nv_data_standard \
    WHERE update_time BETWEEN '{last_time}' AND '{today}'".format(
        today=TODAY, last_time=TODAY - dt.timedelta(2)
    )
    fids = pd.read_sql(sql_fid_upt, ENGINE)["fund_id"].tolist()
    if len(fids) > 0:
        fids = sf.SQL.ids4sql(fids)
    else:
        return None
    # # 将所有存在任一产品更新的投顾及其旗下产品取出(并且要满足投顾成立满3个月, 产品成立满1个月)(TB1)
    # sql_fom = "SELECT fom.org_id, fom.fund_id, oi.found_date, oi.org_name FROM fund_org_mapping fom \
    #            JOIN org_info oi ON fom.org_id = oi.org_id \
    #            JOIN fund_info fi ON fom.fund_id = fi.fund_id \
    #            WHERE fom.org_type_code = 1 AND oi.found_date <= '{o_fd}' AND fi.foundation_date <= '{f_fd}' \
    #            AND fom.fund_id IN {fid}".format(
    #     o_fd=FDATE_O, f_fd=FDATE_F, fid=fids
    # )
    # 将所有存在任一产品更新的投顾及其旗下产品取出(并且要满足投顾成立满3个月, 产品成立满1个月)(TB1)
    sql_fom = "SELECT fom.org_id, fom.fund_id  FROM fund_org_mapping fom \
               JOIN org_info oi ON fom.org_id = oi.org_id \
               JOIN fund_info fi ON fom.fund_id = fi.fund_id \
               WHERE fom.org_type_code = 1 AND oi.found_date <= '{o_fd}' AND fi.foundation_date <= '{f_fd}' \
               ".format(
        o_fd=FDATE_O, f_fd=FDATE_F, fid=fids
    )
    df_fom = pd.read_sql(sql_fom, ENGINE)

    # 取出所有产品的最早日期(TB2)
    sql_firstdate = "SELECT fund_id, MIN(statistic_date) as fdate FROM fund_nv_data_standard GROUP BY fund_id"
    df_firstdate = pd.read_sql(sql_firstdate, ENGINE)

    # 将TB1投顾和产品, 以及TB2产品和最早日期两表merge在一起, 排序去空
    result = df_fom.merge(df_firstdate, on="fund_id", how="left").sort_values(["org_id", "fdate"], ascending=[True, True]).drop_duplicates(subset=["org_id"])[["org_id", "fdate"]].dropna()
    result.columns = ["org_id", "base_date"]

    # 将最早日期对齐至月末作为指数基期
    result["base_date"] = result["base_date"].apply(lambda x: dt.date(x.year, x.month, cld.monthrange(x.year, x.month)[1]) if x is not None else None)

    return result

if __name__ == "__main__":
    result = main()
    if result is not None:
        io.to_sql("org_info", ENGINE, result)
