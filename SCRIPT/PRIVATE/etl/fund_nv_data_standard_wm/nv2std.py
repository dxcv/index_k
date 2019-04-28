import pandas as pd
import datetime as dt
import time
from utils.database import config as cfg
from utils.algorithm import timeutils as tu
from utils.script import scriptutils as su


def friday_of_week(date):
    wday = date.timetuple().tm_wday
    if wday <= 4:
        delta = 4 - wday
    else:
        delta = 11 - wday
    return date + dt.timedelta(delta)


def get_creterias(criterias):
    criteria = criterias[0]
    for x in criterias:
        criteria &= x
    return criteria


def merge_result(result, ids):
    new_result = {
        key: [] for key in ["fund_id", "nav", "added_nav", "swanav", "statistic_date_std", "statistic_date"]
        }
    for i in range(len(ids)):
        tmp_id = [result["fund_id"][i]] * len(result["nav"][i])
        new_result["fund_id"].extend(tmp_id)
        new_result["nav"].extend(result["nav"][i])
        new_result["added_nav"].extend(result["added_nav"][i])
        new_result["swanav"].extend(result["swanav"][i])
        new_result["statistic_date"].extend(result["statistic_date"][i])
        new_result["statistic_date_std"].extend(result["statistic_date_std"][i])
    return new_result

def get_fund_ids():
    sql = "SELECT DISTINCT fund_id FROM fund_nv_data_standard"
    df_ids = pd.read_sql(sql, conn)
    print("Total fund_ids:{}".format(len(df_ids)))
    return df_ids['fund_id'].tolist()

def cal_std(fund_ids=None, whole=False):
    """
    每次计算一个基金
    Args:
        fund_ids: str
        whole:

    Returns:

    """
    if whole is True:
        sql_navs = "SELECT fund_id, nav, added_nav, swanav, statistic_date FROM fund_nv_data_standard"
    else:
        sql_navs = "SELECT fund_id, nav, added_nav, swanav, statistic_date FROM fund_nv_data_standard \
                    WHERE update_time >= {ut}".format(ut=yesterday)

    if fund_ids:
        if 'WHERE' in sql_navs:
            sql_navs += " AND fund_id in ({})".format(fund_ids)
        else:
            sql_navs += " WHERE fund_id in ({})".format(fund_ids)

    su.tic("Fetching nv Data......")
    df_nav = pd.read_sql(sql_navs, conn)

    criterias = [
        (df_nav["nav"] >= 0.2),
        (df_nav["added_nav"] >= 0.2),
        (df_nav["statistic_date"] >= dt.date(1970, 1, 2)),
        (df_nav["statistic_date"] <= dt.date.today())
    ]

    su.tic("Preprocessing......")
    criteria = get_creterias(criterias)
    df_nav = df_nav.loc[criteria].sort_values(["fund_id", "statistic_date"], ascending=[True, False])
    df_nav.index = range(len(df_nav))
    ids = df_nav["fund_id"].drop_duplicates().tolist()

    t_mins = list(df_nav.groupby("fund_id")["statistic_date"].min())
    t_mins_tstp = [time.mktime(x.timetuple()) for x in t_mins]
    t_maxs = list(df_nav.groupby("fund_id")["statistic_date"].max())
    t_maxs_tstp = [time.mktime(x.timetuple()) for x in t_maxs]

    idx4slice = su.idx4slice(df_nav, slice_by="fund_id")
    navs = su.slice(df_nav, idx4slice, "nav")
    added_navs = su.slice(df_nav, idx4slice, "added_nav")
    swanavs = su.slice(df_nav, idx4slice, "swanav")
    t_reals = su.slice(df_nav, idx4slice, "statistic_date")
    t_reals_tstp = []
    for t_real in t_reals:
        t_reals_tstp.append([time.mktime(x.timetuple()) for x in t_real])

    t_std_alls_w = [tu.timeseries_std(friday, tu.periods_in_interval(friday, t_min, 12), extend=4) for t_min in
                    t_mins]  # 标准序列_成立以来
    t_std_alls_w = [t_std_all[:len([x for x in t_std_all if x >= t_min]) + 1] for t_std_all, t_min in
                    zip(t_std_alls_w, t_mins_tstp)]
    t_std_alls_w = [t_std_all[-len([x for x in t_std_all if x < t_max]) - 1:] for t_std_all, t_max in
                    zip(t_std_alls_w, t_maxs_tstp)]

    t_std_alls_m = [
        tu.timeseries_std(date, tu.periods_in_interval(date, t_min, 12), periods_y=12, use_lastday=True, extend=6) for
        date, t_min in zip(t_maxs, t_mins)]  # 标准序列_成立以来
    t_std_alls_m = [t_std_all[:len([x for x in t_std_all if x >= t_min]) + 1] for t_std_all, t_min in
                    zip(t_std_alls_m, t_mins_tstp)]

    su.tic("Matching......")
    matchs_w = [tu.outer_match4indicator_w(t_real, t_std) for t_real, t_std in zip(t_reals_tstp, t_std_alls_w)]
    idx_matchs_w = [x[1] for x in matchs_w]
    nav_matchs_w = [[nav[ix] if ix is not None else None for ix in idx.values()] for nav, idx in
                    zip(navs, idx_matchs_w)]
    anav_matchs_w = [[nav[ix] if ix is not None else None for ix in idx.values()] for nav, idx in
                     zip(added_navs, idx_matchs_w)]
    swanav_matchs_w = [[nav[ix] if ix is not None else None for ix in idx.values()] for nav, idx in
                       zip(swanavs, idx_matchs_w)]
    t_matchs_w = [[t_real[ix] if ix is not None else None for ix in idx.values()] for t_real, idx in
                  zip(t_reals, idx_matchs_w)]
    t_matchs_std_w = [tu.tr(x[:-1], "date") if x is not None else None for x in t_std_alls_w]

    matchs_m = [tu.outer_match4indicator_m(t_real, t_std) for t_real, t_std in zip(t_reals_tstp, t_std_alls_m)]
    idx_matchs_m = [x[1] for x in matchs_m]
    nav_matchs_m = [[nav[ix] if ix is not None else None for ix in idx.values()] for nav, idx in
                    zip(navs, idx_matchs_m)]
    anav_matchs_m = [[nav[ix] if ix is not None else None for ix in idx.values()] for nav, idx in
                     zip(added_navs, idx_matchs_m)]
    swanav_matchs_m = [[nav[ix] if ix is not None else None for ix in idx.values()] for nav, idx in
                       zip(swanavs, idx_matchs_m)]
    t_matchs_m = [[t_real[ix] if ix is not None else None for ix in idx.values()] for t_real, idx in
                  zip(t_reals, idx_matchs_m)]
    t_matchs_std_m = [tu.tr(x[:-1], "date") if x is not None else None for x in t_std_alls_m]

    result_w = {
        "fund_id": ids,
        "nav": nav_matchs_w,
        "added_nav": anav_matchs_w,
        "swanav": swanav_matchs_w,
        "statistic_date": t_matchs_w,
        "statistic_date_std": t_matchs_std_w
    }

    result_m = {
        "fund_id": ids,
        "nav": nav_matchs_m,
        "added_nav": anav_matchs_m,
        "swanav": swanav_matchs_m,
        "statistic_date": t_matchs_m,
        "statistic_date_std": t_matchs_std_m
    }

    su.tic("Merging Result......")
    result = {}
    result["w"] = pd.DataFrame.from_dict(merge_result(result_w, ids))
    result["m"] = pd.DataFrame.from_dict(merge_result(result_m, ids))

    return result


engines = cfg.load_engine()
engine_read = engines["2Gb"]
conn = engine_read.connect()
yesterday = dt.date.today() - dt.timedelta(1)
friday = friday_of_week(yesterday)


def main():
    total_fund_ids = get_fund_ids()

    #一次计算100个基金
    STEP = 100
    sliced = [total_fund_ids[i: i + STEP] for i in range(0, len(total_fund_ids), STEP)]

    index = 1
    for fund_ids in sliced:
        sql_fund_ids = ",".join(["'{}'".format(x) for x in fund_ids])

        result = cal_std(sql_fund_ids, True)

        su.tic("Process:{}/{}".format(index, len(sliced)))
        index += 1

        su.tic("Data to DB...month..{}.".format(sql_fund_ids))
        conn.execute("DELETE FROM fund_nv_standard_m where fund_id in ({})".format(sql_fund_ids))
        result["m"].to_sql("fund_nv_standard_m", conn, if_exists="append", index=False, chunksize=10000)

        su.tic("Data to DB...week..{}.".format(','.join(fund_ids)))
        conn.execute("DELETE FROM fund_nv_standard_w where fund_id in ({})".format(sql_fund_ids))
        result["w"].to_sql("fund_nv_standard_w", conn, if_exists="append", index=False, chunksize=10000)


if __name__ == "__main__":
    su.tic("nv2std...")
    main()
    su.tic("Done...")
