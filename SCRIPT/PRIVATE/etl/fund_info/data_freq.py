from utils.database.config import load_engine
from utils.database import sqlfactory as sf
from utils.script import scriptutils as su
from utils.database import io
import pandas as pd
import time


engines = load_engine()
engine_rd = engines["2Gb"]
engine_wt = engines["2Gb"]


def get_original(ids_to_update):
    sql_fi = "SELECT fund_id, data_freq FROM fund_info WHERE fund_id IN {ids}".format(
        ids=sf.SQL.ids4sql(ids_to_update)
    )
    fi = pd.read_sql(sql_fi, engine_rd)
    return fi


def is_daily(ids_to_update):
    sql_fnds = "SELECT fund_id, statistic_date FROM fund_nv_data_standard WHERE statistic_date >= '19700101' \
                AND fund_id IN {ids}".format(ids=sf.SQL.ids4sql(ids_to_update))
    su.tic("Fetching get_data......")
    fnds = pd.read_sql(sql_fnds, engine_rd)
    fnds = fnds.dropna().sort_values(by=["fund_id", "statistic_date"], ascending=[True, False])
    fnds.index = range(len(fnds))
    fnds["statistic_date"] = fnds["statistic_date"].apply(lambda x: time.mktime(x.timetuple()))

    su.tic("Calculating......")
    ids4slice = su.idx4slice(fnds, slice_by="fund_id")
    ids = fnds.drop_duplicates(subset=["fund_id"])["fund_id"].tolist()
    t_reals = su.slice(fnds, ids4slice, "statistic_date")

    match_ratio = [(len(t_real) - 1) / ((t_real[0] - t_real[-1]) / 86400) if len(t_real) > 1 else None for t_real in
                   t_reals]
    matched = ["日度" if (x is not None and x >= 0.5) else None for x in match_ratio]
    result = dict([x for x in list(zip(ids, matched)) if x[1] is not None])
    return result


def is_weekly(ids_to_update):
    sql_std_w = "SELECT fund_id, statistic_date_std, statistic_date FROM fund_nv_standard_w \
                WHERE fund_id IN {ids} \
                AND fund_id IN (SELECT fund_id FROM (SELECT fund_id, COUNT(fund_id) cnt FROM fund_nv_standard_w \
                GROUP  BY fund_id HAVING cnt >= 3) T)".format(ids=sf.SQL.ids4sql(ids_to_update))
    su.tic("Fetching get_data......")
    d_std_w = pd.read_sql(sql_std_w, engine_rd)

    ids4slice = su.idx4slice(d_std_w, slice_by="fund_id")
    ids = d_std_w.drop_duplicates(subset=["fund_id"])["fund_id"].tolist()
    t_reals = su.slice(d_std_w, ids4slice, "statistic_date")
    t_stds = su.slice(d_std_w, ids4slice, "statistic_date_std")
    match_ratio = [len([x for x in t_real if x is not None]) / len(t_std) for t_real, t_std in zip(t_reals, t_stds)]
    matched = ["周度" if (x is not None and x >= 0.5) else None for x in match_ratio]
    result = dict([x for x in list(zip(ids, matched)) if x[1] is not None])
    return result


def is_monthly(ids_to_update):
    su.tic("Fetching get_data......")
    sql_std_m = "SELECT fund_id, statistic_date_std, statistic_date FROM fund_nv_standard_m \
                 WHERE fund_id IN {ids} \
                 AND fund_id IN (SELECT fund_id FROM (SELECT fund_id, COUNT(fund_id) cnt FROM fund_nv_standard_m \
                 GROUP BY fund_id HAVING cnt >= 3) T)".format(ids=sf.SQL.ids4sql(ids_to_update))
    d_std_m = pd.read_sql(sql_std_m, engine_rd)

    ids4slice = su.idx4slice(d_std_m, slice_by="fund_id")
    ids = d_std_m.drop_duplicates(subset=["fund_id"])["fund_id"].tolist()
    t_reals = su.slice(d_std_m, ids4slice, "statistic_date")
    t_stds = su.slice(d_std_m, ids4slice, "statistic_date_std")
    match_ratio = [len([x for x in t_real if x is not None]) / len(t_std) for t_real, t_std in zip(t_reals, t_stds)]
    matched = ["月度" if (x is not None and x >= 0.5) else None for x in match_ratio]
    result = dict([x for x in list(zip(ids, matched)) if x[1] is not None])
    return result


def main():
    sql_ids_used = "SELECT DISTINCT fund_id FROM fund_info \
    WHERE fund_id IN (SELECT fund_id FROM (SELECT fund_id, COUNT(fund_id) cnt FROM fund_nv_data_standard \
    GROUP BY fund_id HAVING cnt >= 3) as T)"

    ids_to_update = [x[0] for x in engine_rd.execute(sql_ids_used).fetchall()]

    fi = get_original(ids_to_update)

    result_m = is_monthly(ids_to_update)
    result_w = is_weekly(ids_to_update)
    result_d = is_daily(ids_to_update)

    result = result_m.copy()
    result.update(result_w)
    result.update(result_d)

    fi["data_freq2"] = [result[id_] if id_ in result.keys() else None for id_ in fi["fund_id"].tolist()]
    fi["data_freq2"] = fi["data_freq2"].fillna("其他")
    fi["data_freq"] = fi["data_freq2"]

    io.to_sql("fund_info", engine_wt, fi[["fund_id", "data_freq"]], "update")


if __name__ == "__main__":
    main()


