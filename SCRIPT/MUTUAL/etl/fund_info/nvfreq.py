import datetime as dt
import pandas as pd
from multiprocessing import Pool
from utils.database import config as cfg, io
from utils.sqlfactory.constructor import sqlfmt
from utils.timeutils.const import bday_chn


engine = cfg.load_engine()["2Gb"]


def get_nv_data(fund_ids):
    sql = "SELECT fi.fund_id, IFNULL(fn.statistic_date, fy.statistic_date) statistic_date " \
          "FROM base_public.fund_info fi " \
          "LEFT JOIN base_public.fund_nv fn ON fi.fund_id = fn.fund_id " \
          "LEFT JOIN base_public.fund_yield fy ON fi.fund_id = fy.fund_id " \
          "WHERE fi.fund_id IN ({fids})".format(fids=sqlfmt(fund_ids))
    df = pd.read_sql(sql, engine).dropna()
    df["statistic_date"] = df["statistic_date"].apply(lambda x: dt.datetime(x.year, x.month, x.day))
    return df


def weighted_score(xdata):
    l = len(xdata)
    res = []
    for idx, x in enumerate(xdata, 1):

        if idx / l <= 0.5:
            weighted_x = x * 0.2
        elif 0.5 < idx / l <= 0.8:
            weighted_x = x * 0.5
        elif idx / l > 0.8:
            weighted_x = x * 0.8
        res.append(weighted_x)

    tot = 0.5 * l * 0.2 + 0.3 * l * 0.5 + 0.2 * l * 0.8  # N * v_i * w_i
    s = sum(res)

    return s / tot


def juege_freq(date_series, freq):
    res = {}
    if freq == "d":
        rule = bday_chn
    elif freq == "w":
        rule = "W-FRI"
    else:
        raise NotImplementedError

    for fid, ts in zip(date_series.index, date_series.values.tolist()):
        ts_std = pd.Series(1, index=ts).resample(rule=rule).last().fillna(0)
        xdata = list(ts_std)
        score = weighted_score(xdata)
        res[fid] = score
    return pd.DataFrame.from_dict(res, orient="index")


def judge(chunk):
    THRESHOLD = .75
    df = get_nv_data(chunk)
    date_series = df.groupby("fund_id")["statistic_date"].apply(lambda x: list(x))

    overall = set(chunk)
    whole = set(date_series.index)

    df_d = juege_freq(date_series.dropna(), "d")
    res_d = set(df_d[df_d > THRESHOLD].dropna().index)

    # Passing list-likes to .loc or [] with any missing label will raise
    # KeyError in the future, you can use .reindex() as an alternative.
    # df_w = juege_freq(date_series[list(set(chunk) - set(res_d))].dropna(), "w")

    df_w = juege_freq(date_series.reindex(list(set(chunk) - set(res_d))).dropna(), "w")
    res_w = set(df_w[df_w > THRESHOLD].dropna().index)

    res_others = whole - res_d - res_w

    date_series[list(res_d)] = "日度"
    date_series[list(res_w)] = "周度"
    date_series[list(res_others)] = "其它"
    res_withoutdata = pd.Series("其它", index=pd.Index(overall - whole - res_d - res_w, name="fund_id"))

    res = date_series.append(res_withoutdata).reset_index()
    res.columns = ["fund_id", "nv_freq"]
    return res


def save(dataframe):
    io.to_sql("base_public.fund_info", engine, dataframe)


def main():
    STEP = 20
    pool = Pool(4)

    fids_all = sorted(pd.read_sql("SELECT fund_id FROM base_public.fund_info fi", engine)["fund_id"].tolist())
    [pool.apply_async(judge, args=(chunk,), callback=save)
     for chunk in [fids_all[i: i + STEP] for i in range(0, len(fids_all), STEP)]]
    pool.close()
    pool.join()


def test():
    from utils.etlkit.ext.tools import TableComparer
    import os
    import datetime as dt

    tc = TableComparer(
        "base_test.fund_info_mutual", "base_public.fund_info", engine,
        cols_included={"nv_freq"}
    )
    tc.result.to_excel(os.path.expanduser("~/Desktop/nv_freq_{t}.xlsx".format(t=dt.date.today().strftime("%Y%m%d"))))


if __name__ == "__main__":
    main()
