import numpy as np
import pandas as pd
from utils.database import io, config as cfg
from utils.script.scriptutils import tic

# 解决fund_nv_data_source表中data_source == 2的数据，added_nav为空的情况

engines = cfg.load_engine()
engine_rd = engines["2Gb"]
engine_wt = engines["2Gb"]


def check_equal(dataframe):
    dataframe = dataframe.dropna(subset=["nav", "added_nav"])
    is_equivalent = list(dataframe["nav"] == dataframe["added_nav"])
    if len(is_equivalent) >= 1:
        if False in is_equivalent:
            first_unequal = is_equivalent.index(False)
        else:
            first_unequal = dataframe.index[-1] + 1
    else:
        return 0
    return first_unequal


def main():
    sql_tgt = "SELECT fund_id, statistic_date, nav, added_nav, swanav1 FROM fund_nv_data_source WHERE data_source = 2 AND \
               fund_id IN (SELECT DISTINCT fund_id FROM fund_nv_updata_source WHERE data_source = 2)\
               ORDER BY fund_id ASC, statistic_date ASC"
    df = pd.read_sql(sql_tgt, engine_rd)
    ids = df["fund_id"].drop_duplicates().tolist()
    df.index = df["fund_id"].tolist()

    # 1 IF swanav1 == nav, THEN added_nav = nav
    df.loc[df["nav"] == df["swanav1"], "added_nav"] = df.loc[df["nav"] == df["swanav1"], "nav"]

    # 2 IF swanav1和added_nav从最早日期没值, THEN added_nav = nav
    tgt = []
    for idx in ids:
        tmp = df.ix[idx]
        if type(tmp) is pd.DataFrame:
            if all(np.isnan(tmp["swanav1"])) and all(np.isnan(tmp["added_nav"])):
                tgt.append(idx)
        elif type(tmp) is pd.Series:
            if np.isnan(tmp["swanav1"]) and np.isnan(tmp["added_nav"]):
                tgt.append(idx)
    if len(tgt) > 0:
        df.ix[tgt, "added_nav"] = df.ix[tgt, "nav"]

    unpro = set(df.loc[np.isnan(df.added_nav)].index)
    df_p1 = df.loc[df["fund_id"].apply(lambda x: x not in unpro)]

    df_p2 = df.loc[df["fund_id"].apply(lambda x: x in unpro)]
    ids = df_p2["fund_id"].drop_duplicates().tolist()

    # 3 IF added_nav前后出现并且缺失片段前后的nav == added_nav, 中间缺失, THEN 中间缺失的added_nav = nav
    # df_p2_cp = df_p2.copy()
    # df_p2_cp.index = range(len(df_p2_cp))
    for idx in ids:
        # print(idx)
        tmp = df_p2.ix[idx]
        if type(tmp) is pd.DataFrame:
            tmp.index = range(len(tmp))
            fue = check_equal(tmp)
            if fue >= 1:
                df_p2.ix[idx].ix[:fue, "added_nav"] = df_p2.ix[idx].ix[:fue, "nav"]
            else:
                continue
        else:
            print(idx)
            continue

    result = df_p1.append(df_p2).sort_values(by=["fund_id", "statistic_date"], ascending=[True, True])
    ids_error = set(result.loc[(result.nav == result.added_nav) & (result.nav != result.swanav1) & (
        ~np.isnan(result.swanav1))].fund_id.drop_duplicates().tolist())

    result = result.loc[result.fund_id.apply(lambda x: x not in ids_error)]
    result["data_source"] = 2
    result.index = range(len(result))
    io.to_sql("fund_nv_data_source", engine_wt, result[["fund_id", "statistic_date", "data_source", "added_nav"]], type="update")
    return result


if __name__ == "__main__":
    tic("nv_data_source...")
    main()
    tic("Done...")
