import datetime as dt
import pandas as pd
from utils.database import io, config as cfg

engine_rd = cfg.load_engine()["2Gb"]
engine_wt = cfg.load_engine()["2Gb"]

yesterday = dt.date.today() - dt.timedelta(1)
sql_fi = "SELECT fund_id FROM fund_info"
sql_fom = "SELECT fund_id, org_name, org_type_code FROM fund_org_mapping WHERE org_type_code IN (1, 2)"


def reduce_dup(df):
    df_tmp = df.copy()
    df_tmp.index = df_tmp["fund_id"]
    grouped = df.groupby("fund_id")["fund_id"]
    grouped_cnt = grouped.count()
    idxs = df_tmp.drop_duplicates(subset=["fund_id"]).index[grouped_cnt >= 2]
    return df_tmp.ix[idxs]


def gen_dict(df):
    dict_dup = {}

    df_2 = reduce_dup(df)
    for idx in df_2.index:
        tmp = str(df_2.ix[idx]["org_name"].tolist())[1:-1]
        tmp = tmp.replace("'", "")
        dict_dup[idx] = tmp

    df_1 = df.loc[df["fund_id"].apply(lambda x: x not in set(df_2.index))]
    dict_undup = dict(zip(df_1["fund_id"], df_1["org_name"]))

    result = {}
    result.update(dict_dup)
    result.update(dict_undup)
    return result


def main():
    print("fetching get_data...")
    fom = pd.read_sql(sql_fom, engine_rd)
    fi = pd.read_sql(sql_fi, engine_rd)

    print("processing...")
    fi_org = fom.loc[fom["org_type_code"] == 1, ["fund_id", "org_name"]]
    fi_mng = fom.loc[fom["org_type_code"] == 2, ["fund_id", "org_name"]]

    org_dict, mng_dict = list(map(lambda x: gen_dict(x), [fi_org, fi_mng]))
    fi["fund_consultant"] = fi["fund_id"].apply(lambda x: org_dict.get(x))
    fi["fund_manager_nominal"] = fi["fund_id"].apply(lambda x: mng_dict.get(x))

    result = fi
    print("get_data to db...")

    io.to_sql("fund_info", engine_wt, result, "update")
    print("done...")


if __name__ == "__main__":
    main()
