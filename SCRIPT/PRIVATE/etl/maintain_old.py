from utils.database import config as cfg, io, sqlfactory as sf
from multiprocessing.dummy import Pool as ThreadPool
import pandas as pd


ENGINE = cfg.load_engine()["2Gb"]

DATA_SOURCE = {
    "020001": 3,
    "020002": 4,
    "020003": 5,
    "020004": 2,
    "020005": 12,
    "020007": 13,
    "020008": 1,
    "03": 7,
    "04": 6,
    "05": 9
}

DATA_SOURCE_NAME = {
    "020001": "好买基金",
    "020002": "金斧子",
    "020003": "格上理财",
    "020004": "朝阳永续",
    "020005": "期货资管网",
    "020007": "wind",
    "020008": "私募排排",
    "03": "券商",
    "04": "信托",
    "05": "投顾非公开"
}

SOURCE_CODE = {
    "020001": 3,
    "020002": 3,
    "020003": 3,
    "020004": 3,
    "020005": 3,
    "020007": 3,
    "020008": 3,
    "03": 1,
    "04": 1,
    "05": 4
}

SOURCE = {
    "020001": "第三方",
    "020002": "第三方",
    "020003": "第三方",
    "020004": "第三方",
    "020005": "第三方",
    "020007": "第三方",
    "020008": "第三方",
    "03": "托管机构",
    "04": "托管机构",
    "05": "投顾非公开",
}


def fetch_nvdata(fund_ids):
    fids = sf.SQL.values4sql(fund_ids)
    sql_nv = "SELECT fi.fund_id, fi.fund_name, statistic_date, source_id, nav, added_nav, adjusted_nav as swanav FROM fund_nv_data_source_copy2 fnds " \
             "LEFT JOIN fund_info fi ON fnds.fund_id = fi.fund_id " \
             "WHERE fi.fund_id IN {fids} AND is_used = 1".format(
        fids=fids
    )

    df = pd.read_sql(sql_nv, ENGINE)
    return df


def _tag_data_source(source_id):
    return DATA_SOURCE.get(source_id) or DATA_SOURCE.get(source_id[:2])


def _tag_data_source_name(source_id):
    return DATA_SOURCE_NAME.get(source_id) or DATA_SOURCE_NAME.get(source_id[:2])


def _tag_source(source_id):
    return SOURCE.get(source_id) or SOURCE.get(source_id[:2])


def _tag_source_code(source_id):
    return SOURCE_CODE.get(source_id) or SOURCE_CODE.get(source_id[:2])


def encapsulate_to_old(df):
    if len(df) == 0:
        return df
    df["data_source"] = df["source_id"].apply(lambda x: _tag_data_source(x))
    df["data_source_name"] = df["source_id"].apply(lambda x: _tag_data_source_name(x))
    df["source_code"] = df["source_id"].apply(lambda x: _tag_source_code(x))
    df["source"] = df["source_id"].apply(lambda x: _tag_source(x))
    df = df .rename(columns={"adjusted_nav": "swanav"})
    del df["source_id"]
    df = df.dropna(subset=["data_source"])
    return df


def main():
    df = fetch_nvdata(["JR000001"])
    df = encapsulate_to_old(df)


if __name__ == "__main__":
    main()
