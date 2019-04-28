from utils.database import io, config as cfg
import pandas as pd
from sqlalchemy import create_engine

'''
原data source
0-私募云通; 1-私募排排; 2-朝阳永续; 3-好买基金; 
4-金斧子; 5-格上理财; 6-信托; 7-券商; 
8-投顾公开; 9-投顾非公开; 10-公募专户; 11-期货; 
12-期货资管网; 13-wind
'''

'''
id_match: 原match type
数据源类型: {1-私募排排; 2-期货公司; 3-好买基金; 4-朝阳永续; 
5-基金业协会; 6-格上理财; 7-金斧子; 8-信托1(LY); 9-信托(os); 
10-公募子公司; 11-券商公司; 12-期货资管网; 13-wind(unknown); 
14-投顾公司; 111-券商净值; 15-券商托管}
'''

SOURCES = {
    0: None,  # 云通源, 2017.3.14, 4
    1: "020008",
    2: None,  # 原wind,11896, 改为期货公司
    3: "020001",
    4: "020004",
    5: "010001",
    6: "020003",
    7: "020002",
    8: "04",  # 自采
    9: "04",  # 外包
    10: None,  # 2017.08.21, 158 (f_fund_nv_data)
    11: "03",  # 2017.05.08, 1767
    12: "020005",
    13: "020007",  # 2017.11.13, 0(由期货公司改为wind)
    14: "05",  # 2017.10.25, 185
    111: "03", # 2017.08.29, 5598 (s_fund_nv_data)
    15: "03",  # 2017.05.15, 38
}


# eg_2Gcfgp = create_engine("mysql+pymysql://root:smyt0317@182.254.128.241:4171/config_private?charset=utf8")


def rematch_fid():
    # 将fund_id_match中配对过的记录中的match_type重新映射为统一编码;
    eg_2Gb = cfg.load_engine()["2Gb"]

    df = pd.read_sql("SELECT fund_ID, source_ID, match_type FROM fund_id_match", eg_2Gb)
    df["match_type"] = df["match_type"].apply(lambda x: SOURCES.get(x))
    df["id_type"] = 1
    df = df.rename_axis({"source_ID": "source_id", "fund_ID": "matched_id", "match_type": "source"}, axis=1)
    df = df.dropna(subset=["matched_id", "source_id", "source"], how="any")
    io.to_sql("id_match", eg_2Gb, df)


def rematch_oid():
    eg_2Gb = cfg.load_engine()["2Gb"]

    df = pd.read_sql("SELECT org_ID, source_org_ID, match_type FROM org_id_match", eg_2Gb)
    df["match_type"] = df["match_type"].apply(lambda x: SOURCES.get(x))
    df["id_type"] = 2
    df = df.rename_axis({"source_org_ID": "source_id", "org_ID": "matched_id", "match_type": "source"}, axis=1)
    df = df.dropna(subset=["matched_id", "source_id", "source"], how="any")
    io.to_sql("id_match", eg_2Gb, df)


def rematch_pid():
    eg_2Gb = cfg.load_engine()["2Gb"]

    df = pd.read_sql("SELECT manager_ID, source_manager_ID, match_type FROM manager_id_match", eg_2Gb)
    df["match_type"] = df["match_type"].apply(lambda x: SOURCES.get(x))
    df["id_type"] = 3
    df = df.rename_axis({"source_manager_ID": "source_id", "manager_ID": "matched_id", "match_type": "source"}, axis=1)
    df = df.dropna(subset=["matched_id", "source_id", "source"], how="any")
    io.to_sql("id_match", eg_2Gb, df)


def main():
    rematch_fid()
    rematch_oid()
    rematch_pid()


if __name__ == "__main__":
    main()
