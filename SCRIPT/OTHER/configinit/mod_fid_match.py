from utils.database import io, config as cfg
import pandas as pd
from sqlalchemy import create_engine

'''
0-私募云通; 1-私募排排; 2-朝阳永续; 3-好买基金; 
4-金斧子; 5-格上理财; 6-信托; 7-券商; 
8-投顾公开; 9-投顾非公开; 10-公募专户; 11-期货; 
12-期货资管网; 13-wind
'''

'''
数据源类型: {1-私募排排; 2-wind; 3-好买基金; 4-朝阳永续; 5-基金业协会; 6-格上理财; 7-金斧子; 8-信托1; 9-信托; 10-公募子公司; 11-券商公司; 12-期货资管网; 13-期货公司; 14-投顾公司; 111-券商净值; 15-券商托管}
'''

SOURCES = {
    0: "000001",
    1: "020008",
    2: "020004",
    3: "020001",
    4: "020002",
    5: "020003",
    6: "04",
    7: "03",
    8: "05",
    9: "05",
    10: "05",
    11: None,
    12: "020005",
    13: "020007",
}


def rematch_source_used():
    eg_2Gb = cfg.load_engine()["2Gb"]
    eg_2Gcfgp = create_engine("mysql+pymysql://root:smyt0317@182.254.128.241:4171/config_private?charset=utf8")

    df = pd.read_sql("SELECT * FROM fund_nv_updata_source WHERE is_updata = 1", eg_2Gb)
    tmp = df.copy(False)
    tmp = tmp.loc[tmp["fund_id"].apply(lambda x: x[:2] == "JR")]
    tmp["source_id"] = tmp["data_source"].apply(lambda x: SOURCES.get(x))
    tmp["is_granted"] = tmp["data_source"].apply(lambda x: {9: 1}.get(x, 0))
    tmp["pk"] = tmp["fund_id"]
    tmp["target_table"] = "fund_nv_data_standard"
    tmp = tmp[["target_table", "pk", "source_id"]]
    tmp = tmp.dropna()

    io.to_sql("sync_source", eg_2Gcfgp, tmp)


def rematch_source_not_used():
    eg_2Gb = cfg.load_engine()["2Gb"]
    eg_2Gcfgp = create_engine("mysql+pymysql://root:smyt0317@182.254.128.241:4171/config_private?charset=utf8")

    df = pd.read_sql("SELECT * FROM fund_nv_updata_source WHERE is_updata = 0", eg_2Gb)
    tmp = df.copy(False)
    tmp = tmp.loc[tmp["fund_id"].apply(lambda x: x[:2] == "JR")]
    tmp["source_id"] = tmp["data_source"].apply(lambda x: SOURCES.get(x))
    tmp["is_granted"] = tmp["data_source"].apply(lambda x: {9: 1}.get(x, 0))
    tmp["pk"] = tmp["fund_id"]
    tmp["target_table"] = "fund_nv_data_standard"
    tmp = tmp[["target_table", "pk", "source_id"]]
    tmp = tmp.dropna()
    tmp["is_used"] = 0

    io.to_sql("sync_source", eg_2Gcfgp, tmp)


def main():
    rematch_source_used()
    rematch_source_not_used()


if __name__ == "__main__":
    main()
