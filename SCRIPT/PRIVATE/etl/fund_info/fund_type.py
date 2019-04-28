import datetime as dt
import pandas as pd
from utils.database import io, config as cfg

engine = cfg.load_engine()["2Gb"]

sql_fi = "SELECT fund_id FROM fund_info"
yesterday = dt.date.today() - dt.timedelta(1)

sql_ftm = "SELECT fund_id, typestandard_code, typestandard_name, type_code, type_name, stype_code, stype_name \
FROM fund_type_mapping WHERE flag=1 AND update_time >= {ut} ORDER by typestandard_code, type_code, stype_code".format(
    ut=yesterday.strftime("%Y%m%d")
)


def main():
    print("fetching get_data...")
    ftm = pd.read_sql(sql_ftm, engine)
    if len(ftm) == 0:
        print("there is no get_data to be updated...")
        return None
    fi = pd.read_sql(sql_fi, engine)

    ftm_stra = ftm.loc[ftm["typestandard_code"] == 601, ["fund_id", "type_name", "stype_name"]]
    ftm_stra["fund_type_strategy"] = ftm_stra["type_name"] + "_" + ftm_stra["stype_name"]
    ftm_stra = ftm_stra[["fund_id", "fund_type_strategy"]]

    ftm_issu = ftm.loc[ftm["typestandard_code"] == 604, ["fund_id", "type_name", "stype_name"]]
    ftm_issu = ftm_issu[["fund_id", "type_name"]]
    ftm_issu.columns = ["fund_id", "fund_type_issuance"]

    ftm_stru = ftm.loc[ftm["typestandard_code"] == 602, ["fund_id", "type_name", "stype_name"]]
    ftm_stru.columns = [["fund_id", "fund_type_structure", "structure_hierarchy"]]

    tmp = fi.copy()
    tmp = tmp[["fund_id"]]
    # 使用right join, 有分类数据时才更新对应的fund_id
    result = pd.merge(tmp, ftm_stra, on="fund_id", how="right")
    result = pd.merge(result, ftm_issu, on="fund_id", how="right")
    result = pd.merge(result, ftm_stru, on="fund_id", how="right")
    result = result.loc[result.fund_id.apply(lambda x: x[:2] == "JR")]

    print("get_data to db...")

    io.to_sql("fund_info", engine, result, "update")

if __name__ == "__main__":
    main()
