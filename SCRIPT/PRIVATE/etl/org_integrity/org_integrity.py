import pandas as pd
import re
from utils.database import config as cfg, io


ENGINE_RD = cfg.load_engine()["etl_crawl_private"]
ENGINE_WT = cfg.load_engine()["etl_base_private"]


ABNORMALS = ["其他诚信信息", "异常机构", "失联机构", "重大遗漏", "违反八条底线", "其他情形", "是否已经整改"]
MOD = len(ABNORMALS) + 1
patt = "|".join(["({abnormal}(?=[:|：]))".format(abnormal=abnormal) for abnormal in ABNORMALS])


def reduce_list(lst):
    reduced_lst = [list(filter(lambda x: x is not None, lst[i: i + MOD])) for i in range(0, len(lst), MOD)]
    return dict(reduced_lst)


def preprocess(df):
    df["clean"] = df["integrity_info"].apply(lambda x: re.split(patt, x)[1:]).apply(lambda x: reduce_list(x))
    data_dict = dict(zip(df["org_id"], df["clean"]))
    result = pd.DataFrame(data_dict).unstack().reset_index().dropna()
    return result


def main():
    sql_info = "SELECT xoi.org_id, integrity_info FROM x_org_info xoi \
    JOIN (SELECT MAX(version) latest_ver, org_id FROM x_org_info GROUP BY org_id) tb_tmp \
    ON xoi.org_id = tb_tmp.org_id AND xoi.version = tb_tmp.latest_ver WHERE (integrity_info != '')"

    df = pd.read_sql(sql_info, ENGINE_RD)
    result = preprocess(df)
    result.columns = ["org_id", "ab_type", "ab_reason"]
    result["ab_reason"] = result["ab_reason"].apply(lambda x: re.sub("\s", "", x))
    result["ab_reason"] = result["ab_reason"].apply(lambda x: re.sub(":|：", "", x[:2]) + x[2:])

    io.to_sql("org_integrity", ENGINE_WT, result)

if __name__ == "__main__":
    main()
