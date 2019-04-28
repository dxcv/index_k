from utils.database import io, config as cfg
import pandas as pd
import re

engine_rd = cfg.load_engine()["2Gb"]
engine_wt = cfg.load_engine()["2Gb"]

PROVINCES_CHN = {
    "山东", "江苏", "浙江", "安徽", "福建", "江西", "广东", "广西", "海南", "河南", "湖南",
    "湖北", "天津", "河北", "山西", "内蒙古", "宁夏", "青海", "陕西", "甘肃", "新疆", "四川",
    "贵州", "云南", "重庆", "西藏", "辽宁", "吉林", "黑龙江", "香港", "澳门", "台湾"
}
PROVINCES_CHN_re = ".*(" + "|".join(PROVINCES_CHN) + ").*"

CITIES = {
    "北京", "上海", "深圳"
}
CITIES_re = ".*(" + "|".join(CITIES) + ").*"


def main():
    sql = "SELECT org_id, address, region FROM base.org_info"

    with engine_rd.connect() as conn:
        df_oi = pd.read_sql(sql, conn)

    # 在去空值后, 找出region字段不被包含在address字段的机构, 并且根据address去空
    df = df_oi.loc[
        list(map(lambda x, y: (x not in y), df_oi["region"].fillna("__NONE1"), df_oi["address"].fillna("__NONE2")))
    ].dropna(subset=["address"])

    # 对剩余address不为空, 但是region为空或者与address不一致的数据, 从address中提取region;
    df["region_1"] = df["address"].apply(lambda x: re.search(CITIES_re, x))
    df["region_1"] = df["region_1"].apply(lambda x: x.groups()[0] if x is not None else None)
    df["region_2"] = df["address"].apply(lambda x: re.search(PROVINCES_CHN_re, x))
    df["region_2"] = df["region_2"].apply(lambda x: x.groups()[0] if x is not None else None)
    df["region"] = df["region_1"].fillna(df["region_2"])
    del df["region_1"]; del df["region_2"]; del df["address"]
    df = df.dropna(subset=["region"])

    with engine_wt.connect() as conn:
        io.to_sql("org_info", conn, df)
        conn.close()


if __name__ == "__main__":
    main()
