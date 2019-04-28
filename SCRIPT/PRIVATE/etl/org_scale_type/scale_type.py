import pandas as pd
import datetime as dt
from utils.database import config as cfg, io

# org_info.scale_range

ENGINE = cfg.load_engine()["2Gb"]
DICT_RANGE = {
    None: 0,
    "0-1亿": 1,
    "1-10亿": 2,
    "10-20亿": 3,
    "20-50亿": 4,
    "50亿以上": 5,
}
DICT_RANGE_rev = {v: k for k, v in DICT_RANGE.items()}

CATEGORY = {
    "私募证券自主发行": "scale_range_issue",
    "私募证券顾问管理": "scale_range_consultant"
}


def main():
    sql_xfosr = "SELECT oi.org_id as org_id, org_scale_type, org_scale_range as org_scale_range, DATE(xfosr.update_time) as update_time \
                 FROM crawl_private.x_org_scale_range xfosr \
                 JOIN base.org_info oi ON oi.org_id = xfosr.org_id "

    df_xfosr = pd.read_sql(sql_xfosr, ENGINE)
    df_xfosr["org_scale_range"] = df_xfosr["org_scale_range"].apply(lambda x: x.replace(" ", ""))  # 移除了字符串中的空格, e.g. "0-1 亿"
    df_xfosr = df_xfosr.loc[df_xfosr["org_scale_type"] != ""]
    df_xfosr["org_scale_type"] = df_xfosr["org_scale_type"].apply(lambda x: CATEGORY.get(x, "am_scale_range_others"))

    df_xfosr = df_xfosr.sort_values(["org_id", "org_scale_type", "update_time"], ascending=[True, True, False])
    df_xfosr = df_xfosr.loc[df_xfosr["org_scale_type"] != "am_scale_range_others"].drop_duplicates(subset=["org_id", "org_scale_type"])

    df = df_xfosr.groupby(["org_id", "org_scale_type"])["org_scale_range"].last().apply(lambda x: DICT_RANGE.get(x, 0)).unstack().fillna(0)
    k1, k2 = "scale_range_issue", "scale_range_consultant"

    # 百亿级
    cond1 = (df[k1].apply(lambda x: x in {4, 5})) & (df[k2].apply(lambda x: x in {4, 5}))

    # 大型
    cond2 = ((df[k1].apply(lambda x: x == 0)) & (df[k2].apply(lambda x: x == 5))) \
            | ((df[k1].apply(lambda x: x == 1)) & (df[k2].apply(lambda x: x == 5))) \
            | ((df[k1].apply(lambda x: x == 2)) & (df[k2].apply(lambda x: x == 5))) \
            | ((df[k1].apply(lambda x: x == 3)) & (df[k2].apply(lambda x: x in {4, 5}))) \
            | ((df[k1].apply(lambda x: x == 4)) & (df[k2].apply(lambda x: x in {2, 3}))) \
            | ((df[k1].apply(lambda x: x == 5)) & (df[k2].apply(lambda x: x in {0, 1, 2, 3})))


    # 中大型
    cond3 = ((df[k1].apply(lambda x: x == 0)) & (df[k2].apply(lambda x: x == 4))) \
            | ((df[k1].apply(lambda x: x == 1)) & (df[k2].apply(lambda x: x in {4, 5}))) \
            | ((df[k1].apply(lambda x: x == 2)) & (df[k2].apply(lambda x: x in {4, 5}))) \
            | ((df[k1].apply(lambda x: x == 3)) & (df[k2].apply(lambda x: x in {3, 5}))) \
            | ((df[k1].apply(lambda x: x == 4)) & (df[k2].apply(lambda x: x in {0, 1}))) \
            | ((df[k1].apply(lambda x: x == 5)) & (df[k2].apply(lambda x: x in {0, 1, 2, 3})))

    # 成长型
    cond4 = ((df[k1].apply(lambda x: x == 0)) & (df[k2].apply(lambda x: x == 3))) \
            | ((df[k1].apply(lambda x: x == 1)) & (df[k2].apply(lambda x: x == 3))) \
            | ((df[k1].apply(lambda x: x == 2)) & (df[k2].apply(lambda x: x in {2, 3}))) \
            | ((df[k1].apply(lambda x: x == 3)) & (df[k2].apply(lambda x: x in {0, 1, 2})))

    # 小型私募
    cond5 = ((df[k1].apply(lambda x: x == 0)) & (df[k2].apply(lambda x: x == 2))) \
            | ((df[k1].apply(lambda x: x == 1)) & (df[k2].apply(lambda x: x == 2))) \
            | ((df[k1].apply(lambda x: x == 2)) & (df[k2].apply(lambda x: x in {0, 1})))

    # 微型私募
    cond6 = ((df[k1].apply(lambda x: x == 0)) & (df[k2].apply(lambda x: x == 1))) \
            | ((df[k1].apply(lambda x: x == 1)) & (df[k2].apply(lambda x: x in {0, 1})))

    for tp, cond in zip(("百亿级", "大型", "中大型", "成长型", "小型", "微型"), (cond1, cond2, cond3, cond4, cond5, cond6)):
        df.loc[cond, "scale_type"] = tp

    df = df.reset_index()
    import datetime as dt
    df["statistic_date"] = dt.date.today()
    for col in ("scale_range_issue", "scale_range_consultant"):
        df[col] = df[col].apply(lambda x: DICT_RANGE_rev.get(x))

    io.to_sql("org_scale_type", ENGINE, df)

if __name__ == "__main__":
    main()
