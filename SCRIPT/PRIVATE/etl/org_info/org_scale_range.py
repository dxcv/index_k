import pandas as pd
from utils.database import config as cfg, io

# org_info.scale_range

ENGINE = cfg.load_engine()["2Gb"]
DICT_RANGE = {
    "0-1亿": 13,
    "0-2亿": 1,
    "0-10亿": 2,
    "2-5亿": 4,
    "5-10亿": 5,
    "1-10亿": 12,
    "0-20亿": 3,
    "10-20亿": 7,
    "20-50亿": 8,
    "50-100亿": 10,
    "100亿以上": 11,
    "50亿以上": 9,
    "10亿以上": 6,
}

DICT_SORT = {v: k for k, v in enumerate([
    "0-1亿",
    "0-2亿",
    "2-5亿",
    "0-10亿",
    "1-10亿",
    "5-10亿",
    "0-20亿",
    "10-20亿",
    "10亿以上",
    "20-50亿",
    "50-100亿",
    "50亿以上",
    "100亿以上",
])}
CATEGORY = {
    "私募证券自主发行": "am_scale_range_issue",
    "私募证券顾问管理": "am_scale_range_consultant"
}


def main():
    sql_xfosr = "SELECT oi.org_id as org_id, org_scale_type, org_scale_range as org_scale_range, DATE(xfosr.update_time) as update_time \
                 FROM crawl.x_fund_org_scale_range xfosr \
                 JOIN base.org_info oi ON oi.org_id = xfosr.org_id "

    df_xfosr = pd.read_sql(sql_xfosr, ENGINE)
    df_xfosr["org_scale_range"] = df_xfosr["org_scale_range"].apply(lambda x: x.replace(" ", ""))
    df_xfosr["org_scale_type"] = df_xfosr["org_scale_type"].apply(lambda x: CATEGORY.get(x, "am_scale_range_others"))
    df_xfosr["_sort"] = df_xfosr["org_scale_range"].apply(lambda x: DICT_SORT[x])
    df_xfosr["org_scale_range"] = df_xfosr["org_scale_range"].apply(lambda x: DICT_RANGE[x])
    df_xfosr = df_xfosr.sort_values(["org_id", "org_scale_type", "update_time", "_sort"], ascending=[True, True,  False, False])
    df_xfosr = df_xfosr.drop_duplicates(subset=["org_id", "org_scale_type"])

    result = df_xfosr.groupby(["org_id", "org_scale_type"]).last().unstack()["org_scale_range"]
    result["org_id"] = result.index
    return result

if __name__ == "__main__":
    result = main()
    io.to_sql("org_info", ENGINE, result)
