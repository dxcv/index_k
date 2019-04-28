import pandas as pd
from utils.database import io, config as cfg

ENGINE_RD = cfg.load_engine()["2Gb"]


def update_flag():
    # 读取fund_info_private表中, 存在于fund_id_match, 配对类型为基协的fund_id和flag;
    sql_fiprivate = "SELECT fim.fund_ID as fund_id, fim.source_ID as source_id, fip.flag FROM fund_info_private fip \
                    JOIN fund_id_match fim ON fip.fund_id = fim.source_ID \
                    WHERE fim.match_type = 5 AND fim.fund_ID LIKE 'JR%%'"
    df = pd.read_sql(sql_fiprivate, ENGINE_RD)

    # 根据source_id排序(降序)
    df["val"] = df["source_id"].apply(lambda x: int(x[2:]))
    df = df.sort_values(["fund_id", "val"], ascending=[True, False])
    del df["val"]

    # 取source_id最大的记录, 更新flag为1, 其余设置为0
    df["flag"] = None
    df_dd = df.drop_duplicates(subset=["fund_id"], keep="first")
    df_dd["flag"] = 1
    df_dd = df.fillna(df_dd).fillna(0)
    df_dd = df_dd[["source_id", "flag"]]
    df_dd.columns = ["fund_id", "flag"]

    # 入库
    result = df_dd
    io.to_sql("fund_info_private", ENGINE_RD, result)


if __name__ == "__main__":
    update_flag()
