from utils.database import io, config as cfg
import pandas as pd

# 一次性脚本, 将存在于org_id_match, 但是不存在于x_fund_org表中的数据
# 存入至crawl库中的im_org_info表中;

def main():
    engine_rd = cfg.load_engine()["2G"]
    engine_wt = cfg.load_engine()["2Gc"]

    sql = "SELECT * FROM base.org_info \
    WHERE address IS NOT NULL and org_id NOT IN (SELECT oim.org_ID as matched_id \
    FROM crawl.x_fund_org xfo \
    JOIN (SELECT org_id, MAX(entry_time) msd FROM crawl.x_fund_org GROUP BY org_id ) tb_msd ON xfo.org_id = tb_msd.org_id AND xfo.entry_time = tb_msd.msd \
    JOIN base.org_id_match oim ON xfo.org_id = oim.source_org_ID)"

    with engine_rd.connect() as conn:
        df = pd.read_sql(sql, conn)
        df["data_date"] = df["entry_time"].apply(lambda x: x.date())    # 将入库日期作为data_date

        sql2 = "SELECT * FROM crawl.im_org_info LIMIT 0, 1"
        df2 = pd.read_sql(sql2, conn)
        df = df[list(set(df2).intersection(set(df)))]
        conn.close()

    with engine_wt.connect() as conn:
        io.to_sql("im_org_info", conn, df, chunksize=500)
        conn.close()
