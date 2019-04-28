from sqlalchemy import create_engine
import pandas as pd
from utils.database import io

# HEDGE
# ENGINE_R = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@182.254.128.241:4119/product?charset=utf8")
# ENGINE_W = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@192.168.11.200:3306/product_private?charset=utf8")

# MUTUAL
ENGINE_R = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@182.254.128.241:4171/base_public?charset=utf8")
ENGINE_W = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@182.254.128.241:4119/product_mutual?charset=utf8")


TABLE_TO_SYNC = "fund_daily_subsidiary3"
STEP = 10


def sync(fids):
    sql = "SELECT * FROM {tb} WHERE fund_id IN {fids}".format(tb=TABLE_TO_SYNC, fids=str(tuple(fids)))
    print("fetching...")
    print(sql)
    df = pd.read_sql(sql, ENGINE_R)
    print("writing...")
    io.to_sql(TABLE_TO_SYNC, ENGINE_W, df)
    print("done")


def main():
    fids = sorted(pd.read_sql("SELECT fund_id FROM fund_info", ENGINE_R)["fund_id"].tolist())
    for i in range(200, len(fids), STEP):
        print(i)
        try:
            sync(fids[i: i + STEP])
        except Exception as e:
            print(e)

if __name__ == "__main__":
    main()
