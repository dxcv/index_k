import pandas as pd
from utils.database import config as cfg, io, sqlfactory
from multiprocessing.dummy import Pool as ThreadPool
from utils.decofactory import common


ENGINE_RD = cfg.load_engine()["2Gb"]


@common.auto_retry(5, 2)
def get_data(stock_ids):
    sql = "SELECT subject_id, subject_name, `date`, closing_price, market_price, circulated_price, pe_ttm, " \
          "pe_deducted_ttm, pe_lyr, pb, pb_lf, pb_mrq, beta_24m, status, last_trading_day " \
          "FROM security_price WHERE subject_id IN {sids}".format(sids=sqlfactory.SQL.values4sql(stock_ids))

    df = pd.read_sql(sql, ENGINE_RD).rename(columns={"subject_id": "stock_id", "subject_name": "name", "closing_price": "close", "last_trading_day": "last_trade_day"})

    col_stock_info = ["stock_id", "name"]
    col_stock_price = ["stock_id", "date", "last_trade_day", "status", "close"]
    col_stock_valuation = ["stock_id", "date", "market_price", "circulated_price", "pe_ttm", "pe_deducted_ttm", "pe_lyr", "pb", "pb_lf", "pb_mrq", "beta_24m"]

    io.to_sql("base_finance.stock_info", ENGINE_RD, df[col_stock_info])
    io.to_sql("base_finance.stock_price", ENGINE_RD, df[col_stock_price])
    io.to_sql("base_finance.stock_valuation", ENGINE_RD, df[col_stock_valuation])


def sync():
    sql = "SELECT DISTINCT subject_id as stock_id FROM base.security_price"
    STEP = 5
    sids = [x[0] for x in ENGINE_RD.execute(sql).fetchall()]
    sids = [sids[i:i + STEP] for i in range(0, len(sids), STEP)]
    p = ThreadPool(6)
    p.map(get_data, sids)


def main():
    sync()


if __name__ == "__main__":
    main()
