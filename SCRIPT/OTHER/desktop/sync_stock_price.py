import pandas as pd
from utils.database import config as cfg, io

engine_base = cfg.load_engine()["2Gb"]


def fetch_stockvaluation(start, end):
    sql = "SELECT * FROM ( \
            SELECT p.stock_id ,p.date ,v.`market_price` ,v.`circulated_price` ,v.`pe_ttm` , \
            v.`pe_deducted_ttm` ,v.`pe_lyr`,v.`pb`,v.pb_lf,v.`pb_mrq`,v.`beta_24m` , \
            p.`last_trading_day`,p.`status`,p.`close`   \
            FROM base_finance.stock_price as p \
            LEFT JOIN base_finance.stock_valuation v  \
            ON p.stock_id = v.stock_id AND p.date = v.date \
            WHERE (p.date between '{before}' AND '{now}')) T1 \
            UNION (SELECT p.stock_id ,p.date ,v.`market_price` ,v.`circulated_price` ,v.`pe_ttm` , \
            v.`pe_deducted_ttm` ,v.`pe_lyr`,v.`pb`,v.pb_lf,v.`pb_mrq`,v.`beta_24m` , \
            p.`last_trading_day`,p.`status`,p.`close`   \
            FROM base_finance.stock_price as p \
            LEFT JOIN base_finance.stock_valuation v  \
            ON p.stock_id = v.stock_id AND p.date = v.date \
            WHERE (p.update_time between '{before}' AND '{now}'))  \
            UNION (SELECT p.stock_id ,p.date ,v.`market_price` ,v.`circulated_price` ,v.`pe_ttm` , \
            v.`pe_deducted_ttm` ,v.`pe_lyr`,v.`pb`,v.pb_lf,v.`pb_mrq`,v.`beta_24m` , \
            p.`last_trading_day`,p.`status`,p.`close`   \
            FROM base_finance.stock_price as p \
            LEFT JOIN base_finance.stock_valuation v  \
            ON p.stock_id = v.stock_id AND p.date = v.date \
            WHERE (v.update_time between '{before}' AND '{now}'))".format(before=start, now=end)

    sql2 = "SELECT stock_id as subject_id, `name` as subject_name  FROM base_finance.stock_info"

    name = pd.read_sql(sql2, engine_base)
    df = pd.read_sql(sql, engine_base)
    if len(df) == 0:
        return pd.DataFrame()
    df.rename(columns={"stock_id": "subject_id", "close": "closing_price"}, inplace=True)
    df_all = pd.merge(df, name, how='inner', on='subject_id')

    return df_all


def sync_stock_price(start, end):
    data = fetch_stockvaluation(start, end)
    io.to_sql("security_price", engine_base, data, type="update")


def main():
    a = input("输入1或者天数拿取数据,默认一周\n")
    is_checked = int(a)

    if is_checked == 1:
        I = 7
        data = fetch_stockvaluation(I)
        print(data)
        a1 = input("输入1确认入库\n")
        if a1 == "1":
            io.to_sql("security_price", engine_base, data, type="update")
        else:
            print("失败")
    else:
        data = fetch_stockvaluation(is_checked)
        print(data)
        a1 = input("输入1确认入库\n")
        if a1 == "1":
            io.to_sql("security_price", engine_base, data, type="update")
        else:
            print("失败")


if __name__ == '__main__':
    main()
