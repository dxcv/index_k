import pandas as pd
from utils.database import config as cfg, io
from utils.sqlfactory import constructor


ENGINE_RD = cfg.load_engine()["2Gb"]

TM_SWS = {
    '801020.SI': '210000',
    '801030.SI': '220000',
    '801040.SI': '230000',
    '801050.SI': '240000',
    '801710.SI': '610000',
    '801720.SI': '620000',
    '801730.SI': '630000',
    '801070.SI': '640000',
    '801740.SI': '650000',
    '801880.SI': '280000',
    '801110.SI': '330000',
    '801130.SI': '350000',
    '801140.SI': '360000',
    '801200.SI': '450000',
    '801010.SI': '110000',
    '801120.SI': '340000',
    '801210.SI': '460000',
    '801150.SI': '370000',
    '801160.SI': '410000',
    '801170.SI': '420000',
    '801180.SI': '430000',
    '801080.SI': '270000',
    '801750.SI': '710000',
    '801760.SI': '720000',
    '801770.SI': '730000',
    '801780.SI': '480000',
    '801790.SI': '490000',
    '801230.SI': '510000'
}


def fetch_swsindex(start, end):
    sql = "SELECT index_id, `date`, `value` " \
          "FROM base_finance.`index_value` " \
          "WHERE index_id IN ({fid})" \
          "AND (date BETWEEN '{start}' AND '{end}' OR update_time BETWEEN '{start}' and '{end}')".format(
        fid=constructor.sqlfmt(TM_SWS), start=start, end=end)

    df = pd.read_sql(sql, ENGINE_RD)
    if len(df) == 0:
        return pd.DataFrame()
    df.rename(columns={"date": "statistic_date", "value": "index_value"}, inplace=True)
    df["index_id"] = df["index_id"].apply(lambda x: TM_SWS.get(x))
    return df


def sync_sws_index(start, end):
    data = fetch_swsindex(start, end)
    io.to_sql("base.sws_index", ENGINE_RD, data, type="update")


def main():
    a = input("输入1或者天数拿取数据,默认一周\n")
    is_checked = int(a)

    if is_checked == 1:
        I = 7
        data = fetch_swsindex(I)
        a1 = input("输入1确认入库\n")
        if a1 == "1":
            io.to_sql("base.sws_index", ENGINE_RD, data, type="update")
        else:
            print("失败")
    else:
        data = fetch_swsindex(is_checked)
        a1 = input("输入1确认入库\n")
        if a1 == "1":
            io.to_sql("base.sws_index", ENGINE_RD, data, type="update")
        else:
            print("失败")


if __name__ == '__main__':
    main()
