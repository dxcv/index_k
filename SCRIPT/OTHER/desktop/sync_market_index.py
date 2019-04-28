import pandas as pd
from sqlalchemy import create_engine
from utils.database import io


engine_base = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base',),
    connect_args={"charset": "utf8"}, echo=False
)


def fetch_market_index(start, end):
    sql = "SELECT index_id, `date`, `value` " \
          "FROM base_finance.index_value " \
          "WHERE index_id IN " \
          "('000300.CSI','000002.CSI','000016.CSI','000905.CSI','H11001.CSI', 'NHCI.NHF')" \
          "AND `update_time` BETWEEN '{start}' AND '{now}'".format(start=start, now=end)

    csi = {'000300.CSI': 'hs300', '000002.CSI': 'ssia', '000016.CSI': 'sse50', '000905.CSI': 'csi500', 'H11001.CSI': 'cbi', 'NHCI.NHF': "nfi"}
    df = pd.read_sql(sql, engine_base)
    if len(df) == 0:
        return pd.DataFrame()
    df["index_id"] = df["index_id"].apply(lambda x: csi.get(x))
    data = df.groupby(["index_id", "date"]).last()["value"].unstack().T.reset_index()
    data.rename(columns={"date": "statistic_date"}, inplace=True)
    return data


def sync_market_index(start, end):
    data = fetch_market_index(start, end)
    io.to_sql("market_index", engine_base, data, type="update")


def main():
    a = input("输入1或者天数拿取数据,默认一周\n")
    is_checked = int(a)

    if is_checked == 1:
        I = 7
        data = fetch_market_index(I)
        a1 = input("输入1确认入库\n")
        if a1 == "1":
            io.to_sql("market_index", engine_base, data, type="update")

    else:
        data = fetch_market_index(is_checked)
        a1 = input("输入1确认入库\n")
        if a1 == "1":
            io.to_sql("market_index", engine_base, data, type="update")


if __name__ == '__main__':
    main()
