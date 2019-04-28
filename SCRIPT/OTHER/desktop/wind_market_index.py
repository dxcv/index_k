import sys
import datetime as dt
import pandas as pd
from sqlalchemy import create_engine
from WindPy import w as wind
wind.start()

__engine_rd = create_engine("mysql+pymysql://jr_admin_4:jr_admin_4@db.chfdb.cc:4171/base",connect_args={"charset": "utf8"})
__previous_day = 20
__code_map = {
    "000300.SH": "hs300",
    "000905.SH": "csi500",
    "000016.SH": "sse50",
    "000002.SH": "ssia",
    "H11011.CSI": "cbi",
    "NH0100.NHF": "nfi",
    "M1001940": "y1_treasury_rate"
}


def sql_cols(df, usage="sql"):
    cols = tuple(df.columns)
    if usage == "sql":
        cols_str = str(cols).replace("'", "`")
        if len(df.columns) == 1:
            cols_str = cols_str[:-2] + ")"  # to process dataframe with only one column
        return cols_str
    elif usage == "format":
        base = "'%%(%s)s'" % cols[0]
        for col in cols[1:]:
            base += ", '%%(%s)s'" % col
        return base
    elif usage == "values":
        base = "%s=VALUES(%s)" % (cols[0], cols[0])
        for col in cols[1:]:
            base += ", `%s`=VALUES(`%s`)" % (col, col)
        return base


def to_sql(tb_name, conn, dataframe, type="update", chunksize=2000):
    """
    Dummy of pandas.to_sql, support "REPLACE INTO ..." and "INSERT ... ON DUPLICATE KEY UPDATE (keys) VALUES (values)"
    SQL statement.

    Args:
        tb_name: str
            Table to insert get_data;
        conn:
            DBAPI Instance
        dataframe: pandas.DataFrame
            Dataframe instance
        type: str, optional {"update", "replace"}, default "update"
            Specified the way to update get_data. If "update", then `conn` will execute "INSERT ... ON DUPLICATE UPDATE ..."
            SQL statement, else if "replace" chosen, then "REPLACE ..." SQL statement will be executed;
        chunksize: int
            Size of records to be inserted each time;
        **kwargs:

    Returns:
        None
    """

    df = dataframe.copy()
    df = df.fillna("None")
    cols_str = sql_cols(df)
    for i in range(0, len(df), chunksize):
        # print("chunk-{no}, size-{size}".format(no=str(i/chunksize), size=chunksize))
        df_tmp = df[i: i + chunksize]
        if type == "replace":
            sql_base = "REPLACE INTO `{tb_name}` {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )
            sql_val = sql_cols(df_tmp, "format")
            vals = tuple([sql_val % x for x in df_tmp.to_dict("records")])
            sql_vals = "VALUES ({x})".format(x=vals[0])
            for i in range(1, len(vals)):
                sql_vals += ", ({x})".format(x=vals[i])
            sql_vals = sql_vals.replace("'None'", "NULL")

            sql_main = sql_base + sql_vals

        elif type == "update":
            sql_base = "INSERT INTO `{tb_name}` {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )
            sql_val = sql_cols(df_tmp, "format")
            vals = tuple([sql_val % x for x in df_tmp.to_dict("records")])
            sql_vals = "VALUES ({x})".format(x=vals[0])
            for i in range(1, len(vals)):
                sql_vals += ", ({x})".format(x=vals[i])
            sql_vals = sql_vals.replace("'None'", "NULL")

            sql_update = "ON DUPLICATE KEY UPDATE {0}".format(
                sql_cols(df_tmp, "values")
            )

            sql_main = sql_base + sql_vals + sql_update
        if sys.version_info.major == 2:
            sql_main = sql_main.replace("u`", "`")
        sql_main = sql_main.replace("%", "%%")
        conn.execute(sql_main)


class Index:
    def __init__(self, date_s, date_e):
        self._cached = {}
        self._params = {
            "date_s": date_s.strftime("%Y%m%d"),
            "date_e": date_e.strftime("%Y%m%d")
        }


class MarketIndex(Index):
    def __init__(self, date_s, date_e):
        super().__init__(date_s, date_e)
        # self._fields = ["close", "lastradeday_s"]
        self._fields = ["close"]
        self._params.update(fields=",".join(self._fields))

    @property
    def params(self):
        return self._params

    @property
    def csi300(self):
        k = "csi300"
        if k not in self._cached:
            self._cached[k] = wind.wsd("000300.SH", fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]

    @property
    def csi500(self):
        k = "csi500"
        if k not in self._cached:
            self._cached[k] = wind.wsd("000905.SH", fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]

    @property
    def sse50(self):
        k = "sse50"
        if k not in self._cached:
            self._cached[k] = wind.wsd("000016.SH", fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]

    @property
    def ssia(self):
        k = "ssia"
        if k not in self._cached:
            self._cached[k] = wind.wsd("000002.SH", fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]

    @property
    def cbi(self):
        k = "cbi"
        if k not in self._cached:
            self._cached[k] = wind.wsd("H11011.CSI", fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]

    @property
    def nfi(self):
        k = "nfi"
        if k not in self._cached:
            self._cached[k] = wind.wsd("NH0100.NHF", fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]


class TreasuryBond(Index):
    def __init__(self, date_s, date_e):
        super().__init__(date_s, date_e)

    @property
    def params(self):
        return self._params

    @property
    def tbond_y1(self):
        k = "tbond_y1"
        if k not in self._cached:
            self._cached[k] = wind.edb("M1001940", beginTime=self.params["date_e"], endTime=self.params["date_s"],
                                       options="Fill=Previous")
        return self._cached[k]


def construct(date_s, date_e=None):
    if date_e is None:
        date_e = date_s - dt.timedelta(__previous_day)
    mi = MarketIndex(date_s, date_e)
    result = None
    tb = TreasuryBond(date_s, date_e)
#    wdata = [mi.csi300, mi.csi500, mi.sse50, mi.ssia, mi.cbi, mi.nfi, tb.tbond_y1]
    wdata = [mi.csi300, mi.csi500, mi.sse50, mi.ssia, mi.nfi, tb.tbond_y1]
    for idx, wd in enumerate(wdata):
        tmp = pd.DataFrame(wd.Data).T
        tmp.columns = [__code_map[wd.Codes[0]]]
        tmp["statistic_date"] = [x.date() for x in wd.Times]
        tmp = tmp[tmp.columns[::-1]]
        if idx == 0:
            result = tmp
        else:
            result = result.merge(tmp, how="outer", on="statistic_date")
    result = result.loc[(result["statistic_date"] >= date_e) & (result["statistic_date"] <= date_s)]

    return result


def main():
    print("fetching data...{time}\n\n".format(time=dt.datetime.now()))
    data = construct(dt.date.today())
    print(data)
    checked = input("check your data...press 1 to continue, any other key to break...\n\n")
    if checked == "1":
        to_sql("market_index", __engine_rd, data)
    else:
        pass
    input("done...\npress any key to continue...")

if __name__ == "__main__":
    main()
