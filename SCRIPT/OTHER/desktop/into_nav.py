import numpy as np
import pandas as pd
import os
import re
import sys
import time
import pymysql
import datetime as dt
from sqlalchemy import create_engine

engine_config_private = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'config_private', ),
    connect_args={"charset": "utf8"}, echo=False, )
engine_base = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),
    connect_args={"charset": "utf8"}, echo=False, )
engine_crawl_private = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'crawl_private', ),
    connect_args={"charset": "utf8"}, echo=False, )
PATH = None or os.path.join(os.path.expanduser("~"), 'Desktop', "DIFF_NV")


def to_sql(tb_name, conn, dataframe, type="update", chunksize=2000, debug=False):
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
        type: str, optional {"update", "replace", "ignore"}, default "update"
            Specified the way to update get_data. If "update", then `conn` will execute "INSERT ... ON DUPLICATE UPDATE ..."
            SQL statement, else if "replace" chosen, then "REPLACE ..." SQL statement will be executed; else if "ignore" chosen,
            then "INSERT IGNORE ..." will be excuted;
        chunksize: int
            Size of records to be inserted each time;
        **kwargs:

    Returns:
        None
    """
    tb_name = ".".join(["`" + x + "`" for x in tb_name.split(".")])

    df = dataframe.copy(deep=False)
    df = df.fillna("None")
    df = df.applymap(lambda x: re.sub('([\'\"\\\])', '\\\\\g<1>', str(x)))
    cols_str = sql_cols(df)
    sqls = []
    for i in range(0, len(df), chunksize):
        # print("chunk-{no}, size-{size}".format(no=str(i/chunksize), size=chunksize))
        df_tmp = df[i: i + chunksize]

        if type == "replace":
            sql_base = "REPLACE INTO {tb_name} {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )

        elif type == "update":
            sql_base = "INSERT INTO {tb_name} {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )
            sql_update = "ON DUPLICATE KEY UPDATE {0}".format(
                sql_cols(df_tmp, "values")
            )

        elif type == "ignore":
            sql_base = "INSERT IGNORE INTO {tb_name} {cols}".format(
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
        if type == "update":
            sql_main += sql_update

        if sys.version_info.major == 2:
            sql_main = sql_main.replace("u`", "`")
        if sys.version_info.major == 3:
            sql_main = sql_main.replace("%", "%%")

        if debug is False:
            try:
                conn.execute(sql_main)
            except pymysql.err.InternalError as e:
                print("ENCOUNTERING ERROR: {e}, RETRYING".format(e=e))
                time.sleep(10)
                conn.execute(sql_main)
        else:
            sqls.append(sql_main)
    if debug:
        return sqls


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


def to_list(df):
    a = np.array(df)
    vv = a.tolist()
    return vv


def to_sync(JR, source, is_used, priority=9):
    if priority == '不变':
        print(source + "sync_source不变")
    else:
        if priority < 0:
            priority = 0
        target_table = "fund_nv_data_standard"
        list = [target_table, JR, source, priority, is_used]
        df_sync = pd.DataFrame(list)
        df_sync = df_sync.T
        df_sync.columns = ["target_table", "pk", "source_id", "priority", "is_used"]
        print(df_sync)
        to_sql("sync_source", engine_config_private, df_sync, type="update")


def to_db(JR, df_w):
    df = df_w.iloc[1:]
    c = df_w.iloc[0]
    co = to_list(c)
    num = int((len(co)-5)/3)
    for i in range(num):
        source = i*3+1
        sync = i*3+3
        nav = i*3+1
        added_nav = i*3+2
        one_df = df.iloc[:, [0, nav, added_nav, sync]].dropna()
        if co[sync] == "#":
            priority = "不变"
            is_used = "不变"
        else:
            priority = 0 if int(round((co[sync] - 1) * 10, 0)) <= 0 else int(round((co[sync] - 1) * 10, 0))
            if int(co[sync]) == 2:
                is_used = 1
            else:
                is_used = int(co[sync])
        source_id = str(int(co[source])) if int(co[source]) > 2 else "000001"
        source_id = "0" + source_id if len(source_id) == 5 else source_id

        if is_used == "不变":
            print(source_id, "pass")
        else:

            to_sync(JR, source_id, is_used, priority)
            one_df.columns = ["statistic_date", "nav", "added_nav", "is_used"]
            one_df["source_id"] = source_id
            try:
                one_df["statistic_date"] = one_df["statistic_date"].apply(lambda x: dt.datetime.strptime(x, "%Y/%m/%d"))
            except BaseException:
                pass
            else:
                pass
            one_df["fund_id"] = JR
            one_df["is_used"] = one_df["is_used"].apply(lambda x: int(float(x)))
            one_df["nav"] = one_df["nav"].apply(lambda x: '%.4f' % x)
            one_df["added_nav"] = one_df["added_nav"].apply(lambda x: '%.4f' % x)
            df_last = one_df.iloc[:, [0, 3, 4, 5]].dropna()
            print(df_last)
            assert all(df_last["fund_id"].notnull()) and all(df_last["statistic_date"].notnull()) and all(
                df_last["source_id"].notnull())
            to_sql("fund_nv_data_source_copy2", engine_base, df_last, type="update")
            y_fund_nv = df.iloc[:, [0, -2, -1]].dropna()
            if y_fund_nv.empty:
                pass
            else:
                y_fund_nv.columns = ["statistic_date", "nav", "added_nav"]
                y_fund_nv["fund_id"] = JR
                y_fund_nv["source_id"] = "000001"
                print(y_fund_nv)
                to_sql("y_fund_nv", engine_crawl_private, y_fund_nv, type="update")
                to_sync(JR, "000001", 1, 10)


def get_one(inp):
    inp = str(inp)
    if len(inp) != 6:
        inp = inp.zfill(6)

    if inp[:2] != "JR":
        JR = "JR" + inp
        inp = "JR" + inp + ".csv"
        print(JR)
    else:
        JR = inp
        inp = inp + ".csv"
        print(JR)
    try:
        PP = PATH + '\\' + inp
        df = pd.read_csv(PP, header=None)
        to_db(JR, df)
    except FileNotFoundError:
        print("没有这个文件")


def main():
    while True:
        inp = str(input("Input a fund_id to db, or Q to exit"))
        if inp.lower() == "q":
            input("Press any key to exit.")
            break
        if inp.lower() == "c":
            try:
                ids = pd.read_clipboard(header=None)[0].tolist()
                for id in ids:
                    get_one(id)
            except:
                continue
        else:
            try:
                get_one(inp)
            except:
                continue


if __name__ == '__main__':
    main()
