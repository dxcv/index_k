from sqlalchemy import create_engine
import sys
import os
import pandas as pd
import re
import shutil

#### SETTING HERE ####
DEFAULT_DIR = "c:/Users/Yu/Desktop/TEST"
MODE = "#STRICT"
COLS_USED = {"fund_id", "fund_name", "statistic_date", "nav", "added_nav", "adjusted_nav", "source_id"}
######################

ENGINE_WT = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@182.254.128.241:4171/crawl_private?charset=utf8")


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


def to_sql(tb_name, conn, dataframe, type="update", chunksize=500, debug=False):
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

    df = dataframe.copy(deep=False)
    df = df.fillna("None")
    df = df.applymap(lambda x: re.sub('([\'\"\\\])', '\\\\\g<1>', str(x)))
    cols_str = sql_cols(df)
    sqls = []
    for i in range(0, len(df), chunksize):
        # print("chunk-{no}, size-{size}".format(no=str(i/chunksize), size=chunksize))
        df_tmp = df[i: i + chunksize]

        if type == "replace":
            sql_base = "REPLACE INTO `{tb_name}` {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )

        elif type == "update":
            sql_base = "INSERT INTO `{tb_name}` {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )
            sql_update = "ON DUPLICATE KEY UPDATE {0}".format(
                sql_cols(df_tmp, "values")
            )

        elif type == "ignore":
            sql_base = "INSERT IGNORE INTO `{tb_name}` {cols}".format(
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
            except Exception as e:
                print("ENCOUNTERING ERROR: {e}, RETRYING".format(e=e))
                conn.execute(sql_main)
        else:
            sqls.append(sql_main)
    if debug:
        return sqls


def init_dir(root_dir):
    if os.path.isdir(root_dir):
        DIR_CHECKED = os.path.join(root_dir, "checked")
        DIR_ERRORS = os.path.join(root_dir, "errors")

        for d in {DIR_CHECKED, DIR_ERRORS}:
            if os.path.isdir(d) is False:
                print("New Directory: ", d)
                os.mkdir(d)
    else:
        raise FileNotFoundError("{directory} is not existed".format(directory=root_dir))


def read_data(file):
    file_suffix = os.path.splitext(file)[-1]
    if file_suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(file)
    elif file_suffix in {".csv"}:
        df = pd.read_csv(file)
    else:
        return

    for col in df.columns:
        if col not in COLS_USED:
            del df[col]
    if ("fund_id" not in df.columns) or ("source_id" not in df.columns) or ("statistic_date" not in df.columns):
        raise ValueError("MISSING PK")

    df["source_id"] = df["source_id"].apply(lambda x: "0" * (6-len(str(x))) + str(x))
    return df


def main():
    init_dir(DIR)

    files = list(os.walk(DIR))[0][2]
    err_log = {}

    for file in files:
        try:
            whole_file_path = os.path.join(DIR, file)
            if file == "src2std.py": continue

            df = read_data(whole_file_path)

            if MODE != "STRICT":
                to_sql("g_fund_nv", ENGINE_WT, df)
                shutil.move(whole_file_path, os.path.join(DIR, "checked", file))
                err_log[file] = "Done"
            else:
                print("FILE NAME:", file)
                print(df)
                inp = input("input 1 to write to db...\n")
                if inp == "1":
                    to_sql("g_fund_nv", ENGINE_WT, df)
                    shutil.move(whole_file_path, os.path.join(DIR, "checked", file))
                    err_log[file] = "Done"
                else:
                    err_log[file] = "Unchecned"
                continue
        except Exception as e:
            err_log[file] = "ERROR: " + str(e)
            shutil.move(whole_file_path, os.path.join(DIR, "errors", file))

    print(err_log)


if __name__ == "__main__":
    # DIR = input("INPUT YOUR ROOT DIRECTORY\n") or DEFAULT_DIR
    DIR = DEFAULT_DIR
    main()
    input("press any key to exit...")