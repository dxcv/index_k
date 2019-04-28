import datetime as dt
import pandas as pd
from utils.database import config as cfg, sqlfactory as sf
from utils.script import scriptutils as su
_tb = {
    "re_w": "fund_weekly_return",
    "ri_w": "fund_weekly_risk",
    "sub_w": "fund_subsidiary_weekly_index",
    "re_m": "fund_month_return",
    "ri_m": "fund_month_risk",
    "sub_m": "fund_subsidiary_month_index"
}


def gen_sql_indicator_latest(tb_names, update_time=dt.date(2015, 1, 1)):
    sql_indicator_latest = {}
    for tb_name in tb_names:
        sql = "\
        SELECT tbi.*  FROM (SELECT fund_id, MAX(statistic_date) as date_latest FROM {tb_name}  \
        WHERE update_time >= '{update_time}'\
        GROUP BY fund_id) fld \
        JOIN {tb_name} tbi ON fld.fund_id = tbi.fund_id AND fld.date_latest = tbi.statistic_date".format(
            tb_name=tb_name,
            update_time=update_time
        )
        sql_indicator_latest[tb_name] = sql
    return sql_indicator_latest


def fetch_data(sqls, conn):
    su.tic("fetching new get_data...")
    dfs = {}
    for tb_name, sql in sqls.items():
        su.tic("fetching get_data of `{tb_name}`...".format(tb_name=tb_name))
        dfs[tb_name] = pd.read_sql(sql, conn)
    dfs["w"] = [dfs[_tb["re_w"]], dfs[_tb["ri_w"]], dfs[_tb["sub_w"]]]
    dfs["m"] = [dfs[_tb["re_m"]], dfs[_tb["ri_m"]], dfs[_tb["sub_m"]]]
    return dfs


def merge_result(dfs, how="inner"):
    su.tic("merge new get_data...")
    d = dfs[0]
    for i in range(1, len(dfs)):
        d = pd.merge(d, dfs[i], how=how, on=["fund_id", "statistic_date", "benchmark"], suffixes=["", "_dup"])
    return d


def delete_old(data, tb_name, conn):
    su.tic("delete old get_data...")
    ids4delete = sf.SQL.ids4sql(data["fund_id"].tolist())
    sql_delete = "DELETE FROM {tb_name} WHERE fund_id IN {ids}".format(
        tb_name=tb_name,
        ids=ids4delete
    )
    conn.execute(sql_delete)


def refresh(data, table, conn):
    delete_old(data, table.name, conn)
    data[["fund_id", "statistic_date", "benchmark"] + table.columns()].to_sql(
        table.name, conn, index=False, if_exists="append", chunksize=10000
    )


def main():
    tb_w = sf.Table("w", "indicator")
    tb_m = sf.Table("m", "indicator")

    engines = cfg.load_engine()
    engine_read = engines["2Gb"]
    engine_write = engines["2Gb"]
    conn_read = engine_read.connect()
    conn_write = engine_write.connect()

    tb_names = [_tb["re_w"], _tb["ri_w"], _tb["sub_w"], _tb["re_m"], _tb["ri_m"], _tb["sub_m"]]
    sqls = gen_sql_indicator_latest(tb_names)
    dfs = fetch_data(sqls, conn_read)

    dw = merge_result(dfs["w"], how="inner")
    dm = merge_result(dfs["m"], how="inner")

    refresh(dw, tb_w, conn_write)
    refresh(dm, tb_m, conn_write)


if __name__ == "__main__":
    su.tic("fund_freq_indicator...")
    main()
    su.tic("Done...")
