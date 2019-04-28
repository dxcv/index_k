import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.database.sqlfactory import SQL
from utils.algorithm import etl
from utils.database.models.config import ConfigSource
from utils.sqlfactory.constructor import sqlfmt
from utils.database.models.base_public import FundInfo, FundNvSource, FundNv
import datetime as dt
from dateutil.relativedelta import relativedelta

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["incremental"]

_entities_map = [
    (FundInfo.fund_id, FundNv.fund_id), (FundInfo.fund_name, FundNv.fund_name),
    (FundNvSource.data_source, FundNv.data_source),
    (FundNvSource.statistic_date, FundNv.statistic_date), (FundNvSource.nav, FundNv.nav),
    (FundNvSource.added_nav, FundNv.added_nav),
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_nv(update_time_l):
    """
    Fetch records of DOrgInfo table where record update time >= `update_time`
    Args:
        update_time_l: record update time

    Returns:
        pandas.DataFrame
    """

    where = "update_time BETWEEN '{upt_l}' AND '{upt_r}'".format(upt_l=update_time_l.strftime("%Y%m%d%H%M%S"), upt_r=(update_time_l + relativedelta(hours=1, minutes=5)).strftime("%Y%m%d%H%M%S"))

    sql_upt = "SELECT fund_id, statistic_date FROM fund_nv_source " \
              "WHERE fund_id IN (SELECT DISTINCT fund_id FROM fund_nv_source WHERE {cond}) " \
              "AND {cond}" \
              .format(cond=where)
    df_cond = pd.read_sql(sql_upt, _engine_wt)
    # df_cond["statistic_date"] = df_cond["statistic_date"].apply(lambda x: "'" + str(x) + "'")

    df_gbs = df_cond.groupby("statistic_date")["fund_id"].apply(lambda x: "(" + sqlfmt(x) + ")").reset_index()
    df_gbf = df_cond.groupby("fund_id")["statistic_date"].apply(lambda x: "(" + sqlfmt(x) + ")").reset_index()
    df = df_gbf if len(df_gbf) <= len(df_gbs) else df_gbs
    col_eq, col_in = df.columns
    conds = [
        "({col_eq} = '{val_eq}' AND {col_in} IN {val_in})".format(
            col_eq=col_eq, val_eq=val_eq, col_in=col_in, val_in=val_in)
        for val_eq, val_in in zip(df[col_eq], df[col_in])
    ]
    res = pd.DataFrame()
    for i in range(len(conds)):
        sql_conds = " OR ".join(conds[i: i + 1])
        sql_all = "SELECT fund_id, fund_name, data_source, statistic_date, nav, added_nav " \
                  "FROM fund_nv_source " \
                  "WHERE {conds}".format(conds=sql_conds)
        df = pd.read_sql(sql_all, _engine_wt)

        res = res.append(df)
    res.index = res[["fund_id", "statistic_date"]]
    return res


def transform(upt=UPDATE_TIME):
    df = fetch_multisource_nv(upt)

    df_020001 = df.ix[df[FundInfo.data_source.name] == "020001"]
    df_020002 = df.ix[df[FundInfo.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundInfo.data_source.name] == "020003"]

    result = df_020002.join(
        df_020001, how="outer", rsuffix="_020001"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[df_020002.columns].fillna(df_020001).fillna(df_020003).dropna(subset=["fund_id"]).dropna(
        subset=["statistic_date"]).dropna(subset=["added_nav"])
    return result


def main():
    for upt in pd.date_range(dt.datetime.now() - relativedelta(hours=6), dt.datetime.now(), freq="1H"):
        print(upt)
        try:
            res = transform(upt)
            print(len(res))
            io.to_sql(FundNv.__tablename__, _engine_wt, res)
        except KeyError as err:
            pass


if __name__ == "__main__":
    main()
