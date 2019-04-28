import datetime as dt
import pandas as pd
import re
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundInfoStructured
from utils.database.models.base_public import FundInfo, FundInfoStructured

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]



_entities_map = [
    (FundInfo.fund_id, FundInfoStructured.fund_id), (FundInfo.fund_name, FundInfoStructured.fund_name),
    (DFundInfoStructured.data_source, FundInfoStructured.data_source), (DFundInfoStructured.portfolio, DFundInfoStructured.portfolio),
    (DFundInfoStructured.is_LOF, FundInfoStructured.is_LOF),
    (DFundInfoStructured.is_inverse, FundInfoStructured.is_inverse),
    (DFundInfoStructured.termination_condition, FundInfoStructured.termination_condition),
    (DFundInfoStructured.discount_date_regular, FundInfoStructured.discount_date_regular),
    (DFundInfoStructured.discount_date_irregular, FundInfoStructured.discount_date_irregular),
    (DFundInfoStructured.close_period, FundInfoStructured.close_period),
    (DFundInfoStructured.fix_return_a, FundInfoStructured.fix_return_a),
    (DFundInfoStructured.loss_limit, FundInfoStructured.loss_limit),
    (DFundInfoStructured.guaranteed_limit, FundInfoStructured.guaranteed_limit),
    (DFundInfoStructured.excess_limit, FundInfoStructured.excess_limit),
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = [FundInfoStructured.sfund_id, FundInfoStructured.sfund_name]
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_info_structured(update_time):
    """
    Fetch records of DOrgInfo table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """
    query_fi = _session.query(FundInfo).join(
        DFundInfoStructured, and_(FundInfo.fund_id == DFundInfoStructured.fund_id)
    ).filter(
        and_(DFundInfoStructured.update_time >= update_time, DFundInfoStructured.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_fi.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[FundInfoStructured.fund_id.name]
    return df


def transform():
    df = fetch_multisource_fund_info_structured(UPDATE_TIME)
    df["portfolio"] = df["portfolio"].apply(lambda x: re.search("(?P<fund>.*?)（母）(?P<sfund>.*?)（子）", x).groupdict())
    fund_names = dict(zip(df[FundInfoStructured.fund_id.name], df[FundInfoStructured.fund_name.name]))
    portfolios = df["portfolio"].apply(lambda x: {x["fund"]: x["sfund"].split(" ")}).tolist()
    portfolio_ls = []
    for dkt in portfolios:
        for fund, sfunds in dkt.items():
            for sfund in sfunds:
                portfolio_ls.append([fund, sfund])
    df_portfolio = pd.DataFrame(portfolio_ls, columns=[FundInfoStructured.fund_id.name, FundInfoStructured.sfund_id.name]).drop_duplicates()
    df_portfolio.index = df_portfolio[FundInfoStructured.fund_id.name]
    df = df_portfolio.join(df, on=["fund_id"], rsuffix="_")
    df[FundInfoStructured.fund_name.name] = df[FundInfoStructured.fund_id.name].apply(lambda x: fund_names[x])
    df[FundInfoStructured.sfund_name.name] = df[FundInfoStructured.sfund_id.name].apply(lambda x: fund_names[x])

    df_020001 = df.ix[df[FundInfo.data_source.name] == "020001"]
    df_020002 = df.ix[df[FundInfo.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundInfo.data_source.name] == "020003"]

    result = df_020002.join(
        df_020001, how="outer", rsuffix="_020001"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[df_020002.columns].fillna(df_020001).fillna(df_020003)
    result = result.drop([DFundInfoStructured.portfolio.name, "fund_id_"], axis=1)
    return result


def main():
    io.to_sql(FundInfoStructured.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
