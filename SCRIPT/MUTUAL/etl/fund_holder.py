import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundHolder
from utils.database.models.base_public import IdMatch, FundInfo, FundHolder

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]

_entities_map = [
    (FundInfo.fund_id, FundHolder.fund_id), (FundInfo.fund_name, FundHolder.fund_name), (DFundHolder.data_source, FundHolder.data_source),
    (DFundHolder.statistic_date, FundHolder.statistic_date), (DFundHolder.holder_type, FundHolder.holder_type),
    (DFundHolder.proportion_held, FundHolder.proportion_held), (DFundHolder.share_held, FundHolder.share_held),
    (DFundHolder.holder_num, FundHolder.holder_num), (DFundHolder.total_share, FundHolder.total_share),
]


_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_holder(update_time):
    """
    Fetch records of DOrgAssetScale table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """

    query_oh = _session.query(IdMatch).join(
        FundInfo, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundInfo.fund_id)
    ).join(
        DFundHolder, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundHolder.fund_id, IdMatch.data_source == DFundHolder.data_source)
    ).filter(
        and_(DFundHolder.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oh.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundHolder.fund_id.name, FundHolder.statistic_date.name, FundHolder.holder_type.name]]
    return df


def transform():
    """

    Args:
        style:

    Returns:

    """
    # General process
    df = fetch_multisource_fund_holder(UPDATE_TIME)
    df = df.dropna(subset=[FundHolder.fund_id.name])
    df[FundHolder.proportion_held.name] = df[FundHolder.proportion_held.name].fillna("").apply(lambda x: etl.StringParser.percentage(x))
    df[FundHolder.share_held.name] = df[FundHolder.share_held.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x, "亿"))
    df[FundHolder.total_share.name] = df[FundHolder.total_share.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x, "亿"))
    df[FundHolder.holder_type.name] = list(map(lambda data_source, holder_type: etl.EnumMap.DFundHolder.holder_type[(data_source, holder_type)], df[FundHolder.data_source.name], df[FundHolder.holder_type.name]))

    # Process of different sources
    df_020001 = df.ix[df[FundHolder.data_source.name] == "020001"]
    df_020002 = df.ix[df[FundHolder.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundHolder.data_source.name] == "020003"]

    result = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[df_020001.columns].fillna(df_020002).fillna(df_020003)
    result = result.dropna(subset=[FundHolder.proportion_held.name])
    return result


def main():
    io.to_sql(FundHolder.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
