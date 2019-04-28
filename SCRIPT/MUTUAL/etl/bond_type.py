import pandas as pd
from utils.algorithm import etl
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.database.models.crawl_public import DFundNv
from utils.database.models.base_public import IdMatch, FundInfo, FundNvSource, FundNv

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]

_entities_map = [
    (FundInfo.fund_id, FundNv.fund_id), (FundInfo.fund_name, FundNv.fund_name), (FundNvSource.data_source, FundNv.data_source),
    (FundNvSource.statistic_date, FundNv.statistic_date), (FundNvSource.nav, FundNv.nav), (FundNvSource.added_nav, FundNv.added_nav),
    (FundNvSource.swanav, FundNv.swanav)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_nv(update_time):
    """
    Fetch records of DOrgInfo table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """
    query_fnv = _session.query(IdMatch).join(
        FundInfo, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundInfo.fund_id)
    ).join(
        DFundNv, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundNv.fund_id, IdMatch.data_source == DFundNv.data_source)
    ).filter(
        and_(DFundNv.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_fnv.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundInfo.fund_id.name, FundNvSource.statistic_date.name]]
    return df


def transform():
    df = fetch_multisource_nv(UPDATE_TIME)

    df_020001 = df.ix[df[FundInfo.data_source.name] == "020001"]
    df_020002 = df.ix[df[FundInfo.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundInfo.data_source.name] == "020003"]

    result = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[df_020001.columns].fillna(df_020002).fillna(df_020003)

    return result


def main():
    io.to_sql(FundNvSource.__tablename__, _engine_wt, transform())

if __name__ == "__main__":
    main()
