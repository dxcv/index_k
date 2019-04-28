import pandas as pd
from sqlalchemy import and_, or_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundNv
from utils.database.models.base_public import IdMatch, FundInfo, FundNvSource

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["incremental"]

_entities_map = [
    (IdMatch.matched_id, FundInfo.fund_id), (DFundNv.fund_name, FundInfo.fund_name), (DFundNv.data_source, FundNvSource.data_source),
    (DFundNv.statistic_date, FundNvSource.statistic_date), (DFundNv.nav, FundNvSource.nav), (DFundNv.added_nav, FundNvSource.added_nav),
    (DFundNv.swanav, FundNvSource.swanav)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_nv(update_time_l, update_time_r=None):
    """
    Fetch records of DFundNv table where record update time >= `update_time`
    Args:
        update_time_l: record update time

    Returns:
        pandas.DataFrame
    """
    if update_time_r is None:
        condition = or_(
            and_(DFundNv.update_time >= update_time_l, IdMatch.is_used == 1, DFundNv.is_used == 1),
            and_(IdMatch.is_used == 1, IdMatch.id_type == 1, IdMatch.update_time >= update_time_l, DFundNv.is_used == 1)
        )
    else:
        condition = or_(
            and_(DFundNv.update_time >= update_time_l, DFundNv.update_time <= update_time_r, IdMatch.is_used == 1),
            and_(IdMatch.is_used == 1, IdMatch.id_type == 1, IdMatch.update_time >= update_time_l, IdMatch.update_time <= update_time_r)
        )

    query_fnv = _session.query(IdMatch).join(
        FundInfo, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundInfo.fund_id)
    ).join(
        DFundNv, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundNv.fund_id, IdMatch.data_source == DFundNv.data_source)
    ).filter(
        condition
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_fnv.all())
    df.columns = [x.name for x in _map_entities]
    return df


def transform():
    df = fetch_multisource_nv(UPDATE_TIME)
    result = df

    return result


def tmpsync():
    import datetime as dt
    import pandas as pd
    rngs = pd.date_range(dt.datetime(2017, 7, 13), dt.datetime(2017, 12, 14), freq="6H")
    rngs = [dt.datetime.fromtimestamp(x.timestamp()) for x in rngs]
    for i in range(len(rngs) - 1):
        print(rngs[i])
        try:
            df = fetch_multisource_nv(rngs[i], rngs[i + 1])
            io.to_sql(FundNvSource.__tablename__, _engine_wt, df)
        except ValueError as e:
            continue


def main():
    io.to_sql(FundNvSource.__tablename__, _engine_wt, transform())

if __name__ == "__main__":
    main()
