import datetime as dt
import pandas as pd
from sqlalchemy import and_, or_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundYield
from utils.database.models.base_public import IdMatch, FundInfo, FundYield

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["incremental"]


_entities_map = [
    (FundInfo.fund_id, FundYield.fund_id), (FundInfo.fund_name, FundYield.fund_name),
    (DFundYield.data_source, FundYield.data_source), (DFundYield.statistic_date, FundYield.statistic_date),
    (DFundYield.return_10k, FundYield.return_10k), (DFundYield.d7_return_a, FundYield.d7_return_a)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_yield(update_time):
    """
    Fetch records of DOrgAssetScale table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """

    query_oas = _session.query(IdMatch).join(
        FundInfo, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundInfo.fund_id)
    ).join(
        DFundYield, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundYield.fund_id, IdMatch.data_source == DFundYield.data_source)
    ).filter(
        or_(
            and_(DFundYield.update_time >= update_time, IdMatch.is_used == 1, DFundYield.is_used == 1),
            and_(IdMatch.is_used == 1, IdMatch.id_type == 1, IdMatch.update_time >= update_time, DFundYield.is_used == 1)
        )
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oas.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundYield.fund_id.name, FundYield.statistic_date.name]]
    return df


def transform():
    # General process
    df = fetch_multisource_fund_yield(UPDATE_TIME)

    # Process of different sources
    df_020001 = df.ix[df[FundYield.data_source.name] == "020001"]
    df_020002 = df.ix[df[FundYield.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundYield.data_source.name] == "020003"]

    result = df_020002.join(
        df_020001, how="outer", rsuffix="_020001"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[df_020002.columns].fillna(df_020001).fillna(df_020003)
    return result


def main():
    io.to_sql(FundYield.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
