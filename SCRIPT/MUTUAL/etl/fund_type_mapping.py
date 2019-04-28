import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.base_public import FundTypeMappingSource, FundTypeMapping

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]

_entities_map = [
    (FundTypeMappingSource.fund_id, FundTypeMapping.fund_id),
    (FundTypeMappingSource.fund_name, FundTypeMapping.fund_name),
    (FundTypeMappingSource.data_source, FundTypeMapping.data_source),
    (FundTypeMappingSource.typestandard_code, FundTypeMapping.typestandard_code),
    (FundTypeMappingSource.typestandard_name, FundTypeMapping.typestandard_name),
    (FundTypeMappingSource.type_code, FundTypeMapping.type_code),
    (FundTypeMappingSource.type_name, FundTypeMapping.type_name),
    (FundTypeMappingSource.stype_code, FundTypeMapping.stype_code),
    (FundTypeMappingSource.stype_name, FundTypeMapping.stype_name),
]


_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_type(update_time):
    """
    Fetch records of DOrgAssetScale table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """

    query_oh = _session.query(FundTypeMappingSource).filter(
        FundTypeMappingSource.update_time >= update_time
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oh.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundTypeMappingSource.fund_id.name, FundTypeMappingSource.typestandard_code.name]]
    return df


def transform():
    """

    Args:
        style:

    Returns:

    """
    # General process
    df = fetch_multisource_fund_type(UPDATE_TIME)

    # Process of different sources
    df_000001 = df.ix[df[FundTypeMappingSource.data_source.name] == "000001"]
    df_020001 = df.ix[df[FundTypeMappingSource.data_source.name] == "020001"]
    df_020002 = df.ix[df[FundTypeMappingSource.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundTypeMappingSource.data_source.name] == "020003"]

    result = df_000001.join(
        df_020001, how="outer", rsuffix="_020001"
    ).join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[df_020001.columns].fillna(df_020002).fillna(df_020003)
    return result


def main():
    io.to_sql(FundTypeMapping.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
