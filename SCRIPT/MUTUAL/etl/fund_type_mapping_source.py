import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundInfo
from utils.database.models.base_public import IdMatch, FundInfo, FundTypeMappingSource

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = dt.date.today() - dt.timedelta(1)
UPDATE_TIME = etl.update_time["all"]



_entities_map = [
    (FundInfo.fund_id, FundTypeMappingSource.fund_id), (FundInfo.fund_name, FundTypeMappingSource.fund_name),
    (DFundInfo.data_source, FundTypeMappingSource.data_source), (DFundInfo.fund_type, DFundInfo.fund_type),
]


_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = [FundTypeMappingSource.typestandard_code, FundTypeMappingSource.typestandard_name, FundTypeMappingSource.type_code, FundTypeMappingSource.type_name]
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_type(update_time):
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
        DFundInfo, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundInfo.fund_id, IdMatch.data_source == DFundInfo.data_source)
    ).filter(
        and_(DFundInfo.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oh.all())
    df.columns = [x.name for x in _map_entities]
    return df


def transform(typestandard):
    """

    Args:
        style:

    Returns:

    """
    # General process
    MapMethod = {
        "02": etl.EnumMap.DFundInfo.type_code2,
        "04": etl.EnumMap.DFundInfo.type_code4
    }

    df = fetch_multisource_fund_type(UPDATE_TIME)
    df[FundTypeMappingSource.type_code.name] = list(map(
        lambda ds, tp: MapMethod[typestandard].get((ds, tp)), df[DFundInfo.data_source.name], df[DFundInfo.fund_type.name]
    ))
    df[FundTypeMappingSource.type_name.name] = df[FundTypeMappingSource.type_code.name].apply(lambda x: etl.Enum.TypeMapping.type_name.get(x))
    df[FundTypeMappingSource.typestandard_code.name] = df[FundTypeMappingSource.type_code.name].apply(lambda x: x[:-2] if x is not None else None)
    df[FundTypeMappingSource.typestandard_name.name] = df[FundTypeMappingSource.typestandard_code.name].apply(lambda x: etl.Enum.TypeMapping.typestandard_name.get(x))
    df = df.dropna(subset=[FundTypeMappingSource.typestandard_code.name])
    # df.index = df[[FundTypeMappingSource.fund_id.name, FundTypeMappingSource.typestandard_code.name]]

    # # Process of different sources
    # df_020001 = df.ix[df[FundTypeMappingSource.data_source.name] == "020001"]
    # df_020002 = df.ix[df[FundTypeMappingSource.data_source.name] == "020002"]
    # df_020003 = df.ix[df[FundTypeMappingSource.data_source.name] == "020003"]
    #
    # result = df_020001.join(
    #     df_020002, how="outer", rsuffix="_020002"
    # ).join(
    #     df_020003, how="outer", rsuffix="_020003"
    # )[df_020001.columns].fillna(df_020001).fillna(df_020003)
    # result = result[[x.name for x in _output_entities]].drop(DFundInfo.fund_type.name, axis=1)
    result = df.drop(DFundInfo.fund_type.name, axis=1)
    return result


def main():
    for method in ("02", "04"):
        result = transform(method)
        # io.to_sql(FundTypeMappingSource.__tablename__, _engine_wt, result)
        io.to_sql("fund_type_mapping_source_test", _engine_wt, result)
        break

if __name__ == "__main__":
    main()
