import pandas as pd
import re
from sqlalchemy import and_, or_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundInfo
from utils.database.models.base_public import IdMatch, FundInfo, FundTypeMapping

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]


_entities_map = [
    (IdMatch.matched_id, FundInfo.fund_id), (DFundInfo.fund_name, FundInfo.fund_name), (DFundInfo.fund_full_name, FundInfo.fund_full_name),
    (DFundInfo.data_source, FundInfo.data_source), (DFundInfo.foundation_date, FundInfo.foundation_date), (DFundInfo.fund_status, FundInfo.fund_status),
    (DFundInfo.purchase_status, FundInfo.purchase_status), (DFundInfo.purchase_range, FundInfo.purchase_range), (DFundInfo.redemption_status, FundInfo.redemption_status),
    (DFundInfo.aip_status, FundInfo.aip_status), (DFundInfo.recommendation_start, FundInfo.recommendation_start), (DFundInfo.recommendation_end, FundInfo.recommendation_end),
    (DFundInfo.init_raise, FundInfo.init_raise), (DFundInfo.update_time, FundInfo.update_time), (FundTypeMapping.type_name, FundInfo.fund_type)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_info(update_time):
    """
    Fetch records of DOrgInfo table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """
    query_fi = _session.query(IdMatch).join(
        DFundInfo, and_(IdMatch.source_id == DFundInfo.fund_id, IdMatch.data_source == DFundInfo.data_source)
    ).outerjoin(
        FundTypeMapping, and_(IdMatch.matched_id == FundTypeMapping.fund_id, FundTypeMapping.typestandard_code == "02")
    ).filter(
        or_(
            and_(DFundInfo.update_time >= update_time, DFundInfo.is_used == 1, IdMatch.is_used == 1, IdMatch.id_type == 1),
            and_(IdMatch.update_time >= update_time, IdMatch.is_used == 1, IdMatch.id_type == 1, DFundInfo.is_used == 1)
        )
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_fi.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[DFundInfo.fund_id.name]
    return df


def transform():
    df = fetch_multisource_fund_info(UPDATE_TIME).sort_values(
        [FundInfo.fund_id.name, FundInfo.data_source.name, FundInfo.update_time.name], ascending=[True, True, False]
    ).drop_duplicates(subset=[FundInfo.fund_id.name, FundInfo.data_source.name])

    df[FundInfo.init_raise.name] = df[FundInfo.init_raise.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x, "亿"))
    df[FundInfo.fund_status.name] = list(map(lambda x, y: etl.EnumMap.DFundInfo.fund_status.get((x, y)), df[FundInfo.data_source.name], df[FundInfo.fund_status.name]))
    df[FundInfo.purchase_status.name] = list(map(lambda x, y: etl.EnumMap.DFundInfo.purchase_status[(x, y)], df[FundInfo.data_source.name], df[FundInfo.purchase_status.name]))
    df[FundInfo.redemption_status.name] = list(map(lambda x, y: etl.EnumMap.DFundInfo.redemption_status.get((x, y)), df[FundInfo.data_source.name], df[FundInfo.redemption_status.name]))
    df[FundInfo.aip_status.name] = list(map(lambda x, y: etl.EnumMap.DFundInfo.aip_status.get((x, y)), df[FundInfo.data_source.name], df[FundInfo.aip_status.name]))

    df_020001 = df.ix[df[FundInfo.data_source.name] == "020001"]
    df_020002 = df.ix[df[FundInfo.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundInfo.data_source.name] == "020003"]
    patt = "(?P<sta>.*?)(（(?P<rng>.*)）)|(?P<sta2>.*)"
    df_020002[FundInfo.purchase_range.name] = df_020002[FundInfo.purchase_range.name].fillna(df_020002[FundInfo.purchase_status.name].fillna("").apply(lambda x: re.search(patt, x).groupdict().get("rng") if  re.search(patt, x) is not None else None))
    df_020002[FundInfo.purchase_range.name].drop_duplicates()
    df_020002[FundInfo.purchase_status.name] = df_020002[FundInfo.purchase_status.name].fillna("").apply(lambda x: re.search(patt, x).groupdict().get("sta2", re.search(patt, x).groupdict().get("sta")) if re.search(patt, x) is not None else None)
    df_020002[FundInfo.purchase_status.name].drop_duplicates()

    result = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[df_020001.columns].fillna(df_020002).fillna(df_020003)

    result_020002 = df_020002.join(
        df_020001, how="outer", rsuffix="_020001"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[df_020002.columns].fillna(df_020002).fillna(df_020001)

    result[FundInfo.purchase_status.name] = result_020002[FundInfo.purchase_status.name]
    result[FundInfo.purchase_status.name] = list(map(lambda x, y:  "申购关闭(限额)" if y != "" and y is not None else x, result[FundInfo.purchase_status.name], result[FundInfo.purchase_range.name]))

    return result


def main():
    io.to_sql(FundInfo.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
