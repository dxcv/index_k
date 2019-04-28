import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DOrgPosition
from utils.database.models.base_public import IdMatch, OrgInfo, OrgPositionStock, OrgAssetScale

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]

_entities_map = [
    (OrgInfo.org_id, OrgPositionStock.org_id), (OrgInfo.org_name, OrgPositionStock.org_name), (DOrgPosition.data_source, OrgPositionStock.data_source),
    (DOrgPosition.statistic_date, OrgPositionStock.statistic_date), (DOrgPosition.subject_id, OrgPositionStock.subject_id),
    (DOrgPosition.subject_name, OrgPositionStock.subject_name), (DOrgPosition.scale, OrgPositionStock.scale),
    (DOrgPosition.proportion, OrgPositionStock.proportion), (DOrgPosition.quantity, OrgPositionStock.quantity),
    (OrgAssetScale.total_asset, OrgPositionStock.asset_scale)
]


_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_org_position_stock(update_time):
    """
    Fetch records of DOrgAssetScale table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """

    query_oas = _session.query(IdMatch).join(
        OrgInfo, and_(IdMatch.id_type == 2, IdMatch.matched_id == OrgInfo.org_id)
    ).join(
        DOrgPosition, and_(IdMatch.id_type == 2, IdMatch.source_id == DOrgPosition.org_id, IdMatch.data_source == DOrgPosition.data_source)
    ).join(
        OrgAssetScale, and_(IdMatch.id_type == 2, IdMatch.matched_id == OrgAssetScale.org_id, DOrgPosition.statistic_date == OrgAssetScale.statistic_date)
    ).filter(
        and_(DOrgPosition.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oas.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[OrgPositionStock.org_id.name, OrgPositionStock.statistic_date.name]]
    return df


def transform():
    """

    Args:
        style:

    Returns:

    """
    # General process
    df = fetch_multisource_org_position_stock(UPDATE_TIME)
    df[OrgPositionStock.proportion.name] = df[OrgPositionStock.proportion.name].fillna("").apply(lambda x: etl.StringParser.percentage(x))
    df[OrgPositionStock.scale.name] = df[OrgPositionStock.asset_scale.name].apply(lambda x: float(x)) * df[OrgPositionStock.proportion.name]
    df[OrgPositionStock.quantity.name] = df[OrgPositionStock.quantity.name].apply(lambda x: etl.StringParser.num_with_unit(x))

    # Process of different sources
    df_020001 = df.ix[df[OrgPositionStock.data_source.name] == "020001"]
    df_020002 = df.ix[df[OrgPositionStock.data_source.name] == "020002"]
    df_020003 = df.ix[df[OrgPositionStock.data_source.name] == "020003"]

    merged = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    ).fillna(df_020002).fillna(df_020003)[[x.name for x in _output_entities]]

    result = merged
    return result


def main():
    io.to_sql(OrgPositionStock.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
