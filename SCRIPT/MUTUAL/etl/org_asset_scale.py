import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DOrgAssetScale
from utils.database.models.base_public import IdMatch, OrgInfo, OrgAssetScale

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["incremental"]



_entities_map = [
    (OrgInfo.org_id, OrgAssetScale.org_id), (OrgInfo.org_name, OrgAssetScale.org_name), (DOrgAssetScale.data_source, OrgAssetScale.data_source),
    (DOrgAssetScale.statistic_date, OrgAssetScale.statistic_date), (DOrgAssetScale.total_asset, OrgAssetScale.total_asset), (DOrgAssetScale.funds_num, OrgAssetScale.funds_num)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_org_asset_scale(update_time):
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
        DOrgAssetScale, and_(IdMatch.id_type == 2, IdMatch.source_id == DOrgAssetScale.org_id, IdMatch.data_source == DOrgAssetScale.data_source)
    ).filter(
        and_(DOrgAssetScale.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oas.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[OrgAssetScale.org_id.name, OrgAssetScale.statistic_date.name]]
    return df


def transform():
    df = fetch_multisource_org_asset_scale(UPDATE_TIME)
    df[DOrgAssetScale.total_asset.name] = df[DOrgAssetScale.total_asset.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x, "亿"))

    df_020001 = df.ix[df[OrgAssetScale.data_source.name] == "020001"]
    df_020002 = df.ix[df[OrgAssetScale.data_source.name] == "020002"]
    df_020003 = df.ix[df[OrgAssetScale.data_source.name] == "020003"]

    result = df_020002.join(
        df_020001, how="outer", rsuffix="_020001"
    ).join(
        df_020002, how="outer", rsuffix="_020003"
    ).fillna(df_020001).fillna(df_020003)[[x.name for x in _output_entities]]
    return result


def main():
    io.to_sql(OrgAssetScale.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
