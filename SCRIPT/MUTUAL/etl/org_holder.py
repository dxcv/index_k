import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DOrgHolder
from utils.database.models.base_public import IdMatch, OrgInfo, OrgHolder

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]


_entities_map = [
    (OrgInfo.org_id, OrgHolder.org_id), (OrgInfo.org_name, OrgHolder.org_name), (DOrgHolder.data_source, OrgHolder.data_source),
    (DOrgHolder.statistic_date, OrgHolder.statistic_date), (DOrgHolder.holder_type, OrgHolder.holder_type),
    (DOrgHolder.proportion_held, OrgHolder.proportion_held), (DOrgHolder.share_held, OrgHolder.share_held),
    (DOrgHolder.holder_num, OrgHolder.holder_num), (DOrgHolder.total_scale, OrgHolder.total_scale),
]


_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_org_holder(update_time):
    """
    Fetch records of DOrgAssetScale table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """

    query_oh = _session.query(IdMatch).join(
        OrgInfo, and_(IdMatch.id_type == 2, IdMatch.matched_id == OrgInfo.org_id)
    ).join(
        DOrgHolder, and_(IdMatch.id_type == 2, IdMatch.source_id == DOrgHolder.org_id, IdMatch.data_source == DOrgHolder.data_source)
    ).filter(
        and_(DOrgHolder.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oh.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[OrgHolder.org_id.name, OrgHolder.statistic_date.name, OrgHolder.holder_type.name]]
    return df


def transform():
    """

    Args:
        style:

    Returns:

    """
    # General process
    df = fetch_multisource_org_holder(UPDATE_TIME)
    df[OrgHolder.proportion_held.name] = df[OrgHolder.proportion_held.name].fillna("").apply(lambda x: etl.StringParser.percentage(x))
    df[OrgHolder.total_scale.name] = df[OrgHolder.total_scale.name].apply(lambda x: etl.StringParser.num_with_unit(x, "亿"))
    df[OrgHolder.holder_type.name] = list(map(lambda data_source, holder_type: etl.EnumMap.DOrgHolder.holder_type[(data_source, holder_type)], df[OrgHolder.data_source.name], df[OrgHolder.holder_type.name]))

    # Process of different sources
    df_020001 = df.ix[df[OrgHolder.data_source.name] == "020001"]
    df_020002 = df.ix[df[OrgHolder.data_source.name] == "020002"]
    df_020003 = df.ix[df[OrgHolder.data_source.name] == "020003"]

    result = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    ).fillna(df_020002).fillna(df_020003)[[x.name for x in _output_entities]]
    result = result.dropna(subset=[OrgHolder.proportion_held.name])

    return result


def main():
    io.to_sql(OrgHolder.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
