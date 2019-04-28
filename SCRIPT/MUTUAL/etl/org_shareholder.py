import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DOrgShareholder
from utils.database.models.base_public import IdMatch, OrgInfo, OrgShareholder

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]



_entities_map = [
    (OrgInfo.org_id, OrgShareholder.org_id), (OrgInfo.org_name, OrgShareholder.org_name), (DOrgShareholder.data_source, OrgShareholder.data_source),
    (DOrgShareholder.statistic_date, OrgShareholder.statistic_date), (DOrgShareholder.shareholder_name, OrgShareholder.shareholder_name),
    (DOrgShareholder.shareholder_num, OrgShareholder.shareholder_num), (DOrgShareholder.capital_stock, OrgShareholder.capital_stock),
    (DOrgShareholder.stock_held, OrgShareholder.stock_held), (DOrgShareholder.proportion_held, OrgShareholder.proportion_held),
]


_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_org_shareholder(update_time):
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
        DOrgShareholder, and_(IdMatch.id_type == 2, IdMatch.source_id == DOrgShareholder.org_id, IdMatch.data_source == DOrgShareholder.data_source)
    ).filter(
        and_(DOrgShareholder.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oas.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[OrgShareholder.org_id.name, OrgShareholder.statistic_date.name]]
    return df


def transform():
    """

    Args:
        style:

    Returns:

    """
    # General process
    df = fetch_multisource_org_shareholder(UPDATE_TIME)
    df[OrgShareholder.proportion_held.name] = df[OrgShareholder.proportion_held.name].fillna("").apply(lambda x: etl.StringParser.percentage(x))
    df[OrgShareholder.capital_stock.name] = df[OrgShareholder.capital_stock.name].apply(lambda x: etl.StringParser.num_with_unit(x))
    df[OrgShareholder.stock_held.name] = df[OrgShareholder.stock_held.name].apply(lambda x: etl.StringParser.num_with_unit(x))

    # Process of different sources
    df_020001 = df.ix[df[OrgShareholder.data_source.name] == "020001"]
    df_020002 = df.ix[df[OrgShareholder.data_source.name] == "020002"]
    df_020003 = df.ix[df[OrgShareholder.data_source.name] == "020003"]

    merged = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    ).fillna(df_020002).fillna(df_020003)[[x.name for x in _output_entities]]

    result = merged
    return result


def main():
    io.to_sql(OrgShareholder.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
