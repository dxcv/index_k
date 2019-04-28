import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DOrgPortfolioAsset
from utils.database.models.base_public import IdMatch, OrgInfo, OrgPortfolioAsset

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]

_entities_map = [
    (OrgInfo.org_id, DOrgPortfolioAsset.org_id), (OrgInfo.org_name, DOrgPortfolioAsset.org_name), (DOrgPortfolioAsset.data_source, OrgPortfolioAsset.data_source),
    (DOrgPortfolioAsset.statistic_date, OrgPortfolioAsset.statistic_date), (DOrgPortfolioAsset.asset_type, OrgPortfolioAsset.asset_stype), (DOrgPortfolioAsset.proportion, OrgPortfolioAsset.proportion),
    (DOrgPortfolioAsset.asset_scale, OrgPortfolioAsset.asset_scale)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = [OrgPortfolioAsset.scale, OrgPortfolioAsset.asset_type]
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_org_portfolio_asset(update_time):
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
        DOrgPortfolioAsset, and_(IdMatch.id_type == 2, IdMatch.source_id == DOrgPortfolioAsset.org_id, IdMatch.data_source == DOrgPortfolioAsset.data_source)
    ).filter(
        and_(DOrgPortfolioAsset.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oas.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[OrgPortfolioAsset.org_id.name, OrgPortfolioAsset.statistic_date.name]]
    return df


def transform():
    # General process
    df = fetch_multisource_org_portfolio_asset(UPDATE_TIME)
    df[OrgPortfolioAsset.proportion.name] = df[OrgPortfolioAsset.proportion.name].fillna("").apply(lambda x: etl.StringParser.percentage(x))
    df[OrgPortfolioAsset.asset_scale.name] = df[OrgPortfolioAsset.asset_scale.name].fillna("").apply(lambda x: etl.StringParser.num_without_unit(x) * etl.Unit.tr("", "亿"))
    df[OrgPortfolioAsset.scale.name] = df[OrgPortfolioAsset.asset_scale.name] * df[OrgPortfolioAsset.proportion.name]

    # Process of different sources
    df_020001 = df.ix[df[DOrgPortfolioAsset.data_source.name] == "020001"]
    df_020002 = df.ix[df[DOrgPortfolioAsset.data_source.name] == "020002"]
    df_020003 = df.ix[df[DOrgPortfolioAsset.data_source.name] == "020003"]

    df_020001[OrgPortfolioAsset.asset_type.name] = list(map(
        lambda data_source, asset_stype: etl.EnumMap.DOrgPortfolioAsset.type.get((data_source, asset_stype)),
        df_020001[OrgPortfolioAsset.data_source.name], df_020001[OrgPortfolioAsset.asset_stype.name]
    ))

    df_020001[OrgPortfolioAsset.asset_stype.name] = list(map(
        lambda data_source, asset_stype: etl.EnumMap.DOrgPortfolioAsset.stype.get((data_source, asset_stype)),
        df_020001[OrgPortfolioAsset.data_source.name], df_020001[OrgPortfolioAsset.asset_stype.name]
    ))

    merged = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    ).fillna(df_020002).fillna(df_020003)[[x.name for x in _output_entities]]

    group_by = [OrgPortfolioAsset.org_id.name, OrgPortfolioAsset.statistic_date.name, OrgPortfolioAsset.asset_type.name, OrgPortfolioAsset.asset_stype.name]
    grouped = merged.groupby(group_by).sum()

    # idx = pd.DataFrame(grouped.index)
    result = merged.drop_duplicates(subset=group_by).sort_values(group_by)
    result["proportion"] = grouped["proportion"].tolist()
    result["scale"] = grouped["scale"].tolist()

    return result


def main():
    io.to_sql(OrgPortfolioAsset.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
