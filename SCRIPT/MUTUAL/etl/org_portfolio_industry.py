import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DOrgPortfolioIndustry
from utils.database.models.base_public import IdMatch, OrgInfo, OrgPortfolioIndustry

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]


_entities_map = [
    (OrgInfo.org_id, OrgPortfolioIndustry.org_id), (OrgInfo.org_name, OrgPortfolioIndustry.org_name), (DOrgPortfolioIndustry.data_source, OrgPortfolioIndustry.data_source),
    (DOrgPortfolioIndustry.statistic_date, OrgPortfolioIndustry.statistic_date), (DOrgPortfolioIndustry.type, OrgPortfolioIndustry.type),
    (DOrgPortfolioIndustry.scale, OrgPortfolioIndustry.scale), (DOrgPortfolioIndustry.proportion, OrgPortfolioIndustry.proportion),
    (DOrgPortfolioIndustry.asset_scale, OrgPortfolioIndustry.asset_scale)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = [OrgPortfolioIndustry.style]
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_org_portfolio_industry(update_time):
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
        DOrgPortfolioIndustry, and_(IdMatch.id_type == 2, IdMatch.source_id == DOrgPortfolioIndustry.org_id, IdMatch.data_source == DOrgPortfolioIndustry.data_source)
    ).filter(
        and_(DOrgPortfolioIndustry.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oas.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[OrgPortfolioIndustry.org_id.name, OrgPortfolioIndustry.statistic_date.name]]
    return df


def transform(style):
    """

    Args:
        style:

    Returns:

    """
    MAP_METHOD = {
        1: etl.EnumMap.DOrgPortfolioIndustry.type_sws,
        2: etl.EnumMap.DOrgPortfolioIndustry.type_wind
    }

    # General process
    df = fetch_multisource_org_portfolio_industry(UPDATE_TIME)
    df[OrgPortfolioIndustry.proportion.name] = df[OrgPortfolioIndustry.proportion.name].fillna("").apply(lambda x: etl.StringParser.percentage(x))
    df[OrgPortfolioIndustry.asset_scale.name] = df[OrgPortfolioIndustry.asset_scale.name].fillna("").apply(lambda x: etl.StringParser.num_without_unit(x) * etl.Unit.tr("", "亿"))
    df[OrgPortfolioIndustry.scale.name] = df[OrgPortfolioIndustry.asset_scale.name] * df[OrgPortfolioIndustry.proportion.name]
    df[OrgPortfolioIndustry.style.name] = style
    df[OrgPortfolioIndustry.type.name] = list(map(
        lambda data_source, type: MAP_METHOD[style].get((data_source, type)),
        df[OrgPortfolioIndustry.data_source.name], df[OrgPortfolioIndustry.type.name]
    ))
    df = df.ix[(df[OrgPortfolioIndustry.type.name] != "ERROR") & (df[OrgPortfolioIndustry.proportion.name] != 0)]   # 过滤错误分类数据

    # Process of different sources
    df_020001 = df.ix[df[OrgPortfolioIndustry.data_source.name] == "020001"]
    df_020002 = df.ix[df[OrgPortfolioIndustry.data_source.name] == "020002"]
    df_020003 = df.ix[df[OrgPortfolioIndustry.data_source.name] == "020003"]

    merged = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    ).fillna(df_020002).fillna(df_020003)[[x.name for x in _output_entities]]

    group_by = [OrgPortfolioIndustry.org_id.name, OrgPortfolioIndustry.statistic_date.name, OrgPortfolioIndustry.type.name]
    grouped = merged.drop_duplicates(subset=group_by).groupby(group_by).sum()
    grouped = grouped.reset_index()

    n = dict(zip(merged["org_id"], merged["org_name"]))
    grouped["org_name"] = grouped["org_id"].apply(lambda x: n[x])

    return grouped


def main():
    style = {"sws": 1, "wind": 2}
    for s in style:
        io.to_sql(OrgPortfolioIndustry.__tablename__, _engine_wt, transform(style[s]))


if __name__ == "__main__":
    main()
