import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundPortfolioIndustry
from utils.database.models.base_public import IdMatch, FundInfo, FundAssetScale, FundPortfolioIndustry

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]

_entities_map = [
    (FundInfo.fund_id, FundPortfolioIndustry.fund_id), (FundInfo.fund_name, FundPortfolioIndustry.fund_name), (DFundPortfolioIndustry.data_source, FundPortfolioIndustry.data_source),
    (DFundPortfolioIndustry.statistic_date, FundPortfolioIndustry.statistic_date), (DFundPortfolioIndustry.type, FundPortfolioIndustry.type),
    (DFundPortfolioIndustry.scale, FundPortfolioIndustry.scale), (DFundPortfolioIndustry.proportion, FundPortfolioIndustry.proportion),
    (FundAssetScale.total_asset, FundPortfolioIndustry.asset_scale)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = [FundPortfolioIndustry.style]
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_portfolio_industry(update_time):
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
        DFundPortfolioIndustry, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundPortfolioIndustry.fund_id, IdMatch.data_source == DFundPortfolioIndustry.data_source)
    ).filter(
        and_(DFundPortfolioIndustry.asset_type=="股票", DFundPortfolioIndustry.update_time >= update_time, IdMatch.is_used == 1)
    ).join(
        FundAssetScale, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundAssetScale.fund_id, DFundPortfolioIndustry.statistic_date == FundAssetScale.statistic_date)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oas.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundPortfolioIndustry.fund_id.name, FundPortfolioIndustry.statistic_date.name]]
    return df


def transform(style):
    """

    Args:
        style:

    Returns:

    """
    MAP_METHOD = {
        1: etl.EnumMap.DFundPortfolioIndustry.type_sws,
        2: etl.EnumMap.DFundPortfolioIndustry.type_wind
    }

    # General process
    df = fetch_multisource_fund_portfolio_industry(UPDATE_TIME)
    df[FundPortfolioIndustry.style.name] = style

    # 不在字典中的分类设置为ERROR并drop
    df[FundPortfolioIndustry.type.name] = list(map(
        lambda data_source, type: MAP_METHOD[style].get((data_source, type), "ERROR"),
        df[FundPortfolioIndustry.data_source.name], df[FundPortfolioIndustry.type.name]
    ))

    df.index = df[[FundPortfolioIndustry.fund_id.name, FundPortfolioIndustry.statistic_date.name, FundPortfolioIndustry.type.name]]

    # Process of different sources
    df_020001 = df.ix[df[FundPortfolioIndustry.data_source.name] == "020001"]

    df_020002 = df.ix[df[FundPortfolioIndustry.data_source.name] == "020002"]
    df_020002[FundPortfolioIndustry.scale.name] = df_020002[FundPortfolioIndustry.scale.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x, "万"))
    df_020002[FundPortfolioIndustry.proportion.name] = df_020002[FundPortfolioIndustry.proportion.name].fillna("").apply(lambda x: etl.StringParser.percentage(x))
    # df_020002[FundPortfolioIndustry.asset_scale.name] = (df_020002[FundPortfolioIndustry.scale.name] / df_020002[FundPortfolioIndustry.proportion.name]) * etl.Unit.tr("万", "亿")
    df_020002 = df_020002.ix[(df_020002[FundPortfolioIndustry.type.name] != "ERROR") & (df_020002[FundPortfolioIndustry.proportion.name] != 0)]   # 过滤错误分类数据

    df_020003 = df.ix[df[FundPortfolioIndustry.data_source.name] == "020003"]

    merged = df_020002.copy()

    group_by = [FundPortfolioIndustry.fund_id.name, FundPortfolioIndustry.statistic_date.name, FundPortfolioIndustry.type.name]
    grouped = merged.groupby(group_by).sum()

    result = merged.drop_duplicates(subset=group_by).sort_values(group_by)
    result["proportion"] = grouped["proportion"].tolist()
    result["scale"] = grouped["scale"].tolist()

    return result


def main():
    # style = {"sws": 1, "wind": 2}
    style = {"sws": 1}
    for s in style:
        io.to_sql(FundPortfolioIndustry.__tablename__, _engine_wt, transform(style[s]))


if __name__ == "__main__":
    main()
