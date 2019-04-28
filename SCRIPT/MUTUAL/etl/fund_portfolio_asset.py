import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundPortfolioAsset
from utils.database.models.base_public import IdMatch, FundInfo, FundPortfolioAsset

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["incremental"]


_entities_map = [
    (FundInfo.fund_id, FundPortfolioAsset.fund_id), (FundInfo.fund_name, FundPortfolioAsset.fund_name),
    (DFundPortfolioAsset.data_source, FundPortfolioAsset.data_source), (DFundPortfolioAsset.statistic_date, FundPortfolioAsset.statistic_date),
    (DFundPortfolioAsset.type, FundPortfolioAsset.asset_stype), (DFundPortfolioAsset.proportion, FundPortfolioAsset.proportion),
    (DFundPortfolioAsset.asset_scale, FundPortfolioAsset.asset_scale)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = [DFundPortfolioAsset.scale, FundPortfolioAsset.asset_stype]
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_portfolio_asset(update_time):
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
        DFundPortfolioAsset, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundPortfolioAsset.fund_id, IdMatch.data_source == DFundPortfolioAsset.data_source)
    ).filter(
        and_(DFundPortfolioAsset.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oas.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundPortfolioAsset.fund_id.name, FundPortfolioAsset.statistic_date.name]]
    return df


def transform():
    # General process
    df = fetch_multisource_fund_portfolio_asset(UPDATE_TIME)
    df[FundPortfolioAsset.proportion.name] = df[FundPortfolioAsset.proportion.name].fillna("").apply(lambda x: etl.StringParser.percentage(x))
    df = df.dropna(subset=["proportion", "asset_scale"], how="any")


    # Process of different sources
    df_020001 = df.ix[df[DFundPortfolioAsset.data_source.name] == "020001"]
    df_020001[FundPortfolioAsset.asset_scale.name] = df_020001[FundPortfolioAsset.asset_scale.name].apply(lambda x: etl.StringParser.num_without_unit(x)) * etl.Unit.tr("", "亿")
    df_020001[FundPortfolioAsset.asset_type.name] = list(map(
        lambda data_source, asset_stype: etl.EnumMap.DFundPortfolioAsset.type.get((data_source, asset_stype)),
        df_020001[FundPortfolioAsset.data_source.name], df_020001[FundPortfolioAsset.asset_stype.name]
    ))
    df_020001[FundPortfolioAsset.asset_stype.name] = list(map(
        lambda data_source, asset_stype: etl.EnumMap.DFundPortfolioAsset.stype.get((data_source, asset_stype)),
        df_020001[FundPortfolioAsset.data_source.name], df_020001[FundPortfolioAsset.asset_stype.name]
    ))

    df_020002 = df.ix[df[DFundPortfolioAsset.data_source.name] == "020002"]
    df_020002[FundPortfolioAsset.asset_scale.name] = df_020002[FundPortfolioAsset.asset_scale.name].apply(lambda x: etl.StringParser.num_with_unit(x, "亿"))

    df_020003 = df.ix[df[DFundPortfolioAsset.data_source.name] == "020003"]

    merged = df_020001
    group_by = [FundPortfolioAsset.fund_id.name, FundPortfolioAsset.statistic_date.name, FundPortfolioAsset.asset_type.name, FundPortfolioAsset.asset_stype.name]
    grouped = merged.groupby(group_by).sum()

    # idx = pd.DataFrame(grouped.index)
    result = merged.drop_duplicates(subset=group_by).sort_values(group_by)
    result[FundPortfolioAsset.proportion.name] = grouped[FundPortfolioAsset.proportion.name].tolist()
    result[FundPortfolioAsset.scale.name] = result[FundPortfolioAsset.proportion.name] * result[FundPortfolioAsset.asset_scale.name]
    return result


def main():
    io.to_sql(FundPortfolioAsset.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
