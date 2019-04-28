import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundDescription
from utils.database.models.base_public import IdMatch, FundInfo, FundDescription

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["incremental"]

_entities_map = [
    (FundInfo.fund_id, FundDescription.fund_id), (FundInfo.fund_name, FundDescription.fund_name), (DFundDescription.data_source, FundDescription.data_source),
    (DFundDescription.investment_target, FundDescription.investment_target), (DFundDescription.investment_scope, FundDescription.investment_scope),
    (DFundDescription.investment_strategy, FundDescription.investment_strategy), (DFundDescription.investment_idea, FundDescription.investment_idea),
    (DFundDescription.income_distribution, FundDescription.income_distribution), (DFundDescription.risk_return_character, FundDescription.risk_return_character),
    (DFundDescription.comparison_criterion, FundDescription.comparison_criterion), (DFundDescription.guarantee_institution, FundDescription.guarantee_institution),
    (DFundDescription.guarantee_period, FundDescription.guarantee_period), (DFundDescription.guarantee_way, FundDescription.guarantee_way),
    (DFundDescription.tracking_benchmark, FundDescription.tracking_benchmark),
]


_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_desc(update_time):
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
        DFundDescription, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundDescription.fund_id, IdMatch.data_source == DFundDescription.data_source)
    ).filter(
        and_(DFundDescription.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oh.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundDescription.fund_id.name]]
    return df


def transform():
    """

    Args:
        style:

    Returns:

    """
    # General process
    df = fetch_multisource_fund_desc(UPDATE_TIME)

    # Process of different sources
    df_020001 = df.ix[df[FundDescription.data_source.name] == "020001"]
    df_020001.ix[df_020001[FundDescription.risk_return_character.name].apply(lambda x: x == ""), FundDescription.risk_return_character.name] = None
    df_020001.ix[df_020001[FundDescription.guarantee_way.name].apply(lambda x: x == ""), FundDescription.guarantee_way.name] = None
    df_020001.ix[df_020001[FundDescription.guarantee_period.name].apply(lambda x: x == ""), FundDescription.guarantee_period.name] = None
    df_020001.ix[df_020001[FundDescription.guarantee_institution.name].apply(lambda x: x == ""), FundDescription.guarantee_institution.name] = None

    df_020002 = df.ix[df[FundDescription.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundDescription.data_source.name] == "020003"]

    result = df_020002.join(
        df_020001, how="outer", rsuffix="_020001"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[df_020002.columns].fillna(df_020001).fillna(df_020003)
    return result


def main():
    result = transform()
    # _engine_wt.execute("DELETE FROM {tb}".format(tb=FundDescription.__tablename__))
    # result.to_sql("fund_description", _engine_wt, index=False, if_exists="append")
    io.to_sql("fund_description", _engine_wt, result, chunksize=20)


if __name__ == "__main__":
    main()
