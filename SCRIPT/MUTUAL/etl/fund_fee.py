import pandas as pd
import re
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundFee
from utils.database.models.base_public import IdMatch, FundInfo, FundFee

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["incremental"]


_entities_map = [
    (FundInfo.fund_id, FundFee.fund_id), (FundInfo.fund_name, FundFee.fund_name), (DFundFee.data_source, FundFee.data_source),
    (DFundFee.fee_subscription, FundFee.fee_subscription), (DFundFee.fee_purchase, FundFee.fee_purchase),
    (DFundFee.fee_redeem, FundFee.fee_redeem), (DFundFee.fee_management, FundFee.fee_management),
    (DFundFee.fee_trust, FundFee.fee_trust), (DFundFee.fee_service, FundFee.fee_service),
]


_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_fee(update_time):
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
        DFundFee, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundFee.fund_id, IdMatch.data_source == DFundFee.data_source)
    ).filter(
        and_(DFundFee.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oh.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundFee.fund_id.name]]
    return df


def transform():
    """

    Args:
        style:

    Returns:

    """
    # General process
    df = fetch_multisource_fund_fee(UPDATE_TIME)

    # Process of different sources
    df_020001 = df.ix[df[FundFee.data_source.name] == "020001"]
    df_020001[FundFee.fee_management.name] = df_020001[FundFee.fee_management.name].apply(lambda x: etl.StringParser.percentage(x))
    df_020001[FundFee.fee_trust.name] = df_020001[FundFee.fee_trust.name].apply(lambda x: etl.StringParser.percentage(x))
    df_020001[FundFee.fee_service.name] = df_020001[FundFee.fee_service.name].apply(lambda x: etl.StringParser.percentage(x))
    df_020001[FundFee.fee_subscription.name] = df_020001[FundFee.fee_subscription.name].apply(
        lambda x: re.search("一.前端认购费率:申购金额（万元）: 申购费率;(?P<sub>.*?);二", x)
    ).apply(
        lambda x: x.groupdict()["sub"] if x is not None else None
    )
    df_020001[FundFee.fee_purchase.name] = df_020001[FundFee.fee_purchase.name].apply(
        lambda x: re.search("一.前端申购费率:申购金额（万元）: 申购费率;(?P<pur>.*?);二", x)
    ).apply(
        lambda x: x.groupdict()["pur"] if x is not None else None
    )
    df_020001[FundFee.fee_redeem.name] = df_020001[FundFee.fee_redeem.name].apply(
        lambda x: re.search("一.前端赎回费率:持有年限: 赎回费率;(?P<red>.*?);二", x)
    ).apply(
        lambda x: x.groupdict()["red"] if x is not None else None
    )
    df_020002 = df.ix[df[FundFee.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundFee.data_source.name] == "020003"]

    result = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[df_020001.columns].fillna(df_020002).fillna(df_020003)
    return result


def main():
    io.to_sql(FundFee.__tablename__, _engine_wt, transform())

if __name__ == "__main__":
    main()
