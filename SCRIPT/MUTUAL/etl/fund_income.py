import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundIncome
from utils.database.models.base_public import IdMatch, FundInfo, FundIncome

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]


_entities_map = [
    (FundInfo.fund_id, FundIncome.fund_id), (FundInfo.fund_name, FundIncome.fund_name),
    (DFundIncome.data_source, FundIncome.data_source), (DFundIncome.statistic_date, FundIncome.statistic_date),
    (DFundIncome.total_income, FundIncome.total_income), (DFundIncome.interest_revenue, FundIncome.interest_revenue),
    (DFundIncome.deposit_interest, FundIncome.deposit_interest),
    (DFundIncome.bonds_interest, FundIncome.bonds_interest),
    (DFundIncome.buying_back_income, FundIncome.buying_back_income),
    (DFundIncome.abs_interest, FundIncome.abs_interest),
    (DFundIncome.investment_income, FundIncome.investment_income),
    (DFundIncome.stock_income, FundIncome.stock_income),
    (DFundIncome.bonds_income, FundIncome.bonds_income),
    (DFundIncome.abs_income, FundIncome.abs_income),
    (DFundIncome.derivatives_income, FundIncome.derivatives_income),
    (DFundIncome.dividend_income, FundIncome.dividend_income),
    (DFundIncome.changes_in_fair_value, FundIncome.changes_in_fair_value),
    (DFundIncome.other_income, FundIncome.other_income),
    (DFundIncome.total_expense, FundIncome.total_expense),
    (DFundIncome.org_compensation, FundIncome.org_compensation),
    (DFundIncome.trustee_expense, FundIncome.trustee_expense),
    (DFundIncome.sales_service_expense, FundIncome.sales_service_expense),
    (DFundIncome.transaction_expense, FundIncome.transaction_expense),
    (DFundIncome.interest_payment, FundIncome.interest_payment),
    (DFundIncome.sold_repurchaseme_payment, FundIncome.sold_repurchaseme_payment),
    (DFundIncome.other_expense, FundIncome.other_expense),
]

_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_income(update_time):
    """
    Fetch records of DOrgAssetScale table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """

    query_fundincome = _session.query(IdMatch).join(
        FundInfo, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundInfo.fund_id)
    ).join(
        DFundIncome, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundIncome.fund_id,
                          IdMatch.data_source == DFundIncome.data_source)
    ).filter(
        and_(DFundIncome.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_fundincome.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundIncome.fund_id.name, FundIncome.statistic_date.name]]
    return df


def transform():
    # General process
    df = fetch_multisource_fund_income(UPDATE_TIME)

    # Process of different sources
    df_020001 = df.ix[df[FundIncome.data_source.name] == "020001"]
    df_020002 = df.ix[df[FundIncome.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundIncome.data_source.name] == "020003"]

    result = df_020001.join(df_020002, how="outer", rsuffix="_020002").join(df_020003, how="outer", rsuffix="_020003")[df_020001.columns]
    result = result.dropna(subset=[FundIncome.fund_id.name])
    return result


def main():
    io.to_sql(FundIncome.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
