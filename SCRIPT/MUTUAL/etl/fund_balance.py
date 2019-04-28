import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.algorithm import etl
from utils.database import io, config as cfg
from utils.database.models.crawl_public import DFundBalance
from utils.database.models.base_public import IdMatch, FundInfo, FundBalance

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["incremental"]


_entities_map = [
    (FundInfo.fund_id, FundBalance.fund_id), (FundInfo.fund_name, FundBalance.fund_name),
    (DFundBalance.data_source, FundBalance.data_source), (DFundBalance.statistic_date, FundBalance.statistic_date),
    (DFundBalance.bank_deposit, FundBalance.bank_deposit), (DFundBalance.provision_settlement_fund, FundBalance.provision_settlement_fund),
    (DFundBalance.refundable_deposits, FundBalance.refundable_deposits),
    (DFundBalance.transaction_monetary_assets, FundBalance.transaction_monetary_assets),
    (DFundBalance.stock_income, FundBalance.stock_income),
    (DFundBalance.bonds_income, FundBalance.bonds_income),
    (DFundBalance.abs_income, FundBalance.abs_income),
    (DFundBalance.derivatives_income, FundBalance.derivatives_income),
    (DFundBalance.buying_back_income, FundBalance.buying_back_income),
    (DFundBalance.securities_settlement_receivable, FundBalance.securities_settlement_receivable),
    (DFundBalance.interest_revenue, FundBalance.interest_revenue),
    (DFundBalance.dividend_income, FundBalance.dividend_income),
    (DFundBalance.subscription_receivable, FundBalance.subscription_receivable),
    (DFundBalance.other_assets, FundBalance.other_assets),
    (DFundBalance.total_assets, FundBalance.total_assets),
    (DFundBalance.short_term_loan, FundBalance.short_term_loan),
    (DFundBalance.transaction_financial_liabilities, FundBalance.transaction_financial_liabilities),
    (DFundBalance.derivative_financial_liabilities, FundBalance.derivative_financial_liabilities),
    (DFundBalance.sold_repurchase_payment, FundBalance.sold_repurchase_payment),
    (DFundBalance.securities_settlement_payable, FundBalance.securities_settlement_payable),
    (DFundBalance.redemption_payable, FundBalance.redemption_payable),
    (DFundBalance.org_compensation_payable, FundBalance.org_compensation_payable),
    (DFundBalance.trustee_payable, FundBalance.trustee_payable),
    (DFundBalance.sales_service_payable, FundBalance.sales_service_payable),
    (DFundBalance.transaction_payable, FundBalance.transaction_payable),
    (DFundBalance.tax_payable, FundBalance.tax_payable),
    (DFundBalance.interest_payable, FundBalance.interest_payable),
    (DFundBalance.profit_payable, FundBalance.profit_payable),
    (DFundBalance.other_liabilities, FundBalance.other_liabilities),
    (DFundBalance.paid_up_capital, FundBalance.paid_up_capital),
    (DFundBalance.undistributed_profit, FundBalance.undistributed_profit),
    (DFundBalance.owner_equity, FundBalance.owner_equity),
    (DFundBalance.total_liabilities_and_owners_equity, FundBalance.total_liabilities_and_owners_equity),
]

_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_balance(update_time):
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
        DFundBalance, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundBalance.fund_id,
                          IdMatch.data_source == DFundBalance.data_source, IdMatch.is_used == 1)
    ).filter(
        DFundBalance.update_time >= update_time
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_fundincome.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundBalance.fund_id.name, FundBalance.statistic_date.name]]
    return df


def transform():
    # General process
    df = fetch_multisource_fund_balance(UPDATE_TIME)

    # Process of different sources
    df_020001 = df.ix[df[FundBalance.data_source.name] == "020001"]
    df_020002 = df.ix[df[FundBalance.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundBalance.data_source.name] == "020003"]

    result = df_020001.join(df_020002, how="outer", rsuffix="_020002").join(df_020003, how="outer", rsuffix="_020003")[df_020001.columns]
    result = result.dropna(subset=[FundBalance.fund_id.name])
    return result


def main():
    io.to_sql(FundBalance.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
