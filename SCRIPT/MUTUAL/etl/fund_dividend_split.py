from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from SCRIPT.MUTUAL.etl import fund_dividend, fund_split
from utils.database.models.base_public import FundDividendSplit

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()


def transform():
    df_dividend = fund_dividend.transform()
    df_dividend[FundDividendSplit.type.name] = "分红"
    df_dividend.columns = [x if x != "dividend_at" else FundDividendSplit.value.name for x in df_dividend.columns]
    df_split = fund_split.transform()
    df_split[FundDividendSplit.type.name] = "拆分"
    df_split.columns = [x if x != "split_ratio" else FundDividendSplit.value.name for x in df_split.columns]
    for x in [x for x in df_split if x not in df_dividend.columns]:
        df_dividend[x] = None
    for x in [x for x in df_dividend if x not in df_split.columns]:
        df_split[x] = None

    result = df_dividend.append(df_split)

    return result


def main():
    io.to_sql(FundDividendSplit.__tablename__, _engine_wt, transform())

if __name__ == "__main__":
    main()
