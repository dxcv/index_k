import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.algorithm import etl
from utils.database import io, config as cfg
from utils.database.models.crawl_public import DFundDividend
from utils.database.models.base_public import IdMatch, FundInfo, FundDividend

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]


_entities_map = [
    (FundInfo.fund_id, FundDividend.fund_id), (FundInfo.fund_name, FundDividend.fund_name), (DFundDividend.data_source, FundDividend.data_source),
    (DFundDividend.statistic_date, FundDividend.statistic_date), (DFundDividend.record_date, FundDividend.record_date),
    (DFundDividend.ex_dividend_date, FundDividend.ex_dividend_date), (DFundDividend.dividend_date, FundDividend.dividend_date),
    (DFundDividend.dividend_at, FundDividend.dividend_at)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_dividend(update_time):
    """
    Fetch records of DOrgInfo table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """
    query_fnv = _session.query(IdMatch).join(
        FundInfo, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundInfo.fund_id)
    ).join(
        DFundDividend, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundDividend.fund_id, IdMatch.data_source == DFundDividend.data_source)
    ).filter(
        and_(DFundDividend.update_time >= update_time, DFundDividend.is_used == 1, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_fnv.all())
    df.columns = [x.name for x in _map_entities]

    df.index = df[[FundInfo.fund_id.name, FundDividend.record_date.name]]
    return df


def transform():
    df = fetch_multisource_dividend(UPDATE_TIME)
    df[FundDividend.statistic_date.name] = df[FundDividend.record_date.name]

    df_020002 = df.ix[df[FundInfo.data_source.name] == "020002"]

    df_020001 = df.ix[df[FundInfo.data_source.name] == "020001"]

    result = df_020002.join(
        df_020001, how="outer", rsuffix="_020001"
    )[df_020002.columns].fillna(df_020001)

    std_rec_date = {}
    for idx in df_020002.index:
        std_rec_date.setdefault(idx[0], set()).add(idx[1])
    std_dividend = {}
    for idx in df_020002.index:
        try:
            std_dividend[idx] = float(df_020002.ix[[idx], FundDividend.dividend_at.name][idx])
        except Exception as e:
            print(e)
            print(idx)
            raise AssertionError

    drop_list = []
    result_not_main = result.ix[result[FundDividend.data_source.name] != "020002"]
    for idx in result_not_main.index:
        fid, rdate = result_not_main.ix[[idx]].index.tolist()[0]
        for tolerant in [0, 1, -1, 2, -2]:
            date_possible = rdate + dt.timedelta(tolerant)
            if date_possible in std_rec_date.get(fid, set()):
                val = float(result_not_main.ix[[idx], FundDividend.dividend_at.name][idx])
                val_std = std_dividend[(fid, date_possible)]
                print(fid, date_possible, val_std, val)
                length_min = min(len(str(val).split(".")[1]), len(str(val_std).split(".")[1]))
                if round(val, length_min) == round(val_std, length_min):
                    drop_list.append(idx)
                    break
    result = result.drop(drop_list)

    return result


def main():
    io.to_sql(FundDividend.__tablename__, _engine_wt, transform())

if __name__ == "__main__":
    main()
