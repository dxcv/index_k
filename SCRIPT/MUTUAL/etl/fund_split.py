import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundSplit
from utils.database.models.base_public import IdMatch, FundInfo, FundSplit

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]

_entities_map = [
    (FundInfo.fund_id, FundSplit.fund_id), (FundInfo.fund_name, FundSplit.fund_name), (DFundSplit.data_source, FundSplit.data_source),
    (DFundSplit.statistic_date, FundSplit.statistic_date), (DFundSplit.split_date, FundSplit.split_date),
    (DFundSplit.split_ratio, FundSplit.split_ratio)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_split(update_time):
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
        DFundSplit, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundSplit.fund_id, IdMatch.data_source == DFundSplit.data_source)
    ).filter(
        and_(DFundSplit.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_fnv.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundInfo.fund_id.name, FundSplit.statistic_date.name]]
    return df


def transform():
    df = fetch_multisource_split(UPDATE_TIME)

    df_020001 = df.ix[df[FundInfo.data_source.name] == "020001"]
    df_020002 = df.ix[df[FundInfo.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundInfo.data_source.name] == "020003"]

    result = df_020002.join(
        df_020001, how="outer", rsuffix="_020001"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[df_020002.columns].fillna(df_020001).fillna(df_020003)

    std_rec_date = {}
    for idx in df_020002.index:
        std_rec_date.setdefault(idx[0], set()).add(idx[1])
    std_split = {}
    for idx in df_020002.index:
        std_split[idx] = float(df_020002.ix[[idx], FundSplit.split_ratio.name][idx])

    drop_list = []
    result_not_main = result.ix[result[FundSplit.data_source.name] != "020002"]
    for idx in result_not_main.index:
        fid, rdate = result_not_main.ix[[idx]].index.tolist()[0]
        for tolerant in [0, 1, -1, 2, -2]:
            date_possible = rdate + dt.timedelta(tolerant)
            if date_possible in std_rec_date.get(fid, set()):
                val = float(result_not_main.ix[[idx], FundSplit.split_ratio.name][idx])
                val_std = std_split[(fid, date_possible)]
                length_min = min(len(str(val).split(".")[1]), len(str(val_std).split(".")[1]))
                if round(val, length_min) == round(val_std, length_min):
                    drop_list.append(idx)
    result = result.drop(drop_list)

    return result


def main():
    io.to_sql(FundSplit.__tablename__, _engine_wt, transform())

if __name__ == "__main__":
    main()


