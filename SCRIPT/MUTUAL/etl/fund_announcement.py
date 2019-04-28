import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.algorithm import etl
from utils.database import io, config as cfg
from utils.database.models.crawl_public import DFundAnnouncement
from utils.database.models.base_public import IdMatch, FundInfo, FundAnnouncement
import datetime as dt
from dateutil.relativedelta import relativedelta

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = dt.datetime.now() - relativedelta(hours=1, minutes=5)


_entities_map = [
    (FundInfo.fund_id, FundAnnouncement.fund_id),
    (FundInfo.fund_name, FundAnnouncement.fund_name),
    (DFundAnnouncement.date, FundAnnouncement.date),
    (DFundAnnouncement.data_source, None),
    (DFundAnnouncement.announcement_id, FundAnnouncement.announcement_id),
    (DFundAnnouncement.announcement_name, FundAnnouncement.announcement_name),
    (DFundAnnouncement.type, FundAnnouncement.type),
    (DFundAnnouncement.content, FundAnnouncement.content),
]

_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_announcement(update_time):
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
        DFundAnnouncement, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundAnnouncement.fund_id,)
    ).filter(
        and_(DFundAnnouncement.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_fundincome.all())
    df = df.rename(columns={x.name: y.name if y is not None else x.name for x, y in zip(_input_entities, _map_entities)})
    # df.columns = [x.name for x in _map_entities]
    df.index = df[[FundAnnouncement.fund_id.name,]]
    return df


def transform():
    # General process
    df = fetch_multisource_fund_announcement(UPDATE_TIME)

    # Process of different sources
    df_020002 = df.ix[df[DFundAnnouncement.data_source.name] == "020002"]

    df_020001 = df.ix[df[DFundAnnouncement.data_source.name] == "020001"]
    df_020001["type"] = df_020001["type"].apply(lambda x: {"最新公告": "其他公告", "招募说明书": "发行运作", "定期报告": "定期报告"}.get(x))
    df_020001["type"] = list(map(lambda x, y: x if "招募说明" not in y else "发行运作", df_020001["type"], df_020001["announcement_name"]))
    #

    # df_020003 = df.ix[df[DFundAnnouncement.data_source.name] == "020003"]

    result = df_020002.append(df_020001).drop_duplicates(subset=[FundAnnouncement.fund_id.name, FundAnnouncement.announcement_name.name])
    result.drop([DFundAnnouncement.data_source.name], axis=1, inplace=True)
    return result


def main():
    try:
        io.to_sql(FundAnnouncement.__tablename__, _engine_wt, transform(), chunksize=10)
    except:
        pass

if __name__ == "__main__":
    main()


def test():
    # temporary
    _engine_wt2 = cfg.load_engine()["4Gpp"]
    def tmp(fund_id):
        """

        Args:
            update_time: record update time

        Returns:
            pandas.DataFrame
        """

        query_fundincome = _session.query(IdMatch).join(
            FundInfo, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundInfo.fund_id)
        ).join(
            DFundAnnouncement, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundAnnouncement.fund_id, )
        # IdMatch.data_source == DFundAnnouncement.data_source

        ).filter(
            DFundAnnouncement.fund_id == fund_id
        ).with_entities(
            *_input_entities
        )
        df = pd.DataFrame(query_fundincome.all())
        df = df.rename(
            columns={x.name: y.name if y is not None else x.name for x, y in zip(_input_entities, _map_entities)}
        )
        # df.columns = [x.name for x in _map_entities]
        df.index = df[[FundAnnouncement.fund_id.name, ]]
        df.drop([DFundAnnouncement.data_source.name], axis=1, inplace=True)
        return df

    fids = sorted(pd.read_sql("SELECT DISTINCT fund_id FROM fund_info", _engine_wt)["fund_id"].tolist())
    for fid in fids:
        try:
            print(fid)
            res = tmp(fid)
            io.to_sql(FundAnnouncement.__tablename__, _engine_wt, res, chunksize=10)
            io.to_sql(FundAnnouncement.__tablename__, _engine_wt2, res, chunksize=10)
        except:
            continue
