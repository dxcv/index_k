from utils.database import config as cfg
from utils.etlkit.reader.mysqlreader import MysqlInput
from sqlalchemy import and_, distinct
from sqlalchemy.orm import sessionmaker
from utils.etlkit.core.base import Stream
from utils.etlkit.core import transform
from utils.database.models.config_private import SourceInfo
from utils.database.models.crawl_private import DFundNv, TFundNv, YFundNv, GFundNv
from utils.database.models.base_private import FundNvDataSource, IdMatch
from utils.database import io
import datetime as dt
from dateutil.relativedelta import relativedelta
from multiprocessing import Pool


engine_r = cfg.load_engine()["etl01"]
engine_w = cfg.load_engine()["etl_base_private"]
dbsession = sessionmaker()


class Trigger:
    def _trigger_from_idmatch(self):
        session = dbsession(bind=engine_r)
        stmt = session.query(IdMatch).filter(
            IdMatch.update_time >= dt.datetime.now() - relativedelta(hours=8)
        ).with_entities(
            IdMatch.f
        )


class SourceRouter:
    DATA_TABLE = {
        "nv": {
            0: YFundNv,
            2: DFundNv,
            4: TFundNv,
        }
    }

    def __init__(self, source_id, datatype):
        self._source_id = source_id
        self._datatype = datatype

    @property
    def source(self):
        return int(self._source_id[:2])

    def route(self):
        return self.DATA_TABLE[self._datatype][self.source]


def yfundnv2src_whole(fund_id=None, source_id=None, update_time=None):
    session = dbsession(bind=engine_r)
    stmt = session.query(YFundNv).filter(
        YFundNv.source_id == source_id, YFundNv.is_used == 1
    ).with_entities(
        YFundNv.fund_id, YFundNv.source_id, YFundNv.statistic_date, YFundNv.source_id, YFundNv.nav, YFundNv.added_nav, YFundNv.adjusted_nav
    )
    if update_time is not None:
        stmt = stmt.filter(YFundNv.update_time >= update_time)
    if fund_id is not None:
        stmt = stmt.filter(YFundNv.fund_id == fund_id)

    inp = MysqlInput(session.bind, stmt)

    km = transform.MapSelectKeys({
        YFundNv.fund_id.name: FundNvDataSource.fund_id.name,
        YFundNv.statistic_date.name: FundNvDataSource.statistic_date.name,
        YFundNv.source_id.name: FundNvDataSource.source_id.name,
        YFundNv.nav.name: FundNvDataSource.nav.name,
        YFundNv.added_nav.name: FundNvDataSource.added_nav.name,
        YFundNv.adjusted_nav.name: FundNvDataSource.adjusted_nav.name,
    })
    s = Stream(inp, (km,))
    return s


def gfundnv2src_whole(fund_id=None, source_id=None, update_time=None):
    session = dbsession(bind=engine_r)
    stmt = session.query(GFundNv).filter(
        GFundNv.source_id == source_id, GFundNv.is_used == 1
    ).with_entities(
        GFundNv.fund_id, GFundNv.source_id, GFundNv.statistic_date, GFundNv.source_id, GFundNv.nav, GFundNv.added_nav, GFundNv.adjusted_nav
    )
    if update_time is not None:
        stmt = stmt.filter(GFundNv.update_time >= update_time)
    if fund_id is not None:
        stmt = stmt.filter(GFundNv.fund_id == fund_id)

    inp = MysqlInput(session.bind, stmt)

    km = transform.MapSelectKeys({
        GFundNv.fund_id.name: FundNvDataSource.fund_id.name,
        GFundNv.statistic_date.name: FundNvDataSource.statistic_date.name,
        GFundNv.source_id.name: FundNvDataSource.source_id.name,
        GFundNv.nav.name: FundNvDataSource.nav.name,
        GFundNv.added_nav.name: FundNvDataSource.added_nav.name,
        GFundNv.adjusted_nav.name: FundNvDataSource.adjusted_nav.name,
    })
    s = Stream(inp, (km,))
    return s


def dfundnv2src_whole(fund_id=None, source_id=None, update_time=None):
    session = dbsession(bind=engine_r)
    stmt = session.query(DFundNv).join(
        IdMatch, and_(IdMatch.source_id == DFundNv.fund_id, IdMatch.source == source_id, IdMatch.id_type == 1, IdMatch.is_used == 1)
    ).filter(
        DFundNv.is_used == 1
    ).with_entities(
        IdMatch.matched_id, DFundNv.fund_id, DFundNv.statistic_date, DFundNv.source_id, DFundNv.nav, DFundNv.added_nav, DFundNv.adjusted_nav
    )
    if update_time is not None:
        stmt = stmt.filter(DFundNv.update_time >= update_time)
    if fund_id is not None:
        stmt = stmt.filter(DFundNv.fund_id == fund_id)

    inp = MysqlInput(session.bind, stmt)

    km = transform.MapSelectKeys({
        IdMatch.matched_id.name: FundNvDataSource.fund_id.name,
        DFundNv.statistic_date.name: FundNvDataSource.statistic_date.name,
        DFundNv.source_id.name: FundNvDataSource.source_id.name,
        DFundNv.nav.name: FundNvDataSource.nav.name,
        DFundNv.added_nav.name: FundNvDataSource.added_nav.name,
        DFundNv.adjusted_nav.name: FundNvDataSource.adjusted_nav.name,
    })
    s = Stream(inp, (km,))
    return s


def tfundnv2src_whole(fund_id=None, source_id=None, update_time=None):
    session = dbsession(bind=engine_r)
    stmt = session.query(TFundNv).join(
        IdMatch, and_(IdMatch.source_id == TFundNv.fund_id, IdMatch.source == source_id, IdMatch.id_type == 1, IdMatch.is_used == 1)
    ).filter(
        TFundNv.is_used == 1
    ).with_entities(
        IdMatch.matched_id, TFundNv.fund_id, TFundNv.statistic_date, TFundNv.source_id, TFundNv.nav, TFundNv.added_nav
    )
    if update_time is not None:
        stmt = stmt.filter(TFundNv.update_time >= update_time)
    if fund_id is not None:
        stmt = stmt.filter(TFundNv.fund_id == fund_id)

    inp = MysqlInput(session.bind, stmt)

    km = transform.MapSelectKeys({
        IdMatch.matched_id.name: FundNvDataSource.fund_id.name,
        TFundNv.statistic_date.name: FundNvDataSource.statistic_date.name,
        TFundNv.source_id.name: FundNvDataSource.source_id.name,
        TFundNv.nav.name: FundNvDataSource.nav.name,
        TFundNv.added_nav.name: FundNvDataSource.added_nav.name,
    })
    s = Stream(inp, (km,))
    return s


def loop_whole_by_fid(source_id):
    session = dbsession(bind=engine_r)
    source = int(source_id[:2])
    upt = dt.datetime.now() - relativedelta(days=1)
    if source == 0:
        stmt = session.query(distinct(YFundNv.fund_id)).filter(
            YFundNv.source_id == source_id
        )
        fids = sorted([x[0] for x in stmt.all()])
        print(fids)
        for fid in fids:
            s = yfundnv2src_whole(fid, source_id, update_time=upt)
            s.flow()
            print(s.dataframe)
            io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)

    elif source == 2:
        stmt = session.query(distinct(DFundNv.fund_id)).filter(
            DFundNv.source_id == source_id
        )
        fids = sorted([x[0] for x in stmt.all()])
        for fid in fids:
            s = dfundnv2src_whole(fid, source_id, update_time=upt)
            s.flow()
            print(s.dataframe)
            io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)

    elif source == 4:
        stmt = session.query(distinct(TFundNv.fund_id)).filter(
            TFundNv.source_id == source_id
        )
        fids = sorted([x[0] for x in stmt.all()])
        for fid in fids:
            s = tfundnv2src_whole(fid, source_id, update_time=upt)
            s.flow()
            print(s.dataframe)
            io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)

    elif source == 5:
        stmt = session.query(distinct(GFundNv.fund_id)).filter(
            GFundNv.source_id == source_id
        )
        fids = sorted([x[0] for x in stmt.all()])
        for fid in fids:
            s = gfundnv2src_whole(fid, source_id, update_time=upt)
            s.flow()
            print(s.dataframe)
            io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)


def loop_by_upt(source_id):
    source = int(source_id[:2])
    upt = dt.datetime.now() - relativedelta(days=1)
    if source == 0:
        s = yfundnv2src_whole(None, source_id, update_time=upt)

    elif source == 2:
        s = dfundnv2src_whole(None, source_id, update_time=upt)

    elif source == 4:
        s = tfundnv2src_whole(None, source_id, update_time=upt)

    elif source == 5:
        s = gfundnv2src_whole(None, source_id, update_time=upt)

    s.flow()
    print(s.dataframe)
    io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)


def fetch_all_source():
    session = dbsession(bind=engine_r)
    return [x[0] for x in session.query(distinct(SourceInfo.source_id)).all()]


def main():
    source_ids = fetch_all_source()
    # source_ids = [x for x in source_ids if int(x[:2]) == 5]
    pool = Pool(4)
    pool.map(loop_whole_by_fid, source_ids)


def test():
    loop_whole_by_fid("000001")


if __name__ == "__main__":
    main()
    # test()
    # g = gfundnv2src_whole("JR010773", "050025")
