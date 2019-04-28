from utils.database import config as cfg
from utils.etlkit.reader.mysqlreader import MysqlInput
from sqlalchemy import and_, distinct
from sqlalchemy.orm import sessionmaker
from utils.etlkit.core.base import Stream
from utils.etlkit.core import transform
from utils.database.models.config_private import SourceInfo
from utils.database.models.crawl_private import DFundNv, TFundNv, SFundNv, YFundNv, GFundNv
from utils.database.models.base_private import FundNvDataSource, IdMatch
from utils.database import io
import datetime as dt
from dateutil.relativedelta import relativedelta
from multiprocessing import Pool
from SCRIPT.PRIVATE.etl.maintain_old import encapsulate_to_old


engine_r = cfg.load_engine()["etl01"]
engine_w = cfg.load_engine()["etl_base_private"]
dbsession = sessionmaker()
UPT = dt.datetime.now() - relativedelta(minutes=12)
MIN_DATE = dt.date(1970, 1, 1)


def yfundnv2src(fund_id=None, source_id=None, update_time=None):
    session = dbsession(bind=engine_r)
    stmt = session.query(YFundNv).filter(
        YFundNv.is_used == 1, YFundNv.statistic_date > MIN_DATE, YFundNv.nav < 2000
    ).with_entities(
        YFundNv.fund_id, YFundNv.source_id, YFundNv.statistic_date, YFundNv.source_id, YFundNv.nav, YFundNv.added_nav, YFundNv.adjusted_nav
    )
    if update_time is not None:
        stmt = stmt.filter(YFundNv.update_time >= update_time)
    if fund_id is not None:
        stmt = stmt.filter(YFundNv.fund_id == fund_id)
    if source_id is not None:
        stmt = stmt.filter(YFundNv.source_id == source_id)

    inp = MysqlInput(session.bind, stmt)

    vm = transform.ValueMap({
        YFundNv.added_nav.name: lambda x: None if x == 0 else x
    })

    km = transform.MapSelectKeys({
        YFundNv.fund_id.name: FundNvDataSource.fund_id.name,
        YFundNv.statistic_date.name: FundNvDataSource.statistic_date.name,
        YFundNv.source_id.name: FundNvDataSource.source_id.name,
        YFundNv.nav.name: FundNvDataSource.nav.name,
        YFundNv.added_nav.name: FundNvDataSource.added_nav.name,
        YFundNv.adjusted_nav.name: FundNvDataSource.adjusted_nav.name,
    })
    s = Stream(inp, (vm, km))
    return s


def gfundnv2src(fund_id=None, source_id=None, update_time=None):
    session = dbsession(bind=engine_r)
    stmt = session.query(GFundNv).filter(
        GFundNv.is_used == 1, GFundNv.statistic_date > MIN_DATE, GFundNv.nav < 2000
    ).with_entities(
        GFundNv.fund_id, GFundNv.source_id, GFundNv.statistic_date, GFundNv.source_id, GFundNv.nav, GFundNv.added_nav, GFundNv.adjusted_nav
    )
    if update_time is not None:
        stmt = stmt.filter(GFundNv.update_time >= update_time)
    if fund_id is not None:
        stmt = stmt.filter(GFundNv.fund_id == fund_id)
    if source_id is not None:
        stmt = stmt.filter(GFundNv.source_id == source_id)

    inp = MysqlInput(session.bind, stmt)

    vm = transform.ValueMap({
        GFundNv.added_nav.name: lambda x: None if x == 0 else x
    })

    km = transform.MapSelectKeys({
        GFundNv.fund_id.name: FundNvDataSource.fund_id.name,
        GFundNv.statistic_date.name: FundNvDataSource.statistic_date.name,
        GFundNv.source_id.name: FundNvDataSource.source_id.name,
        GFundNv.nav.name: FundNvDataSource.nav.name,
        GFundNv.added_nav.name: FundNvDataSource.added_nav.name,
        GFundNv.adjusted_nav.name: FundNvDataSource.adjusted_nav.name,
    })
    s = Stream(inp, [vm, km])
    return s


def dfundnv2src(fund_id=None, source_id=None, update_time=None):
    def cal_added_by_nv_and_adjnv(added_nv, nv, adj_nv, sid):
        if sid != "020003":
            if added_nv != 0:
                return added_nv
            else:
                return None
        else:
            if nv == adj_nv:
                return nv
            else:
                return None

    session = dbsession(bind=engine_r)
    stmt = session.query(DFundNv).join(
        IdMatch, and_(IdMatch.source_id == DFundNv.fund_id, IdMatch.source == DFundNv.source_id, IdMatch.id_type == 1, IdMatch.is_used == 1)
    ).filter(
        DFundNv.is_used == 1, DFundNv.source_id == source_id, DFundNv.statistic_date > MIN_DATE, DFundNv.nav < 2000
    ).with_entities(
        IdMatch.matched_id, DFundNv.fund_id, DFundNv.statistic_date, DFundNv.source_id, DFundNv.nav, DFundNv.added_nav, DFundNv.adjusted_nav
    )
    if update_time is not None:
        stmt = stmt.filter(DFundNv.update_time >= update_time)
    if fund_id is not None:
        stmt = stmt.filter(DFundNv.fund_id == fund_id)
    inp = MysqlInput(session.bind, stmt)

    vm = transform.ValueMap({
        # DFundNv.added_nav.name: lambda x: None if x == 0 else x
        DFundNv.added_nav.name: (
            lambda added_nv, nv, adj_nv, sid: cal_added_by_nv_and_adjnv(added_nv, nv, adj_nv, sid),
            DFundNv.added_nav.name, DFundNv.nav.name, DFundNv.adjusted_nav.name, DFundNv.source_id.name
        )
    })

    km = transform.MapSelectKeys({
        IdMatch.matched_id.name: FundNvDataSource.fund_id.name,
        DFundNv.statistic_date.name: FundNvDataSource.statistic_date.name,
        DFundNv.source_id.name: FundNvDataSource.source_id.name,
        DFundNv.nav.name: FundNvDataSource.nav.name,
        DFundNv.added_nav.name: FundNvDataSource.added_nav.name,
        DFundNv.adjusted_nav.name: FundNvDataSource.adjusted_nav.name,
    })
    s = Stream(inp, (vm, km))
    return s


def sfundnv2src(fund_id=None, source_id=None, update_time=None):
    session = dbsession(bind=engine_r)
    stmt = session.query(SFundNv).join(
        IdMatch, and_(IdMatch.source_id == SFundNv.fund_id, IdMatch.source == SFundNv.source_id, IdMatch.id_type == 1, IdMatch.is_used == 1)
    ).filter(
        SFundNv.is_used == 1, SFundNv.source_id == source_id, SFundNv.statistic_date > MIN_DATE, SFundNv.nav < 2000
    ).with_entities(
        IdMatch.matched_id, SFundNv.fund_id, SFundNv.statistic_date, SFundNv.source_id, SFundNv.nav, SFundNv.added_nav
    )
    if update_time is not None:
        stmt = stmt.filter(SFundNv.update_time >= update_time)
    if fund_id is not None:
        stmt = stmt.filter(SFundNv.fund_id == fund_id)

    inp = MysqlInput(session.bind, stmt)

    vm = transform.ValueMap({
        SFundNv.added_nav.name: lambda x: None if x == 0 else x
    })

    km = transform.MapSelectKeys({
        IdMatch.matched_id.name: FundNvDataSource.fund_id.name,
        SFundNv.statistic_date.name: FundNvDataSource.statistic_date.name,
        SFundNv.source_id.name: FundNvDataSource.source_id.name,
        SFundNv.nav.name: FundNvDataSource.nav.name,
        SFundNv.added_nav.name: FundNvDataSource.added_nav.name,
    })
    s = Stream(inp, (vm, km))
    return s


def tfundnv2src(fund_id=None, source_id=None, update_time=None):
    session = dbsession(bind=engine_r)
    stmt = session.query(TFundNv).join(
        IdMatch, and_(IdMatch.source_id == TFundNv.fund_id, IdMatch.source == TFundNv.source_id, IdMatch.id_type == 1, IdMatch.is_used == 1)
    ).filter(
        TFundNv.is_used == 1, TFundNv.source_id == source_id, TFundNv.statistic_date > MIN_DATE, TFundNv.nav < 2000
    ).with_entities(
        IdMatch.matched_id, TFundNv.fund_id, TFundNv.statistic_date, TFundNv.source_id, TFundNv.nav, TFundNv.added_nav
    )
    if update_time is not None:
        stmt = stmt.filter(TFundNv.update_time >= update_time)
    if fund_id is not None:
        stmt = stmt.filter(TFundNv.fund_id == fund_id)

    inp = MysqlInput(session.bind, stmt)

    vm = transform.ValueMap({
        TFundNv.added_nav.name: lambda x: None if x == 0 else x
    })

    km = transform.MapSelectKeys({
        IdMatch.matched_id.name: FundNvDataSource.fund_id.name,
        TFundNv.statistic_date.name: FundNvDataSource.statistic_date.name,
        TFundNv.source_id.name: FundNvDataSource.source_id.name,
        TFundNv.nav.name: FundNvDataSource.nav.name,
        TFundNv.added_nav.name: FundNvDataSource.added_nav.name,
    })
    s = Stream(inp, (vm, km))
    return s


def loop_whole(source_id):
    session = dbsession(bind=engine_r)
    source = int(source_id[:2])
    upt = None
    if source == 0:
        stmt = session.query(distinct(YFundNv.fund_id)).filter(
            YFundNv.source_id == source_id
        )
        fids = sorted([x[0] for x in stmt.all()])
        for fid in fids:
            s = yfundnv2src(fid, source_id, update_time=upt)
            s.flow()
            io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)

    elif source == 2:
        stmt = session.query(distinct(DFundNv.fund_id)).filter(
            DFundNv.source_id == source_id
        )
        fids = sorted([x[0] for x in stmt.all()])
        for fid in fids:
            s = dfundnv2src(fid, source_id, update_time=upt)
            s.flow()
            io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)

    elif source == 4:
        stmt = session.query(distinct(TFundNv.fund_id)).filter(
            TFundNv.source_id == source_id
        )
        fids = sorted([x[0] for x in stmt.all()])
        for fid in fids:
            s = tfundnv2src(fid, source_id, update_time=upt)
            s.flow()
            io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)

    # elif source == 5:
    #     stmt = session.query(distinct(GFundNv.fund_id)).filter(
    #         GFundNv.source_id == source_id
    #     )
    #     fids = sorted([x[0] for x in stmt.all()])
    #     for fid in fids:
    #         s = gfundnv2src(fid, source_id, update_time=upt)
    #         s.flow()
    #         io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)


def loop_by_upt(source_id):
    source = int(source_id[:2])
    if source == 0:
        s = yfundnv2src(None, source_id, update_time=UPT)
        s.flow()
        io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)

        #### ENCAP TO OLD
        io.to_sql("fund_nv_data_source", engine_w, encapsulate_to_old(s.dataframe))
        #### ENCAP TO OLD

    elif source == 2:
        s = dfundnv2src(None, source_id, update_time=UPT)
        s.flow()
        io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)

        #### ENCAP TO OLD
        io.to_sql("fund_nv_data_source", engine_w, encapsulate_to_old(s.dataframe))
        #### ENCAP TO OLD

    elif source == 3:
        s = sfundnv2src(None, source_id, update_time=UPT)
        s.flow()
        print(s.dataframe)
        io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)

        #### ENCAP TO OLD
        io.to_sql("fund_nv_data_source", engine_w, encapsulate_to_old(s.dataframe))
        #### ENCAP TO OLD

    elif source == 4:
        s = tfundnv2src(None, source_id, update_time=UPT)
        s.flow()
        io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)

        #### ENCAP TO OLD
        io.to_sql("fund_nv_data_source", engine_w, encapsulate_to_old(s.dataframe))
        #### ENCAP TO OLD

    df = gfundnv2src(None, source_id, update_time=UPT).flow()[0]
    io.to_sql(FundNvDataSource.__tablename__, engine_w, df)

    #### ENCAP TO OLD
    io.to_sql("fund_nv_data_source", engine_w, encapsulate_to_old(df))
    #### ENCAP TO OLD


def loop_by_fid(fund_id):
    session = dbsession(bind=engine_r)
    stmt = session.query(IdMatch).filter(
        and_(IdMatch.matched_id == fund_id, IdMatch.is_used == 1)
    ).with_entities(
        IdMatch.source, IdMatch.source_id
    )
    print(fund_id)
    for src, sid in stmt.all():
        source = int(src[:2])
        if source == 2:
            s = dfundnv2src(sid, src)
        elif source == 3:
            s = sfundnv2src(sid, src)

        elif source == 4:
            s = tfundnv2src(sid, src)
        else:
            continue
        s.flow()
        io.to_sql(FundNvDataSource.__tablename__, engine_w, s.dataframe)

        #### ENCAP TO OLD
        io.to_sql("fund_nv_data_source", engine_w, encapsulate_to_old(s.dataframe))
        #### ENCAP TO OLD

    df_y = yfundnv2src(fund_id, "000001").flow()[0]
    io.to_sql(FundNvDataSource.__tablename__, engine_w, df_y)

    df_g = gfundnv2src(fund_id).flow()[0]
    io.to_sql(FundNvDataSource.__tablename__, engine_w, df_g)

    #### ENCAP TO OLD
    io.to_sql("fund_nv_data_source", engine_w, encapsulate_to_old(df_y))
    io.to_sql("fund_nv_data_source", engine_w, encapsulate_to_old(df_g))
    #### ENCAP TO OLD


def fetch_all_source():
    session = dbsession(bind=engine_r)
    return [x[0] for x in session.query(distinct(SourceInfo.source_id)).all()]


def main():
    pool = Pool(4)
    session = dbsession(bind=engine_r)
    stmt = session.query(distinct(IdMatch.matched_id)).filter(
        IdMatch.update_time >= UPT
    )
    fids = sorted([x[0] for x in stmt.all()])

    source_ids = fetch_all_source()
    source_ids = [x for x in source_ids if int(x[:2]) not in {1}]  # 1-基协, 5-投顾
    # source_ids = [x for x in source_ids if int(x[:2]) not in {1, 2, 3, 4}]  # 1-基协, 5-投顾

    # sync by triggered update_time looped by source
    pool.map(loop_by_upt, source_ids)

    # sync triggered by id_match
    pool.map(loop_by_fid, fids)


def tmpsync(fund_id):
    pool = Pool(4)

    pool.map(loop_by_fid, [fund_id])
    # pool.map(loop_by_fid, fund_ids)
    # pool.map(loop_by_fid, fids)
    pool.close()
    pool.join()
    print("done")


def tmp():
    pool = Pool(4)

    # fids = [x[0] for x in engine_r.execute("SELECT DISTINCT fund_id FROM base.fund_nv_data_source_copy2 WHERE source_id LIKE '04'").fetchall()]
    # print("JR147289" in fids)
    # print(len(fids))
    # source_ids = fetch_all_source()
    # source_ids = [x for x in source_ids if int(x[:2]) not in {1, 5}]  # 1-基协, 3-券商, 5-投顾
    # source_ids = [x for x in source_ids if int(x[:2]) in {3}]  # 1-基协, 3-券商, 5-投顾

    # sync all
    # pool.map(loop_whole_by_fid, source_ids)

    # # sync by triggered update_time looped by source
    # pool.map(loop_by_upt, source_ids)

    # sync triggered by id_match
    # import pandas as pd
    # sql = "SELECT DISTINCT matched_id FROM base.id_match WHERE source LIKE '03____'"
    # fids = pd.read_sql(sql, engine_r)["matched_id"].tolist()
    # pool.map(loop_by_fid, fids)

if __name__ == "__main__":
    # tmp()
    main()
