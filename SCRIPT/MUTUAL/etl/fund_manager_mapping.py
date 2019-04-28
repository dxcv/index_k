from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from utils.database import config as cfg, io
from utils.etlkit.core import transform, base
from utils.etlkit.reader.mysqlreader import MysqlInput
from utils.database.models.crawl_public import DPersonFund
from utils.database.models.base_public import IdMatch, FundInfo, FundManagerMapping, PersonInfo
import datetime as dt

ENGINE = cfg.load_engine()["2Gbp"]

dbsession = sessionmaker()


def stream_fi(data_source):
    session = dbsession(bind=ENGINE)
    stmt = session.query(IdMatch).join(
        FundInfo, and_(IdMatch.matched_id == FundInfo.fund_id, IdMatch.id_type == 1, IdMatch.is_used == 1, IdMatch.data_source == data_source)
    ).with_entities(
        IdMatch.source_id, IdMatch.matched_id, FundInfo.fund_name)

    inp = MysqlInput(session.bind, stmt)

    return inp


def stream_020001():
    from dateutil.relativedelta import relativedelta
    import re

    def clean_dimission_date(tenure_date, tenure_period, is_current):
        if is_current == 1:
            return None

        sre = re.search("((?P<years>\d*)年)?((?P<days>\d*)天)", tenure_period)
        if sre is not None:
            d = sre.groupdict()
            years, days = d.get("years") or 0, d.get("days") or 0
            return tenure_date + relativedelta(years=int(years), days=int(days))
        else:
            return None


    DATA_SOURCE = "020001"
    session = dbsession(bind=ENGINE)
    stmt = session.query(IdMatch).join(
        DPersonFund, and_(IdMatch.source_id == DPersonFund.person_id, IdMatch.data_source == DPersonFund.data_source,
        IdMatch.id_type == 3, IdMatch.is_used == 1, IdMatch.data_source == DATA_SOURCE)
    ).join(
        PersonInfo, and_(IdMatch.matched_id == PersonInfo.person_id, IdMatch.id_type == 3, IdMatch.is_used == 1, IdMatch.data_source == DATA_SOURCE)
    ).filter(
        DPersonFund.is_used == 1
    ).with_entities(
        IdMatch.matched_id.label(FundManagerMapping.person_id.name), PersonInfo.person_name, DPersonFund.fund_id,
        DPersonFund.tenure_date, DPersonFund.tenure_period, DPersonFund.is_current
    )

    inp = MysqlInput(session.bind, stmt)

    fi = stream_fi(DATA_SOURCE)

    jn = transform.Join(fi, left_on=DPersonFund.fund_id.name, right_on=IdMatch.source_id.name)

    vm = transform.ValueMap(
        {
            FundManagerMapping.dimission_date.name: (clean_dimission_date, DPersonFund.tenure_date.name, DPersonFund.tenure_period.name, DPersonFund.is_current.name)
        }
    )

    sk = transform.MapSelectKeys(
        {
            FundManagerMapping.person_id.name: None,
            PersonInfo.person_name.name: FundManagerMapping.person_name.name,
            IdMatch.matched_id.name: FundManagerMapping.fund_id.name,
            FundInfo.fund_name.name: FundManagerMapping.fund_name.name,
            DPersonFund.tenure_date.name: FundManagerMapping.tenure_date.name,
            DPersonFund.dimission_date.name: FundManagerMapping.dimission_date.name,
            DPersonFund.is_current.name: FundManagerMapping.is_current.name
        }
    )

    s = base.Stream(inp, (jn, vm, sk))
    return s


def stream_020002():
    def clean_date(date_str):
        if date_str is not None:
            date_str = date_str.strip()
            if date_str == "至今":
                return None
            else:
                return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        else:
            return None
        pass

    DATA_SOURCE = "020002"
    session = dbsession(bind=ENGINE)
    stmt = session.query(IdMatch).join(
        DPersonFund, and_(IdMatch.source_id == DPersonFund.person_id, IdMatch.data_source == DPersonFund.data_source,
        IdMatch.id_type == 3, IdMatch.is_used == 1, IdMatch.data_source == DATA_SOURCE)
    ).join(
        PersonInfo, and_(IdMatch.matched_id == PersonInfo.person_id, IdMatch.id_type == 3, IdMatch.is_used == 1, IdMatch.data_source == DATA_SOURCE)
    ).filter(
        DPersonFund.is_used == 1
    ).with_entities(
        IdMatch.matched_id.label(FundManagerMapping.person_id.name), PersonInfo.person_name, DPersonFund.fund_id,
        DPersonFund.tenure_date, DPersonFund.dimission_date, DPersonFund.is_current
    )

    inp = MysqlInput(session.bind, stmt)

    fi = stream_fi(DATA_SOURCE)

    jn = transform.Join(fi, left_on=DPersonFund.fund_id.name, right_on=IdMatch.matched_id.name)

    vm = transform.ValueMap(
        {
            FundManagerMapping.dimission_date.name: lambda x: clean_date(x)
        }
    )

    sk = transform.MapSelectKeys(
        {
            FundManagerMapping.person_id.name: None,
            PersonInfo.person_name.name: FundManagerMapping.person_name.name,
            IdMatch.matched_id.name: FundManagerMapping.fund_id.name,
            FundInfo.fund_name.name: FundManagerMapping.fund_name.name,
            DPersonFund.tenure_date.name: FundManagerMapping.tenure_date.name,
            DPersonFund.dimission_date.name: FundManagerMapping.dimission_date.name,
            DPersonFund.is_current.name: FundManagerMapping.is_current.name
        }
    )

    s = base.Stream(inp, (jn, vm, sk))
    return s


def stream_020003():
    def clean_date(date_str):
        if date_str is not None:
            date_str = date_str.strip()
            if date_str == "至今":
                return None
            else:
                return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        else:
            return None
        pass

    DATA_SOURCE = "020003"
    session = dbsession(bind=ENGINE)
    stmt = session.query(IdMatch).join(
        DPersonFund, and_(IdMatch.source_id == DPersonFund.person_id, IdMatch.data_source == DPersonFund.data_source,
        IdMatch.id_type == 3, IdMatch.is_used == 1, IdMatch.data_source == DATA_SOURCE)
    ).join(
        PersonInfo, and_(IdMatch.matched_id == PersonInfo.person_id, IdMatch.id_type == 3, IdMatch.is_used == 1, IdMatch.data_source == DATA_SOURCE)
    ).filter(
        DPersonFund.is_used == 1
    ).with_entities(
        IdMatch.matched_id.label(FundManagerMapping.person_id.name), PersonInfo.person_name, DPersonFund.fund_id,
        DPersonFund.tenure_date, DPersonFund.dimission_date, DPersonFund.is_current
    )

    inp = MysqlInput(session.bind, stmt)

    fi = stream_fi(DATA_SOURCE)

    jn = transform.Join(fi, left_on=DPersonFund.fund_id.name, right_on=IdMatch.matched_id.name)

    vm = transform.ValueMap(
        {
            FundManagerMapping.dimission_date.name: lambda x: clean_date(x)
        }
    )

    sk = transform.MapSelectKeys(
        {
            FundManagerMapping.person_id.name: None,
            PersonInfo.person_name.name: FundManagerMapping.person_name.name,
            IdMatch.matched_id.name: FundManagerMapping.fund_id.name,
            FundInfo.fund_name.name: FundManagerMapping.fund_name.name,
            # DPersonFund.tenure_date.name: FundManagerMapping.tenure_date.name,
            # DPersonFund.dimission_date.name: FundManagerMapping.dimission_date.name,
            # DPersonFund.is_current.name: FundManagerMapping.is_current.name
        }
    )

    s = base.Stream(inp, (jn, vm, sk))
    return s


def confluence():
    s21 = stream_020001()
    s22 = stream_020002()
    # s23 = stream_020003()

    c = base.Confluence(s22, s21, on=[FundManagerMapping.fund_id.name, FundManagerMapping.person_id.name])

    def clean_tenure_period(is_current, tenure_date, dimission_date):
        if is_current == 1:
            return (dt.date.today() - tenure_date).days
        else:
            return (dimission_date.today() - tenure_date).days

    vm = transform.ValueMap(
        {
            FundManagerMapping.tenure_period.name: (clean_tenure_period, FundManagerMapping.is_current.name, FundManagerMapping.tenure_date.name, FundManagerMapping.dimission_date.name)
        }
    )

    s = base.Stream(c, transform=(vm,))
    c = base.Confluence(s)

    return c


def main():
    c = confluence()
    io.to_sql(FundManagerMapping.__tablename__, ENGINE, c.dataframe)


def test():
    import os
    from utils.etlkit.ext import tools
    from utils.database import config as cfg
    ENGINER = cfg.load_engine()["2Gbp"]

    tc = tools.TableComparer(
        "base_public.fund_manager_mapping", "base_public.fund_manager_mapping",
        ENGINER,
        cols_included={"tenure_date", "dimission_date", "tenure_period"},
    )
    res = tc.result
    res.to_csv(os.path.expanduser("~/Desktop/fund_manager_mapping.csv"), encoding="gbk")


if __name__ == "__main__":
    main()
