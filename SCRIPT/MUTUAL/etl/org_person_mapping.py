from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from utils.database import config as cfg, io
from utils.etlkit.core import transform, base
from utils.etlkit.reader.mysqlreader import MysqlInput
from utils.database.models.crawl_public import DOrgPerson
from utils.database.models.base_public import IdMatch, OrgInfo, OrgPersonMapping, PersonInfo
import datetime as dt

ENGINE = cfg.load_engine()["2Gbp"]

dbsession = sessionmaker()


def stream_oi(data_source):
    session = dbsession(bind=ENGINE)
    stmt = session.query(IdMatch).join(
        OrgInfo, and_(IdMatch.matched_id == OrgInfo.org_id, IdMatch.id_type == 2, IdMatch.is_used == 1, IdMatch.data_source == data_source)
    ).with_entities(
        IdMatch.source_id, IdMatch.matched_id, OrgInfo.org_name)

    inp = MysqlInput(session.bind, stmt)

    return inp


def stream_020001():
    DATA_SOURCE = "020001"
    session = dbsession(bind=ENGINE)
    stmt = session.query(IdMatch).join(
        DOrgPerson, and_(IdMatch.source_id == DOrgPerson.person_id, IdMatch.data_source == DOrgPerson.data_source,
        IdMatch.id_type == 3, IdMatch.is_used == 1, IdMatch.data_source == DATA_SOURCE)
    ).join(
        PersonInfo, and_(IdMatch.matched_id == PersonInfo.person_id, IdMatch.id_type == 3, IdMatch.is_used == 1, IdMatch.data_source == DATA_SOURCE)
    ).with_entities(
        IdMatch.matched_id.label(OrgPersonMapping.person_id.name), PersonInfo.person_name, DOrgPerson.org_id,
        DOrgPerson.tenure_date, DOrgPerson.dimission_date.name, DOrgPerson.is_current, DOrgPerson.duty
    )

    inp = MysqlInput(session.bind, stmt)

    oi = stream_oi(DATA_SOURCE)

    def clean_duty_detail(duty_detail):
        duty_detail = duty_detail.strip()
        duty_detail = duty_detail.replace("（", "(")
        duty_detail = duty_detail.replace("）", ")")
        duty_detail = duty_detail.replace("基金经理,", "")
        return duty_detail

    vm1 = transform.ValueMap({
        OrgPersonMapping.duty_detail.name: (lambda x: clean_duty_detail(x), DOrgPerson.duty.name)
    })

    vm2 = transform.ValueMap({
        OrgPersonMapping.duty.name: lambda x: x if x == "基金经理" else "高管"
    })

    jn = transform.Join(oi, left_on=DOrgPerson.org_id.name, right_on=IdMatch.source_id.name)

    sk = transform.MapSelectKeys(
        {
            OrgPersonMapping.person_id.name: None,
            PersonInfo.person_name.name: OrgPersonMapping.person_name.name,
            IdMatch.matched_id.name: OrgPersonMapping.org_id.name,
            OrgInfo.org_name.name: OrgPersonMapping.org_name.name,
            OrgPersonMapping.duty.name: None,
            OrgPersonMapping.duty_detail.name: None,
            DOrgPerson.tenure_date.name: OrgPersonMapping.tenure_date.name,
            DOrgPerson.dimission_date.name: OrgPersonMapping.dimission_date.name,
            DOrgPerson.is_current.name: OrgPersonMapping.is_current.name
        }
    )

    s = base.Stream(inp, (vm1, vm2, jn, sk))
    return s


def stream_020002():
    DATA_SOURCE = "020002"
    session = dbsession(bind=ENGINE)
    stmt = session.query(IdMatch).join(
        DOrgPerson, and_(IdMatch.source_id == DOrgPerson.person_id, IdMatch.data_source == DOrgPerson.data_source,
        IdMatch.id_type == 3, IdMatch.is_used == 1, IdMatch.data_source == DATA_SOURCE)
    ).join(
        PersonInfo, and_(IdMatch.matched_id == PersonInfo.person_id, IdMatch.id_type == 3, IdMatch.is_used == 1, IdMatch.data_source == DATA_SOURCE)
    ).with_entities(
        IdMatch.matched_id.label(OrgPersonMapping.person_id.name), PersonInfo.person_name, DOrgPerson.org_id,
        DOrgPerson.tenure_date, DOrgPerson.dimission_date, DOrgPerson.is_current, DOrgPerson.duty
    )

    inp = MysqlInput(session.bind, stmt)

    fi = stream_oi(DATA_SOURCE)

    jn = transform.Join(fi, left_on=DOrgPerson.org_id.name, right_on=IdMatch.source_id.name)

    sk = transform.MapSelectKeys(
        {
            OrgPersonMapping.person_id.name: None,
            PersonInfo.person_name.name: OrgPersonMapping.person_name.name,
            IdMatch.matched_id.name: OrgPersonMapping.org_id.name,
            DOrgPerson.duty.name: OrgPersonMapping.duty.name,
            OrgInfo.org_name.name: OrgPersonMapping.org_name.name,
            DOrgPerson.tenure_date.name: OrgPersonMapping.tenure_date.name,
            DOrgPerson.dimission_date.name: OrgPersonMapping.dimission_date.name,
            DOrgPerson.is_current.name: OrgPersonMapping.is_current.name
        }
    )

    s = base.Stream(inp, (jn, sk))
    return s


def stream_020003():
    DATA_SOURCE = "020003"
    session = dbsession(bind=ENGINE)
    stmt = session.query(IdMatch).join(
        DOrgPerson, and_(IdMatch.source_id == DOrgPerson.person_id, IdMatch.data_source == DOrgPerson.data_source,
        IdMatch.id_type == 3, IdMatch.is_used == 1, IdMatch.data_source == DATA_SOURCE)
    ).join(
        PersonInfo, and_(IdMatch.matched_id == PersonInfo.person_id, IdMatch.id_type == 3, IdMatch.is_used == 1, IdMatch.data_source == DATA_SOURCE)
    ).with_entities(
        IdMatch.matched_id.label(OrgPersonMapping.person_id.name), PersonInfo.person_name, DOrgPerson.org_id,
        DOrgPerson.tenure_date, DOrgPerson.dimission_date, DOrgPerson.is_current, DOrgPerson.duty
    )

    inp = MysqlInput(session.bind, stmt)

    fi = stream_oi(DATA_SOURCE)

    jn = transform.Join(fi, left_on=DOrgPerson.org_id.name, right_on=IdMatch.source_id.name)

    sk = transform.MapSelectKeys(
        {
            OrgPersonMapping.person_id.name: None,
            PersonInfo.person_name.name: OrgPersonMapping.person_name.name,
            IdMatch.matched_id.name: OrgPersonMapping.org_id.name,
            OrgInfo.org_name.name: OrgPersonMapping.org_name.name,
            DOrgPerson.duty.name: OrgPersonMapping.duty.name,
            DOrgPerson.tenure_date.name: OrgPersonMapping.tenure_date.name,
            DOrgPerson.dimission_date.name: OrgPersonMapping.dimission_date.name,
            DOrgPerson.is_current.name: OrgPersonMapping.is_current.name
        }
    )

    s = base.Stream(inp, (jn, sk))
    return s


def confluence():
    s21 = stream_020001()
    s22 = stream_020002()
    s23 = stream_020003()

    c = base.Confluence(s21, s22, s23, on=[OrgPersonMapping.person_id.name, OrgPersonMapping.org_id.name, OrgPersonMapping.duty.name])

    return c


def main():
    c = confluence()
    io.to_sql(OrgPersonMapping.__tablename__, ENGINE, c.dataframe)

if __name__ == "__main__":
    main()
