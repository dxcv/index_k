from utils.database import config as cfg
from utils.etlkit.core.base import Frame, Stream, Confluence
from utils.etlkit.reader.mysqlreader import MysqlInput, MysqlNativeInput
from utils.etlkit.core import transform
from utils.database import io
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker, Session
from utils.database.models.crawl_finance import DBondInfo, YBondInfo
from utils.database.models.base_finance import BondInfo


engine_r = cfg.load_engine()["etl_finance"]
engine_w = cfg.load_engine()["etl_base_finance"]
dbsession = sessionmaker()


def stream_000001():
    session = dbsession(bind=engine_r)
    stmt = session.query(YBondInfo).join(
        BondInfo, YBondInfo.bond_id == BondInfo.bond_id
    ).filter(
        and_(YBondInfo.is_used == 1, YBondInfo.source_id == "000001")
    ).with_entities(
        YBondInfo.bond_id,
        YBondInfo.interest_freq
    )
    inp = MysqlInput(session.bind, stmt)

    dn = transform.Dropna(subset=[YBondInfo.interest_freq.name])

    km = transform.MapSelectKeys(
        {
            YBondInfo.bond_id.name: BondInfo.bond_id.name,
            YBondInfo.source_id.name: None,
            YBondInfo.interest_freq.name: BondInfo.interest_freq.name,
        }
    )

    s = Stream(inp, (dn, km,))

    return s


def stream_010001():
    session = dbsession(bind=engine_r)
    stmt = session.query(DBondInfo).join(
        BondInfo, DBondInfo.bond_id == BondInfo.bond_id
    ).filter(
        DBondInfo.source_id == "010001"
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.source_id, DBondInfo.bond_full_name,
        DBondInfo.interest_freq, DBondInfo.interest_freq
    )
    inp = MysqlInput(session.bind, stmt)

    def clean_interest_freq(type_, freq, fname):
        if fname.find("贴现") >= 0:
            return "贴现"

        if type_ in {"零息", "贴现"}:
            return "贴现"
        elif type_ == "一次还本息":
            return "到期一次还本付息"
        else:

            d_freq = {
                "到期一次还本付息": "到期一次还本付息",  # 除去type_为"零息"的情况, 其余freq为"到期一次还本付息"映射为"到期一次还本付息"
                "按年付息": "年付息",
                "按半年付息": "半年付息",
                "按季度付息": "季度付息",
                "按月付息": "月付息",
            }
            return d_freq.get(freq)

    vm = transform.ValueMap(
        {
            BondInfo.interest_freq.name: (lambda type_, freq, fname: clean_interest_freq(type_, freq, fname),
                       DBondInfo.interest_freq.name, DBondInfo.interest_freq.name, DBondInfo.bond_full_name.name)
        }
    )

    dn = transform.Dropna(subset=[BondInfo.interest_freq.name])

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondInfo.bond_id.name,
            BondInfo.interest_freq.name: None,
        }
    )

    s = Stream(inp, (vm, dn, km))

    return s


def stream_020001():
    session = dbsession(bind=engine_r)
    stmt = session.query(DBondInfo).join(
        BondInfo, DBondInfo.bond_id == BondInfo.bond_id
    ).filter(
        DBondInfo.source_id == "020001"
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.source_id, DBondInfo.bond_full_name,
        DBondInfo.interest_type, DBondInfo.interest_freq
    )
    inp = MysqlInput(session.bind, stmt)

    def clean_interest_freq(type_, freq, fname):
        if fname.find("贴现") >= 0:
            return "贴现"

        if type_ in {"贴现", "零息"}:
            return "贴现"
        else:
            d_type_freq = {
                "固定": {
                    "年付": "年付息",
                    "半年付": "半年付息",
                    "季付": "季度付息",
                    "月付": "月付息",
                    "贴现": "贴现"
                },
                "浮动": {
                    "年付": "年付息",
                    "半年付": "半年付息",
                    "季付": "季度付息",
                    "月付": "月付息",
                }
            }
            return d_type_freq.get(type_).get(freq)

    vm = transform.ValueMap(
        {
            DBondInfo.interest_freq.name: (lambda type_, freq, fname: clean_interest_freq(type_, freq, fname),
                       DBondInfo.interest_type.name, DBondInfo.interest_freq.name, DBondInfo.bond_full_name.name)
        }
    )

    dn = transform.Dropna(subset=[BondInfo.interest_freq.name])

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondInfo.bond_id.name,
            BondInfo.interest_freq.name: None,
        }
    )

    s = Stream(inp, (vm, dn, km))

    return s


def stream_020002():
    session = dbsession(bind=engine_r)
    stmt = session.query(DBondInfo).join(
        BondInfo, DBondInfo.bond_id == BondInfo.bond_id
    ).filter(
        DBondInfo.source_id == "020002"
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.source_id, DBondInfo.bond_full_name,
        DBondInfo.interest_type, DBondInfo.interest_freq
    )
    inp = MysqlInput(session.bind, stmt)

    def clean_interest_freq(type_, freq, fname):
        if fname.find("贴现") >= 0:
            return "贴现"

        if type_ == "固定利率" and freq == "到期一次性还本付息":
            return "到期一次还本付息"
        elif type_ == "贴现债券":
            return "贴现"

    vm = transform.ValueMap(
        {
            DBondInfo.interest_freq.name: (lambda type_, freq, fname: clean_interest_freq(type_, freq, fname),
                       DBondInfo.interest_type.name, DBondInfo.interest_freq.name, DBondInfo.bond_full_name.name)
        }
    )

    dn = transform.Dropna(subset=[BondInfo.interest_freq.name])

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondInfo.bond_id.name,
            BondInfo.interest_freq.name: None,
        }
    )

    s = Stream(inp, (vm, dn, km))

    return s


def stream_020003():
    session = dbsession(bind=engine_r)
    stmt = session.query(DBondInfo).join(
        BondInfo, DBondInfo.bond_id == BondInfo.bond_id
    ).filter(
        DBondInfo.source_id == "020003"
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.source_id, DBondInfo.bond_full_name,
        DBondInfo.interest_type, DBondInfo.interest_freq
    )
    inp = MysqlInput(session.bind, stmt)

    def clean_interest_freq(freq, fname):
        if fname.find("贴现") >= 0:
            return "贴现"

        d_freq = {
            "到期一次还本付息":  "到期一次还本付息",
            "贴现": "贴现",
            "按年付息": "年付息",
            "半年付息": "半年付息",
            "按季付息": "季度付息",
        }
        return d_freq.get(freq)

    vm = transform.ValueMap(
        {
            DBondInfo.interest_freq.name: (lambda freq, fname: clean_interest_freq(freq, fname),
                       DBondInfo.interest_freq.name, DBondInfo.bond_full_name.name)
        }
    )

    dn = transform.Dropna(subset=[BondInfo.interest_freq.name])

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondInfo.bond_id.name,
            BondInfo.interest_freq.name: None,
        }
    )

    s = Stream(inp, (vm, dn, km))

    return s


def main():
    s01 = stream_000001()
    s11 = stream_010001()
    s21 = stream_020001()
    s22 = stream_020002()
    s23 = stream_020003()
    c = Confluence(s01, s11, s21, s22, s23, on=[BondInfo.bond_id.name])
    io.to_sql(BondInfo.__tablename__, engine_w, c.dataframe)


if __name__ == "__main__":
    main()
