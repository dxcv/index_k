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
        and_(YBondInfo.is_used == 1, YBondInfo.is_used == 1)
    ).with_entities(
        YBondInfo.bond_id,
        YBondInfo.interest_type
    )
    inp = MysqlInput(session.bind, stmt)

    dn = transform.Dropna(subset=[YBondInfo.interest_type.name])

    km = transform.MapSelectKeys(
        {
            YBondInfo.bond_id.name: BondInfo.bond_id.name,
            YBondInfo.source_id.name: None,
            YBondInfo.interest_type.name: BondInfo.interest_type.name,
        }
    )

    s = Stream(inp, (dn, km,))

    return s


def stream_010001():
    session = dbsession(bind=engine_r)
    stmt = session.query(DBondInfo).join(
        BondInfo, DBondInfo.bond_id == BondInfo.bond_id
    ).filter(
        and_(DBondInfo.source_id == "010001", DBondInfo.is_used == 1)
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.source_id, DBondInfo.bond_full_name,
        DBondInfo.interest_type
    )
    inp = MysqlInput(session.bind, stmt)

    def clean_interest_type(type_, fname):
        if fname.find("贴现") >= 0:
            return "零息"

        else:
            d_type = {
                "零息": "零息",
                "固定利息": "固定利率",
                "浮动利息": "浮动利率",
            }

            res = d_type.get(type_)
            if res is None:
                if fname.find("短期融资券") >= 0:
                    return "固定利率"
            return res

    vm = transform.ValueMap(
        {
            BondInfo.interest_type.name: (lambda type_, fname: clean_interest_type(type_, fname),
                                          DBondInfo.interest_type.name, DBondInfo.bond_full_name.name)
        }
    )

    dn = transform.Dropna(subset=[BondInfo.interest_type.name])

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondInfo.bond_id.name,
            BondInfo.interest_type.name: None
        }
    )

    s = Stream(inp, (vm, dn, km))

    return s


def stream_020001():
    session = dbsession(bind=engine_r)
    stmt = session.query(DBondInfo).join(
        BondInfo, DBondInfo.bond_id == BondInfo.bond_id
    ).filter(
        and_(DBondInfo.source_id == "020001", DBondInfo.is_used == 1)
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.source_id, DBondInfo.bond_full_name,
        DBondInfo.interest_type, DBondInfo.interest_freq
    )
    inp = MysqlInput(session.bind, stmt)

    def clean_interest_type(type_, freq, fname):
        if fname.find("贴现") >= 0:
            return "零息"

        else:
            d_freq_type = {
                "贴现": {
                    "贴现": "零息",
                    "零息": "零息",
                    "固定": "零息",
                },
                "月付": {
                    "贴现": "零息",
                    "固定": "固定利率",
                    "浮动": "浮动利率"
                },
                "季付": {
                    "固定": "固定利率",
                    "浮动": "浮动利率",
                },
                "半年付": {
                    "固定": "固定利率",
                    "浮动": "浮动利率",
                },
                "年付": {
                    "贴现": "零息",
                    "一次还本息": "固定利率",
                    "固定": "固定利率",
                    "浮动": "浮动利率",
                },
                "固定单利": {
                    "零息": "零息",
                    "一次还本息": "固定利率",
                    "固定": None,
                    "浮动": None,
                }
            }

            d_type_default = {
                "贴现": "零息"
            }

            res = d_freq_type.get(freq).get(type_, d_type_default.get(freq))
            if res is None:
                if fname.find("短期融资券") >= 0:
                    return "固定利率"
            return res

    vm = transform.ValueMap(
        {
            BondInfo.interest_type.name: (lambda type_, freq, fname: clean_interest_type(type_, freq, fname),
                                     DBondInfo.interest_type.name, DBondInfo.interest_freq.name, DBondInfo.bond_full_name.name)
        }
    )

    dn = transform.Dropna(subset=[BondInfo.interest_type.name])

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondInfo.bond_id.name,
            BondInfo.interest_type.name: None
        }
    )

    s = Stream(inp, (vm, dn, km))

    return s


def stream_020002():
    session = dbsession(bind=engine_r)
    stmt = session.query(DBondInfo).join(
        BondInfo, DBondInfo.bond_id == BondInfo.bond_id
    ).filter(
        and_(DBondInfo.source_id == "020002", DBondInfo.is_used == 1)
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.source_id, DBondInfo.bond_full_name,
        DBondInfo.interest_type
    )
    inp = MysqlInput(session.bind, stmt)

    def clean_interest_type(type_, fname):
        d_type = {
            "固定利率": "固定利率",
            "贴现债券": "零息",
            "累进利率": "累进利率",
            "浮动利率": "浮动利率",
        }

        res = d_type.get(type_, None)
        if res is None:
            if fname.find("短期融资券") >= 0:
                return "固定利率"
        return res

    vm = transform.ValueMap(
        {
            BondInfo.interest_type.name: (lambda type_, fname: clean_interest_type(type_, fname),
                                          DBondInfo.interest_type.name, DBondInfo.bond_full_name.name)
        }
    )

    dn = transform.Dropna(subset=[BondInfo.interest_type.name])

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondInfo.bond_id.name,
            BondInfo.interest_type.name: None
        }
    )

    s = Stream(inp, (vm, dn, km))

    return s


def stream_020003():
    session = dbsession(bind=engine_r)
    stmt = session.query(DBondInfo).join(
        BondInfo, DBondInfo.bond_id == BondInfo.bond_id
    ).filter(
        and_(DBondInfo.source_id == "020003", DBondInfo.is_used == 1)
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.source_id, DBondInfo.bond_full_name,
        DBondInfo.interest_freq
    )
    inp = MysqlInput(session.bind, stmt)

    def clean_interest_type(freq, fname):
        d_freq = {
            "贴现": "零息",
            }

        res = d_freq.get(freq)
        if res is None:
            if fname.find("短期融资券") >= 0:
                return "固定利率"
        return res

    vm = transform.ValueMap(
        {
            BondInfo.interest_type.name: (lambda freq, fname: clean_interest_type(freq, fname),
                                          DBondInfo.interest_freq.name, DBondInfo.bond_full_name.name)
        }
    )

    dn = transform.Dropna(subset=[BondInfo.interest_type.name])

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondInfo.bond_id.name,
            BondInfo.interest_type.name: None
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
