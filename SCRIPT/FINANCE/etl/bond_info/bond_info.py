from utils.database import config as cfg
from utils.etlkit.core import transform
from utils.etlkit.core.base import Frame, Stream, Confluence
from utils.etlkit.reader.mysqlreader import MysqlNativeInput, MysqlOrmInput
from utils.database import io
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker, Session
from utils.database.models.crawl_finance import DBondInfo
from utils.database.models.base_finance import BondInfo

import re

engine_r = cfg.load_engine()["etl_finance"]
engine_w = cfg.load_engine()["etl_base_finance"]
dbsession = sessionmaker()


def stream_010001():
    """
    债券基本信息(上海证交所)
    """
    session = dbsession(bind=engine_r)

    stmt = Session().query(DBondInfo).filter(
        and_(DBondInfo.source_id == "010001", DBondInfo.bond_id.like("%\.__"))
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.source_id, DBondInfo.bond_full_name,  # 名称类数据
        DBondInfo.issue_price, DBondInfo.issue_amount,  # 发行价格类数据
        DBondInfo.coupon_rate, DBondInfo.par_value,  # 条款类数据
        DBondInfo.maturity_date, DBondInfo.issue_date_start, DBondInfo.issue_date_end, DBondInfo.listing_date,  # 日期类数据
        DBondInfo.consigner, DBondInfo.consignee,  # 相关机构数据
    )

    inp = MysqlOrmInput(session.bind, stmt)

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondInfo.bond_id.name,
            DBondInfo.source_id.name: None,
            DBondInfo.bond_name.name: BondInfo.bond_name.name,
            DBondInfo.bond_full_name.name: BondInfo.bond_full_name.name,
            DBondInfo.maturity_date.name: BondInfo.maturity_date.name,
            DBondInfo.issue_price.name: BondInfo.issue_price.name,
            DBondInfo.issue_amount.name: BondInfo.issue_amount.name,
            DBondInfo.par_value.name: BondInfo.par_value.name,
            DBondInfo.coupon_rate.name: BondInfo.coupon_rate.name,
            DBondInfo.issue_date_start.name: BondInfo.issue_date_start.name,
            DBondInfo.issue_date_end.name: BondInfo.issue_date_end.name,
            DBondInfo.listing_date.name: BondInfo.listing_date.name,
            DBondInfo.consigner.name: BondInfo.consigner.name,
            DBondInfo.consignee.name: BondInfo.consignee.name,
        }
    )

    stream = Stream(
        inp, (km,), name="d_bond_info_sse"
    )

    return stream


def stream_020001():
    """
    债券基本信息(和讯)
    """
    session = dbsession(bind=engine_r)

    stmt = Session().query(DBondInfo).filter(
        and_(DBondInfo.source_id == "020001", DBondInfo.bond_id.like("%\.__"))
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.source_id, DBondInfo.bond_full_name,  # 名称类数据
        DBondInfo.issue_price, DBondInfo.issue_amount,  # 发行价格类数据
        DBondInfo.term, DBondInfo.coupon_rate, DBondInfo.par_value,  # 条款类数据
        DBondInfo.maturity_date, DBondInfo.issue_date_start, DBondInfo.issue_date_end, DBondInfo.listing_date,  # 日期类数据
        DBondInfo.consigner, DBondInfo.consignee,  # 相关机构数据
    )

    inp = MysqlOrmInput(session.bind, stmt)

    vm = transform.ValueMap(
        {
            DBondInfo.consignee.name: lambda x: ",".join(re.split(",|，|、| ", x))
        }
    )

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondInfo.bond_id.name,
            DBondInfo.source_id.name: None,
            DBondInfo.bond_name.name: BondInfo.bond_name.name,
            DBondInfo.bond_full_name.name: BondInfo.bond_full_name.name,
            DBondInfo.maturity_date.name: BondInfo.maturity_date.name,
            DBondInfo.issue_price.name: BondInfo.issue_price.name,
            DBondInfo.issue_amount.name: BondInfo.issue_amount.name,
            DBondInfo.par_value.name: BondInfo.par_value.name,
            DBondInfo.coupon_rate.name: BondInfo.coupon_rate.name,
            DBondInfo.term.name: BondInfo.term.name,
            DBondInfo.issue_date_start.name: BondInfo.issue_date_start.name,
            DBondInfo.issue_date_end.name: BondInfo.issue_date_end.name,
            DBondInfo.listing_date.name: BondInfo.listing_date.name,
            DBondInfo.consigner.name: BondInfo.consigner.name,
            DBondInfo.consignee.name: BondInfo.consignee.name,
        }
    )

    stream = Stream(
        inp, (vm, km,), name="d_bond_info_hexun"
    )

    return stream


def stream_020002():
    """
    债券基本信息(东方财富)
    """
    session = dbsession(bind=engine_r)

    stmt = Session().query(DBondInfo).filter(
        and_(DBondInfo.source_id == "020002", DBondInfo.bond_id.like("%\.__"))
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.source_id, DBondInfo.bond_full_name,  # 名称类数据
        DBondInfo.issue_price, DBondInfo.issue_amount,  # 发行价格类数据
        DBondInfo.term, DBondInfo.coupon_rate,  # 条款类数据
        DBondInfo.maturity_date, DBondInfo.issue_date_start, DBondInfo.listing_date,  # 日期类数据
        DBondInfo.consigner,  # 相关机构数据
    )

    inp = MysqlOrmInput(session.bind, stmt)

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondInfo.bond_id.name,
            DBondInfo.source_id.name: None,
            DBondInfo.bond_name.name: BondInfo.bond_name.name,
            DBondInfo.bond_full_name.name: BondInfo.bond_full_name.name,
            DBondInfo.maturity_date.name: BondInfo.maturity_date.name,
            DBondInfo.issue_price.name: BondInfo.issue_price.name,
            DBondInfo.issue_amount.name: BondInfo.issue_amount.name,
            DBondInfo.coupon_rate.name: BondInfo.coupon_rate.name,
            DBondInfo.term.name: BondInfo.term.name,
            DBondInfo.issue_date_start.name: BondInfo.issue_date_start.name,
            DBondInfo.listing_date.name: BondInfo.listing_date.name,
            DBondInfo.consigner.name: BondInfo.consigner.name,
        }
    )

    stream = Stream(
        inp, (km,), name="d_bond_eastmoney"
    )

    return stream


def stream_020003():
    """
    债券基本信息(金融界)
    """
    session = dbsession(bind=engine_r)

    stmt = Session().query(DBondInfo).filter(
        and_(DBondInfo.source_id == "020003", DBondInfo.bond_id.like("%\.__"))
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.source_id, DBondInfo.bond_full_name,  # 名称类数据
        DBondInfo.par_value,  # 条款类数据
        DBondInfo.maturity_date, DBondInfo.issue_date_start, DBondInfo.issue_date_end, DBondInfo.listing_date, DBondInfo.value_date, # 日期类数据
        DBondInfo.consigner,  # 相关机构数据
    )

    inp = MysqlOrmInput(session.bind, stmt)

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondInfo.bond_id.name,
            DBondInfo.source_id.name: None,
            DBondInfo.bond_name.name: BondInfo.bond_name.name,
            DBondInfo.bond_full_name.name: BondInfo.bond_full_name.name,
            DBondInfo.maturity_date.name: BondInfo.maturity_date.name,
            DBondInfo.term.name: BondInfo.term.name,
            DBondInfo.issue_date_start.name: BondInfo.issue_date_start.name,
            DBondInfo.issue_date_end.name: BondInfo.issue_date_end.name,
            DBondInfo.value_date.name: BondInfo.value_date.name,
            DBondInfo.consigner.name: BondInfo.consigner.name,
        }
    )

    stream = Stream(
        inp, (km,)#, name="d_bond_info_jrj"
    )

    return stream


def main():
    s11 = stream_010001()
    s21 = stream_020001()
    s22 = stream_020002()
    s23 = stream_020003()

    # 122577.SH, par_value
    key_priority = {
        0: {
            BondInfo.issue_price.name: (DBondInfo.source_id.name, "020002"),
            BondInfo.issue_amount.name: (DBondInfo.source_id.name, "010001"),
            BondInfo.coupon_rate.name: (DBondInfo.source_id.name, "020001"),
            BondInfo.maturity_date.name: (DBondInfo.source_id.name, "010001"),
            BondInfo.value_date.name: (DBondInfo.source_id.name, "020003"),
        },
        1: {
            BondInfo.issue_price.name: (DBondInfo.source_id.name, "020001"),
            BondInfo.issue_amount.name: (DBondInfo.source_id.name, "020001"),
            BondInfo.coupon_rate.name: (DBondInfo.source_id.name, "020002"),
            BondInfo.maturity_date.name: (DBondInfo.source_id.name, "020002"),
        },
        2: {
            BondInfo.issue_price.name: (DBondInfo.source_id.name, "010001"),
            BondInfo.issue_amount.name: (DBondInfo.source_id.name, "020002"),
            BondInfo.coupon_rate.name: (DBondInfo.source_id.name, "010001"),
            BondInfo.maturity_date.name: (DBondInfo.source_id.name, "020003"),
        },
        3: {
            BondInfo.maturity_date.name: (DBondInfo.source_id.name, "020001"),
        },
    }
    conflu = Confluence(s11, s21, s22, s23, on=BondInfo.bond_id.name, prio_l1=key_priority)
    io.to_sql(BondInfo.__tablename__, engine_w, conflu.dataframe.drop(DBondInfo.source_id.name, axis=1))


if __name__ == "__main__":
    main()


def tmp():
    import pandas as pd
    from sqlalchemy import create_engine
    eg = create_engine("mysql+pymysql://jr_etl_01:smytetl01@182.254.128.241:4171/crawl_finance", pool_size=50, connect_args={"charset": "utf8"})
    df = pd.read_excel(r"C:\Users\Yu\Documents\WeChat Files\wxid_rc6z8uvrexqz21\Files\【1227】interest_type云通源.xlsx")
    df["source_id"] = "000001"
    df_bi = df[["bond_id", "bond_full_name", "source_id", "interest_type", "interest_freq"]]
    df_bi = df_bi.dropna(subset=["interest_type", "interest_freq"], how="all")
    io.to_sql("y_bond_info", eg, df_bi)

    df_bd = df[["bond_id", "source_id", "利率说明"]]
    df_bd.columns = ["bond_id", "source_id", "remark"]
    df_bd = df_bd.dropna(subset=["remark"], how="all")
    io.to_sql("y_bond_description", eg, df_bd)
