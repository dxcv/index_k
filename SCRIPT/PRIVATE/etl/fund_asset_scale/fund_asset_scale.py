from sqlalchemy.orm import sessionmaker
from utils.database.models.base_private import FundAssetScale
from utils.etlkit.reader.mysqlreader import MysqlInput
from utils.etlkit.core import base, transform
from utils.database import config as cfg, io


dbsession = sessionmaker()
ENGINE_C = cfg.load_engine()["etl_crawl_private"]
ENGINE_B = cfg.load_engine()["etl_base_private"]
ENGINE_T = cfg.load_engine()["etl_base_test"]


def stream_010002():
    sql_fi = "SELECT im.matched_id as fund_id, fi.fund_name, fi.foundation_date, issuing_scale_amac " \
             "FROM crawl_private.{tb} xfi " \
             "JOIN (SELECT fund_id, MAX(version) latest_ver FROM crawl_private.{tb} GROUP BY fund_id) tb_latest " \
             "ON tb_latest.latest_ver = xfi.version AND tb_latest.fund_id = xfi.fund_id " \
             "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.source = '010002' AND im.id_type = 1 AND im.is_used = 1 " \
             "LEFT JOIN base.fund_info fi ON fi.fund_id = im.matched_id".format(tb="x_fund_info_fundaccount")

    inp = MysqlInput(ENGINE_C, sql_fi)

    vm = transform.ValueMap(
        {
            "issuing_scale_amac": lambda x: float(x) * 1e4,  # 亿 -> 万
        }
    )

    vm2 = transform.ValueMap(
        {
            "issuing_scale_amac": lambda x: None if x == 0 else x,
        }
    )

    dn = transform.Dropna(subset=["fund_id", "foundation_date", "issuing_scale_amac"])

    sk = transform.MapSelectKeys(
        {
            "fund_id": FundAssetScale.fund_id.name,
            "fund_name": FundAssetScale.fund_name.name,
            "issuing_scale_amac": FundAssetScale.asset_scale.name,
            "foundation_date": FundAssetScale.statistic_date.name,
        }
    )

    s = base.Stream(inp, transform=(vm, vm2, dn, sk))
    return s


def stream_010004():
    sql_fi = "SELECT im.matched_id as fund_id, fi.fund_name, fi.foundation_date, issuing_scale_amac " \
             "FROM crawl_private.{tb} xfi " \
             "JOIN (SELECT fund_id, MAX(version) latest_ver FROM crawl_private.{tb} GROUP BY fund_id) tb_latest " \
             "ON tb_latest.latest_ver = xfi.version AND tb_latest.fund_id = xfi.fund_id " \
             "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.source = '010004' AND im.id_type = 1 AND im.is_used = 1 " \
             "LEFT JOIN base.fund_info fi ON fi.fund_id = im.matched_id".format(tb="x_fund_info_securities")

    inp = MysqlInput(ENGINE_C, sql_fi)

    vm = transform.ValueMap(
        {
            "issuing_scale_amac": lambda x: float(x),
        }
    )

    vm2 = transform.ValueMap(
        {
            "issuing_scale_amac": lambda x: None if x == 0 else x,
        }
    )

    dn = transform.Dropna(subset=["fund_id", "foundation_date", "issuing_scale_amac"])

    sk = transform.MapSelectKeys(
        {
            "fund_id": FundAssetScale.fund_id.name,
            "fund_name": FundAssetScale.fund_name.name,
            "issuing_scale_amac": FundAssetScale.asset_scale.name,
            "foundation_date": FundAssetScale.statistic_date.name,
        }
    )

    s = base.Stream(inp, transform=(vm, vm2, dn, sk))
    return s


def stream_010005():
    sql_fi = "SELECT im.matched_id as fund_id, fi.fund_name, fi.foundation_date, issuing_scale_amac " \
             "FROM crawl_private.{tb} xfi " \
             "JOIN (SELECT fund_id, MAX(version) latest_ver FROM crawl_private.{tb} GROUP BY fund_id) tb_latest " \
             "ON tb_latest.latest_ver = xfi.version AND tb_latest.fund_id = xfi.fund_id " \
             "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.source = '010005' AND im.id_type = 1 AND im.is_used = 1 " \
             "LEFT JOIN base.fund_info fi ON fi.fund_id = im.matched_id".format(tb="x_fund_info_futures")

    inp = MysqlInput(ENGINE_C, sql_fi)

    vm = transform.ValueMap(
        {
            "issuing_scale_amac": lambda x: float(x),
        }
    )

    vm2 = transform.ValueMap(
        {
            "issuing_scale_amac": lambda x: None if x == 0 else x,
        }
    )

    dn = transform.Dropna(subset=["fund_id", "foundation_date", "issuing_scale_amac"])

    sk = transform.MapSelectKeys(
        {
            "fund_id": FundAssetScale.fund_id.name,
            "fund_name": FundAssetScale.fund_name.name,
            "issuing_scale_amac": FundAssetScale.asset_scale.name,
            "foundation_date": FundAssetScale.statistic_date.name,
        }
    )

    s = base.Stream(inp, transform=(vm, vm2, dn, sk))
    return s


def stream_020004():
    sql_fi = "SELECT im.matched_id as fund_id, fi.fund_name, fi.foundation_date, fi.init_total_asset " \
             "FROM crawl_private.{tb} dfi " \
             "JOIN (SELECT fund_id, MAX(version) latest_ver FROM crawl_private.{tb} GROUP BY fund_id) tb_latest " \
             "ON tb_latest.latest_ver = dfi.version AND tb_latest.fund_id = dfi.fund_id " \
             "JOIN base.id_match im ON im.source_id = dfi.fund_id AND im.source = '020004' AND im.id_type = 1 AND im.is_used = 1 " \
             "LEFT JOIN base.fund_info fi ON fi.fund_id = im.matched_id".format(tb="d_fund_info")

    inp = MysqlInput(ENGINE_C, sql_fi)

    vm = transform.ValueMap(
        {
            "init_total_asset": lambda x: float(x),
        }
    )

    vm2 = transform.ValueMap(
        {
            "init_total_asset": lambda x: None if x == 0 else x,
        }
    )

    dn = transform.Dropna(subset=["fund_id", "foundation_date", "init_total_asset"])

    sk = transform.MapSelectKeys(
        {
            "fund_id": FundAssetScale.fund_id.name,
            "fund_name": FundAssetScale.fund_name.name,
            "init_total_asset": FundAssetScale.asset_scale.name,
            "foundation_date": FundAssetScale.statistic_date.name,
        }
    )

    s = base.Stream(inp, transform=(vm, vm2, dn, sk))
    return s


def stream_020005():
    sql_fi = "SELECT im.matched_id as fund_id, fi.fund_name, fi.foundation_date, fi.init_total_asset " \
             "FROM crawl_private.{tb} dfi " \
             "JOIN (SELECT fund_id, MAX(version) latest_ver FROM crawl_private.{tb} GROUP BY fund_id) tb_latest " \
             "ON tb_latest.latest_ver = dfi.version AND tb_latest.fund_id = dfi.fund_id " \
             "JOIN base.id_match im ON im.source_id = dfi.fund_id AND im.source = '020005' AND im.id_type = 1 AND im.is_used = 1 " \
             "LEFT JOIN base.fund_info fi ON fi.fund_id = im.matched_id".format(tb="d_fund_info")

    inp = MysqlInput(ENGINE_C, sql_fi)

    vm = transform.ValueMap(
        {
            "init_total_asset": lambda x: float(x),
        }
    )

    vm2 = transform.ValueMap(
        {
            "init_total_asset": lambda x: None if x == 0 else x,
        }
    )

    dn = transform.Dropna(subset=["fund_id", "foundation_date", "init_total_asset"])

    sk = transform.MapSelectKeys(
        {
            "fund_id": FundAssetScale.fund_id.name,
            "fund_name": FundAssetScale.fund_name.name,
            "init_total_asset": FundAssetScale.asset_scale.name,
            "foundation_date": FundAssetScale.statistic_date.name,
        }
    )

    s = base.Stream(inp, transform=(vm, vm2, dn, sk))
    return s


def stream_020008():
    sql_fi = "SELECT im.matched_id as fund_id, fi.fund_name, fi.foundation_date, fi.init_total_asset " \
             "FROM crawl_private.{tb} dfi " \
             "JOIN (SELECT fund_id, MAX(version) latest_ver FROM crawl_private.{tb} GROUP BY fund_id) tb_latest " \
             "ON tb_latest.latest_ver = dfi.version AND tb_latest.fund_id = dfi.fund_id " \
             "JOIN base.id_match im ON im.source_id = dfi.fund_id AND im.source = '020008' AND im.id_type = 1 AND im.is_used = 1 " \
             "LEFT JOIN base.fund_info fi ON fi.fund_id = im.matched_id".format(tb="d_fund_info")

    inp = MysqlInput(ENGINE_C, sql_fi)

    vm = transform.ValueMap(
        {
            "init_total_asset": lambda x: float(x),
        }
    )

    vm2 = transform.ValueMap(
        {
            "init_total_asset": lambda x: None if x == 0 else x,
        }
    )

    dn = transform.Dropna(subset=["fund_id", "foundation_date", "init_total_asset"])

    sk = transform.MapSelectKeys(
        {
            "fund_id": FundAssetScale.fund_id.name,
            "fund_name": FundAssetScale.fund_name.name,
            "init_total_asset": FundAssetScale.asset_scale.name,
            "foundation_date": FundAssetScale.statistic_date.name,
        }
    )

    s = base.Stream(inp, transform=(vm, vm2, dn, sk))
    return s


# 25期货资管（万元）、28排排（万元）、24朝阳永续（元） 可同步d_fund_info中init_total_asset


def main():
    s_12 = stream_010002()  # 39855
    s_14 = stream_010004()  # 7125
    s_15 = stream_010005()  # 6428
    # s_24 = stream_020004()  # 30929
    # s_25 = stream_020005()  # 1933
    # s_28 = stream_020008()  # 1

    c = base.Confluence(s_12, s_14, s_15, on=["fund_id"])

    io.to_sql(FundAssetScale.__tablename__, ENGINE_B, c.dataframe)


if __name__ == "__main__":
    main()
