from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput
from utils.database.models.base_private import FundOrgMapping
from collections import OrderedDict

ENGINE_C = cfg.load_engine()["2Gcpri"]
ENGINE_B = cfg.load_engine()["etl_base_private"]


def inp_xof():
    sql_maxver = "SELECT im_org.org_id, im_fund.fund_id, xof.org_type, fi.fund_name, oi.org_name FROM x_org_fund xof " \
                 "JOIN (SELECT org_id, fund_id, MAX(version) latest_ver FROM x_org_fund GROUP BY org_id) tb_latest " \
                 "ON xof.org_id = tb_latest.org_id AND xof.fund_id = tb_latest.fund_id " \
                 "JOIN (SELECT matched_id as org_id, source_id as org_id_src FROM base.id_match WHERE source = '010001' AND id_type = 2 AND is_used = 1) im_org " \
                 "ON xof.org_id = im_org.org_id_src " \
                 "JOIN (SELECT matched_id as fund_id, source_id as fund_id_src FROM base.id_match WHERE source IN ('010002', '010003', '010004', '010005') AND id_type=1 AND is_used = 1) im_fund " \
                 "ON xof.fund_id = im_fund.fund_id_src AND xof.version = tb_latest.latest_ver " \
                 "LEFT JOIN base.fund_info fi ON fi.fund_id = im_fund.fund_id "\
                 "LEFT JOIN base.org_info oi ON oi.org_id = im_org.org_id"

    inp = MysqlInput(ENGINE_C, sql_maxver)

    s = Stream(inp, transform=())
    return s


def stream_xof_manager(s):
    vm = transform.ValueMap(
        OrderedDict([
            (FundOrgMapping.org_type_code.name, (lambda x: {"受托管理": 2, "自我管理": 2}.get(x), "org_type")),
            (FundOrgMapping.org_type.name, (lambda x: {"受托管理": "基金管理人", "自我管理": "基金管理人"}.get(x), "org_type"))
        ])
    )

    dn = transform.Dropna()
    sk = transform.MapSelectKeys(
        {
            "fund_id": FundOrgMapping.fund_id.name,
            "org_id": FundOrgMapping.org_id.name,
            FundOrgMapping.org_type.name: None,
            FundOrgMapping.org_type_code.name: None,
            "fund_name": FundOrgMapping.fund_name.name,
            "org_name": FundOrgMapping.org_name.name
        }
    )
    s = Stream(s, transform=(vm, dn, sk))
    return s


def stream_xof_consultant(s):
    vm = transform.ValueMap(
        OrderedDict([
            (FundOrgMapping.org_type_code.name, (lambda x: {"顾问管理": 1, "自我管理": 1}.get(x), "org_type")),
            (FundOrgMapping.org_type.name, (lambda x: {"顾问管理": "投资顾问", "自我管理": "投资顾问",}.get(x), "org_type"))
        ])
    )

    dn = transform.Dropna()
    sk = transform.MapSelectKeys(
        {
            "fund_id": FundOrgMapping.fund_id.name,
            "org_id": FundOrgMapping.org_id.name,
            FundOrgMapping.org_type.name: None,
            FundOrgMapping.org_type_code.name: None,
            "fund_name": FundOrgMapping.fund_name.name,
            "org_name": FundOrgMapping.org_name.name
        }
    )
    s = Stream(s, transform=(vm, dn, sk))
    return s


def conflu_manager():
    s = inp_xof()
    s.flow()
    s1 = stream_xof_manager(s)
    c = Confluence(s1)
    return c


def conflu_consultant():
    s = inp_xof()
    s.flow()
    s2 = stream_xof_consultant(s)
    c = Confluence(s2)
    return c
