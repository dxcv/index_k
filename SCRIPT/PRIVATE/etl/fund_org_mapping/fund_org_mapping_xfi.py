from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput
from utils.database.models.base_private import FundOrgMapping
from collections import OrderedDict
import pandas as pd

ENGINE_C = cfg.load_engine()["2Gcpri"]
ENGINE_B = cfg.load_engine()["etl_base_private"]
ENGINE_W = cfg.load_engine()["etl_base_test"]

TABLE = "base.fund_org_mapping"


class Streams:
    FIELDS = {
        "matched_id": FundOrgMapping.fund_id.name,
        "fund_name": FundOrgMapping.fund_name.name,
        "org_id": FundOrgMapping.org_id.name,
        "org_name": FundOrgMapping.org_name.name,
        FundOrgMapping.org_type_code.name: None,
        FundOrgMapping.org_type.name: None,
        "foundation_date": FundOrgMapping.start_date.name,
        "end_date": FundOrgMapping.end_date.name,
        FundOrgMapping.is_current.name: None
    }

    @classmethod
    def _fetch_olddata(cls, org_type_code):
        """

        Args:
            org_type_code: int, optional {1, 2}

        Returns:
            pd.DataFrame{
                columns: ["fund_id, "org_id"]
            }

        """
        sql = "SELECT org_id, fund_id, org_name, fund_name, start_date, end_date, is_current " \
              "FROM {tb_test} WHERE org_type_code = {otc}".format(
            tb_test=TABLE, otc=org_type_code)
        df = pd.read_sql(sql, ENGINE_B)
        return df

    @classmethod
    def _clean_iscurrent(cls, fund_status):
        if fund_status in {"运行中", "募集发行"}:
            return 1
        elif fund_status == "终止":
            return 0
        else:
            return None

    @classmethod
    def delete_wrong(cls, df_new, org_type_code):
        d = {1: "投资顾问", 2: "基金管理人"}
        df_existed = cls._fetch_olddata(org_type_code)
        fids_new = set(df_new["fund_id"])
        df_existed = df_existed.loc[df_existed["fund_id"].apply(lambda x: x in fids_new)]
        pairs_existed = set([tuple(x) for x in df_existed[["fund_id", "org_id"]].as_matrix()])
        pairs_new = set([tuple(x) for x in df_new[["fund_id", "org_id"]].as_matrix()])
        res = pd.DataFrame(sorted(pairs_existed - pairs_new), columns=["fund_id", "org_id"])
        res = res.merge(df_existed, on=["fund_id", "org_id"])
        res["org_type_code"] = org_type_code
        res["org_type"] = res["org_type_code"].apply(lambda x: d[x])
        return res.dropna(subset=["fund_id", "org_id"])

    @classmethod
    def _d_oi_manager(cls):
        # 全称相同的机构清洗管理人时, 优先使用org_category <> 私募基金管理公司的;
        sql_oi = "SELECT org_id, org_full_name FROM base.org_info WHERE org_category <> '私募基金管理公司'" \
                 "UNION (SELECT org_id, org_full_name FROM base.org_info WHERE org_category = '私募基金管理公司')"
        df = pd.read_sql(sql_oi, ENGINE_C)
        df["org_full_name"] = df["org_full_name"].apply(lambda x: x.strip() if type(x) is str else x)
        df = df.drop_duplicates(subset=["org_full_name"])
        return dict(df[["org_full_name", "org_id"]].as_matrix())

    @classmethod
    def _d_oi_consultant(cls):
        # 仅使用org_category为私募基金管理公司的
        sql_oi = "SELECT org_id, org_full_name FROM base.org_info WHERE org_category IN ('私募基金管理公司', '证券公司', '证券子公司')"
        df = pd.read_sql(sql_oi, ENGINE_C)
        df["org_full_name"] = df["org_full_name"].apply(lambda x: x.strip() if type(x) is str else x)
        return dict(df[["org_full_name", "org_id"]].as_matrix())

    @classmethod
    def stream_manager_000001(cls):
        """
            清洗000001源(基金专户)发行产品的基金管理人配对信息;

            Args:
                d_xoi:

            Returns:

        """

        sql = "SELECT fund_id, fund_name, oi.org_id, oi.org_name, org_type_code, start_date, end_date, is_current " \
              "FROM crawl_private.y_fund_org_mapping yfom " \
              "JOIN base.org_info oi ON yfom.org_id = oi.org_id " \
              "WHERE org_type_code = 2 AND is_used = 1"

        inp = MysqlInput(ENGINE_C, sql)

        vm = transform.ValueMap({
            "org_type": "基金管理人"
        })

        sk = transform.MapSelectKeys({
            "fund_id": None,
            "fund_name": None,
            "org_id": None,
            "org_name": None,
            "org_type": None,
            "org_type_code": None,
            "start_date": None,
            "end_date": None,
            "is_current": None,
        })
        dn = transform.Dropna(
            subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name, FundOrgMapping.org_type_code.name]
        )
        s = Stream(inp, transform=[vm, sk, dn])
        return s

    @classmethod
    def stream_manager_010002(cls, d_xoi=None):
        """
            清洗010002源(基金专户)发行产品的基金管理人配对信息;

            Args:
                d_xoi:

            Returns:

        """

        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_fundaccount xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_fundaccount GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010002' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi " \
                     "ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010002' " \
                     "WHERE fund_name_amac NOT LIKE '%%信托计划'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        if d_xoi is None:
            d_xoi = cls._d_oi_manager()

        vm = transform.ValueMap(OrderedDict([
            ("fund_issue_org_amac", lambda x: x.strip()),
            ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
            (FundOrgMapping.org_type.name, "基金管理人"),  # 所有来自基金专户的, fund_issue_org_amac都是基金管理人
            (FundOrgMapping.org_type_code.name, 2),
            (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
        ]))

        jn = transform.Join(inp2, how="left", on="org_id")

        sk = transform.MapSelectKeys(cls.FIELDS)
        dn = transform.Dropna(
            subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name, FundOrgMapping.org_type_code.name])

        s = Stream(inp, transform=(vm, jn, sk, dn,))
        return s

    @classmethod
    def stream_manager_010003(cls, d_xoi=None):
        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, manage_type_amac, type_name_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_private xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_private GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010003' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi " \
                     "ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010003' " \
                     "WHERE fund_name_amac NOT LIKE '%%信托计划'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        def clean_org_type(mng_tp, tp):
            # 私募证券投资基金 = 证券投资基金; 其他私募投资基金 = 其他投资基金
            if mng_tp == "顾问管理":
                if tp in {"私募证券投资基金", "证券投资基金"}:
                    return "基金管理人"

            elif mng_tp == "受托管理":
                if tp in {"私募证券投资基金", "证券投资基金", "创业投资基金", "其他私募投资基金", "其他投资基金", "股权投资基金"}:
                    return "基金管理人"

            elif mng_tp == "自我管理":
                if tp in {"私募证券投资基金", "证券投资基金", "股权投资基金", "创业投资基金", "其他私募投资基金", "其他投资基金"}:
                    return "基金管理人"

        if d_xoi is None:
            d_xoi = cls._d_oi_manager()

        vm = transform.ValueMap(
            OrderedDict([
                ("fund_issue_org_amac", lambda x: x.strip()),
                ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
                (FundOrgMapping.org_type.name,
                 (lambda mng_tp, tp: clean_org_type(mng_tp, tp), "manage_type_amac", "type_name_amac")),
                (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
            ])
        )

        jn = transform.Join(inp2, how="left", on="org_id")

        dn = transform.Dropna(subset=["matched_id", "org_id", FundOrgMapping.org_type.name])

        vm2 = transform.ValueMap({
            FundOrgMapping.org_type_code.name: (lambda x: 2 if x == "基金管理人" else None, FundOrgMapping.org_type.name),
        })

        sk = transform.MapSelectKeys(cls.FIELDS)

        s = Stream(inp, transform=(vm, jn, dn, vm2, sk))
        return s

    @classmethod
    def stream_manager_010004(cls, d_xoi=None):
        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, manage_type_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_securities xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_securities GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010004' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi " \
                     "ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010004' " \
                     "WHERE fund_name_amac NOT LIKE '%%信托计划'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        if d_xoi is None:
            d_xoi = cls._d_oi_manager()

        vm = transform.ValueMap(
            OrderedDict([
                ("fund_issue_org_amac", lambda x: x.strip()),
                ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
                (FundOrgMapping.org_type.name,
                 (lambda x: {"主动管理": "基金管理人", "被动管理": "基金管理人"}.get(x), "manage_type_amac")),
                (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
            ])
        )

        jn = transform.Join(inp2, how="left", on="org_id")

        dn = transform.Dropna(subset=["matched_id", "org_id", FundOrgMapping.org_type.name])

        vm2 = transform.ValueMap({
            FundOrgMapping.org_type_code.name: (lambda x: 2 if x == "基金管理人" else None, FundOrgMapping.org_type.name),
        })

        sk = transform.MapSelectKeys(cls.FIELDS)
        s = Stream(inp, transform=(vm, jn, dn, vm2, sk))  # 先去空, 避免空值产生, 导致整形列
        return s

    @classmethod
    def stream_manager_010005(cls, d_xoi=None):
        """
            清洗010005源(期货公司)发行产品的基金管理人配对信息;

        Args:
            d_xoi:

        Returns:

        """

        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_futures xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_futures GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010005' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi " \
                     "ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010005' " \
                     "WHERE fund_name_amac NOT LIKE '%%信托计划'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        if d_xoi is None:
            d_xoi = cls._d_oi_manager()

        vm = transform.ValueMap(OrderedDict([
            ("fund_issue_org_amac", lambda x: x.strip()),
            ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
            (FundOrgMapping.org_type.name, "基金管理人"),  # 该来源的fund_issue_org_amac都是基金管理人;
            (FundOrgMapping.org_type_code.name, 2),
            (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
        ]))

        jn = transform.Join(inp2, how="left", on="org_id")

        sk = transform.MapSelectKeys(cls.FIELDS)
        dn = transform.Dropna(
            subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name, FundOrgMapping.org_type_code.name])

        s = Stream(inp, transform=(vm, jn, sk, dn,))
        return s

    @classmethod
    def stream_manager_010002_(cls, d_xoi=None):
        """
            清洗010002源(基金专户)发行产品的基金管理人配对信息;

            Args:
                d_xoi:

            Returns:

        """

        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_fundaccount xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_fundaccount GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010002' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010002' " \
                     "WHERE fund_name_amac LIKE '%%信托计划' AND fund_issue_org_amac LIKE '%%信托%%有限公司%%'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        if d_xoi is None:
            d_xoi = cls._d_oi_manager()

        vm = transform.ValueMap(OrderedDict([
            ("fund_issue_org_amac", lambda x: x.strip()),
            ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
            (FundOrgMapping.org_type.name, "基金管理人"),  # 所有来自基金专户的, fund_issue_org_amac都是基金管理人
            (FundOrgMapping.org_type_code.name, 2),
            (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
        ]))

        jn = transform.Join(inp2, how="left", on="org_id")

        sk = transform.MapSelectKeys(cls.FIELDS)
        dn = transform.Dropna(
            subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name, FundOrgMapping.org_type_code.name])

        s = Stream(inp, transform=(vm, jn, sk, dn,))
        return s

    @classmethod
    def stream_manager_010003_(cls, d_xoi=None):
        """
            清洗010003源(私募基金公司)发行产品的基金管理人配对信息;

            Args:
                d_xoi:

            Returns:

        """

        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_private xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_private GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010003' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010003' " \
                     "WHERE fund_name_amac LIKE '%%信托计划' AND fund_issue_org_amac LIKE '%%信托%%有限公司%%'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        if d_xoi is None:
            d_xoi = cls._d_oi_manager()

        vm = transform.ValueMap(OrderedDict([
            ("fund_issue_org_amac", lambda x: x.strip()),
            ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
            (FundOrgMapping.org_type.name, "基金管理人"),  # 所有来自基金专户的, fund_issue_org_amac都是基金管理人
            (FundOrgMapping.org_type_code.name, 2),
            (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
        ]))

        jn = transform.Join(inp2, how="left", on="org_id")

        sk = transform.MapSelectKeys(cls.FIELDS)
        dn = transform.Dropna(
            subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name, FundOrgMapping.org_type_code.name])

        s = Stream(inp, transform=(vm, jn, sk, dn,))
        return s

    @classmethod
    def stream_manager_010004_(cls, d_xoi=None):
        """
            清洗010004源(证券公司)发行产品的基金管理人配对信息;

            Args:
                d_xoi:

            Returns:

        """

        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_securities xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_securities GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010004' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010004' " \
                     "WHERE fund_name_amac LIKE '%%信托计划' AND fund_issue_org_amac LIKE '%%信托%%有限公司%%'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        if d_xoi is None:
            d_xoi = cls._d_oi_manager()

        vm = transform.ValueMap(OrderedDict([
            ("fund_issue_org_amac", lambda x: x.strip()),
            ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
            (FundOrgMapping.org_type.name, "基金管理人"),  # 所有来自基金专户的, fund_issue_org_amac都是基金管理人
            (FundOrgMapping.org_type_code.name, 2),
            (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
        ]))

        jn = transform.Join(inp2, how="left", on="org_id")

        sk = transform.MapSelectKeys(cls.FIELDS)
        dn = transform.Dropna(
            subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name, FundOrgMapping.org_type_code.name])

        s = Stream(inp, transform=(vm, jn, sk, dn,))
        return s

    @classmethod
    def stream_manager_010005_(cls, d_xoi=None):
        """
            清洗010005源(期货公司)发行产品的基金管理人配对信息;

            Args:
                d_xoi:

            Returns:

        """

        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_futures xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_futures GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010005' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010005' " \
                     "WHERE fund_name_amac LIKE '%%信托计划' AND fund_issue_org_amac LIKE '%%信托%%有限公司%%'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        if d_xoi is None:
            d_xoi = cls._d_oi_manager()

        vm = transform.ValueMap(OrderedDict([
            ("fund_issue_org_amac", lambda x: x.strip()),
            ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
            (FundOrgMapping.org_type.name, "基金管理人"),  # 所有来自基金专户的, fund_issue_org_amac都是基金管理人
            (FundOrgMapping.org_type_code.name, 2),
            (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
        ]))

        jn = transform.Join(inp2, how="left", on="org_id")

        sk = transform.MapSelectKeys(cls.FIELDS)
        dn = transform.Dropna(
            subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name, FundOrgMapping.org_type_code.name])

        s = Stream(inp, transform=(vm, jn, sk, dn,))
        return s

    @classmethod
    def stream_consultant_000001(cls):
        sql = "SELECT fund_id, fund_name, oi.org_id, oi.org_name, org_type_code, start_date, end_date, is_current " \
              "FROM crawl_private.y_fund_org_mapping yfom " \
              "JOIN base.org_info oi ON yfom.org_id = oi.org_id " \
              "WHERE org_type_code = 1 AND is_used = 1"

        inp = MysqlInput(ENGINE_C, sql)

        vm = transform.ValueMap({
            "org_type": "投资顾问"
        })

        sk = transform.MapSelectKeys({
            "fund_id": None,
            "fund_name": None,
            "org_id": None,
            "org_name": None,
            "org_type": None,
            "org_type_code": None,
            "start_date": None,
            "end_date": None,
            "is_current": None,
        })
        dn = transform.Dropna(
            subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name, FundOrgMapping.org_type_code.name]
        )
        s = Stream(inp, transform=[vm, sk, dn])
        return s

    @classmethod
    def stream_consultant_010003(cls, d_xoi=None):
        """
            清洗010003源(私募基金公司)发行产品的投顾公司配对信息;

        Args:
            d_xoi:

        Returns:

        """

        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, manage_type_amac, type_name_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_private xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_private GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010003' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010003'" \
                     "WHERE fund_name_amac NOT LIKE '%%信托计划'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        def clean_org_type(manage_type, type_name):
            if manage_type == "顾问管理":
                if type_name in {"信托计划", "期货公司及其子公司的资产管理计划", "其他", "基金子公司", "基金专户",
                                 "证券公司及其子公司的资产管理计划", "银行理财产品", "保险公司及其子公司的资产管理计划",
                                 "私募证券投资基金", "私募投资基金"}:
                    return "投资顾问"

            elif manage_type == "受托管理":
                if type_name in {"创业投资基金", "其他私募投资基金", "其他投资基金", "股权投资基金"}:
                    return "投资顾问"

            elif manage_type == "自我管理":
                if type_name in {"私募证券投资基金", "私募投资基金", "股权投资基金", "创业投资基金", "其他私募投资基金", "其他投资基金"}:
                    return "投资顾问"

        if d_xoi is None:
            d_xoi = cls._d_oi_consultant()

        vm = transform.ValueMap(
            OrderedDict([
                ("fund_issue_org_amac", lambda x: x.strip()),
                ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
                (FundOrgMapping.org_type.name,
                 (lambda mng_tp, tp: clean_org_type(mng_tp, tp), "manage_type_amac", "type_name_amac")),
                (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
            ])
        )

        jn = transform.Join(inp2, how="left", on="org_id")

        dn = transform.Dropna(subset=["matched_id", "org_id", FundOrgMapping.org_type.name])

        vm2 = transform.ValueMap({
            FundOrgMapping.org_type_code.name: (lambda x: 1 if x == "投资顾问" else None, FundOrgMapping.org_type.name),
        })

        dd = transform.DropDuplicate(subset=["matched_id", "org_id", FundOrgMapping.org_type.name])

        sk = transform.MapSelectKeys(
            {
                "matched_id": FundOrgMapping.fund_id.name,
                "fund_name": FundOrgMapping.fund_name.name,
                "org_id": FundOrgMapping.org_id.name,
                "org_name": FundOrgMapping.org_name.name,
                FundOrgMapping.org_type_code.name: None,
                FundOrgMapping.org_type.name: None,
            }
        )
        s = Stream(inp, transform=(vm, jn, dn, vm2, dd, sk))
        return s

    @classmethod
    def stream_consultant_010004(cls, d_xoi=None):
        """
            清洗010004源(证券公司)发行产品的投顾公司配对信息;

        Args:
            d_xoi:

        Returns:

        """

        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, manage_type_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_securities xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_securities GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010004' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010004'" \
                     "WHERE fund_name_amac NOT LIKE '%%信托计划'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        if d_xoi is None:
            d_xoi = cls._d_oi_consultant()

        vm = transform.ValueMap(
            OrderedDict([
                ("fund_issue_org_amac", lambda x: x.strip()),
                ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
                (FundOrgMapping.org_type.name, (lambda x: {"主动管理": "投资顾问"}.get(x), "manage_type_amac")),
                (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
            ])
        )

        jn = transform.Join(inp2, how="left", on="org_id")

        dn = transform.Dropna(subset=["matched_id", "org_id", FundOrgMapping.org_type.name])

        vm2 = transform.ValueMap({
            FundOrgMapping.org_type_code.name: (lambda x: 1 if x == "投资顾问" else None, FundOrgMapping.org_type.name),
        })

        dd = transform.DropDuplicate(subset=["matched_id", "org_id", FundOrgMapping.org_type.name])

        sk = transform.MapSelectKeys(cls.FIELDS)
        s = Stream(inp, transform=[vm, jn, dn, vm2, dd, sk])  # 先去空, 避免空值产生, 导致整形列变成浮点型
        return s

    @classmethod
    def stream_consultant_010002_(cls, d_xoi=None):
        """
            清洗010002源(基金专户)发行产品的投顾配对信息;

            Args:
                d_xoi:

            Returns:

        """

        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_fundaccount xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_fundaccount GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010002' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010002' " \
                     "WHERE fund_name_amac LIKE '%%信托计划' AND fund_issue_org_amac NOT LIKE '%%信托%%有限公司%%'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        if d_xoi is None:
            d_xoi = cls._d_oi_consultant()

        vm = transform.ValueMap(OrderedDict([
            ("fund_issue_org_amac", lambda x: x.strip()),
            ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
            (FundOrgMapping.org_type.name, "投资顾问"),
            (FundOrgMapping.org_type_code.name, 1),
            (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
        ]))

        jn = transform.Join(inp2, how="left", on="org_id")

        sk = transform.MapSelectKeys(cls.FIELDS)
        dn = transform.Dropna(
            subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name, FundOrgMapping.org_type_code.name])

        s = Stream(inp, transform=(vm, jn, sk, dn,))
        return s

    @classmethod
    def stream_consultant_010003_(cls, d_xoi=None):
        """
            清洗010003源(私募基金公司)发行产品的投顾配对信息;

            Args:
                d_xoi:

            Returns:

        """

        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_private xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_private GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010003' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010003' " \
                     "WHERE fund_name_amac LIKE '%%信托计划' AND fund_issue_org_amac NOT LIKE '%%信托%%有限公司%%'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        if d_xoi is None:
            d_xoi = cls._d_oi_consultant()

        vm = transform.ValueMap(OrderedDict([
            ("fund_issue_org_amac", lambda x: x.strip()),
            ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
            (FundOrgMapping.org_type.name, "投资顾问"),
            (FundOrgMapping.org_type_code.name, 1),
            (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
        ]))

        jn = transform.Join(inp2, how="left", on="org_id")

        sk = transform.MapSelectKeys(cls.FIELDS)
        dn = transform.Dropna(
            subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name, FundOrgMapping.org_type_code.name])

        s = Stream(inp, transform=(vm, jn, sk, dn,))
        return s

    @classmethod
    def stream_consultant_010004_(cls, d_xoi=None):
        """
            清洗010004源(证券公司)发行产品的投顾配对信息;

            Args:
                d_xoi:

            Returns:

        """

        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_securities xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_securities GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010004' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010004' " \
                     "WHERE fund_name_amac LIKE '%%信托计划' AND fund_issue_org_amac NOT LIKE '%%信托%%有限公司%%'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        if d_xoi is None:
            d_xoi = cls._d_oi_consultant()

        vm = transform.ValueMap(OrderedDict([
            ("fund_issue_org_amac", lambda x: x.strip()),
            ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
            (FundOrgMapping.org_type.name, "投资顾问"),
            (FundOrgMapping.org_type_code.name, 1),
            (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
        ]))

        jn = transform.Join(inp2, how="left", on="org_id")

        sk = transform.MapSelectKeys(cls.FIELDS)
        dn = transform.Dropna(
            subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name, FundOrgMapping.org_type_code.name])

        s = Stream(inp, transform=(vm, jn, sk, dn,))
        return s

    @classmethod
    def stream_consultant_010005_(cls, d_xoi=None):
        """
            清洗010005源(期货公司)发行产品的投顾配对信息;

            Args:
                d_xoi:

            Returns:

        """

        sql_maxver = "SELECT im.matched_id, xfi.fund_issue_org_amac, fi.fund_name, fi.foundation_date, fi.end_date, fi.fund_status " \
                     "FROM x_fund_info_futures xfi " \
                     "JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_futures GROUP BY fund_id) tb_latest " \
                     "ON xfi.version = tb_latest.latest_ver AND xfi.fund_id = tb_latest.fund_id " \
                     "JOIN base.id_match im ON im.source_id = xfi.fund_id AND im.id_type = 1 AND im.source = '010005' AND im.is_used = 1 " \
                     "LEFT JOIN base.fund_info fi ON im.matched_id = fi.fund_id AND im.id_type = 1 AND im.source = '010005' " \
                     "WHERE fund_name_amac LIKE '%%信托计划' AND fund_issue_org_amac NOT LIKE '%%信托%%有限公司%%'"

        inp = MysqlInput(ENGINE_C, sql_maxver)

        inp2 = MysqlInput(ENGINE_B, "SELECT org_id, org_name FROM base.org_info")

        if d_xoi is None:
            d_xoi = cls._d_oi_consultant()

        vm = transform.ValueMap(OrderedDict([
            ("fund_issue_org_amac", lambda x: x.strip()),
            ("org_id", (lambda x: d_xoi.get(x), "fund_issue_org_amac")),
            (FundOrgMapping.org_type.name, "投资顾问"),
            (FundOrgMapping.org_type_code.name, 1),
            (FundOrgMapping.is_current.name, (lambda x: cls._clean_iscurrent(x), "fund_status"))
        ]))

        jn = transform.Join(inp2, how="left", on="org_id")

        sk = transform.MapSelectKeys(cls.FIELDS)
        dn = transform.Dropna(
            subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name, FundOrgMapping.org_type_code.name])

        s = Stream(inp, transform=(vm, jn, sk, dn,))
        return s

    @classmethod
    def conflu_consultant(cls):
        d_xoi = cls._d_oi_consultant()

        streams = [
            cls.stream_consultant_000001(),
            cls.stream_consultant_010002_(d_xoi), cls.stream_consultant_010005_(d_xoi),
            cls.stream_consultant_010004_(d_xoi), cls.stream_consultant_010003_(d_xoi),
            cls.stream_consultant_010003(d_xoi), cls.stream_consultant_010004(d_xoi),
        ]

        # stream的定义需根据业务要求保持对应的顺序, 优先级越高, 顺序越靠前
        c = Confluence(*streams, )
        dn = transform.DropDuplicate(subset=[FundOrgMapping.org_id.name, FundOrgMapping.fund_id.name])
        s = Stream(c, transform=[dn])
        return Confluence(s)

    @classmethod
    def conflu_manager(cls):
        d_xoi = cls._d_oi_manager()
        streams = [
            cls.stream_manager_000001(),
            cls.stream_manager_010002_(d_xoi), cls.stream_manager_010005_(d_xoi), cls.stream_manager_010004_(d_xoi),
            cls.stream_manager_010003_(d_xoi), cls.stream_manager_010002(d_xoi), cls.stream_manager_010005(d_xoi),
            cls.stream_manager_010004(d_xoi), cls.stream_manager_010003(d_xoi)
        ]

        # stream的定义需根据业务要求保持对应的顺序, 优先级越高, 顺序越靠前
        c = Confluence(*streams, )
        dn = transform.DropDuplicate(subset=[FundOrgMapping.fund_id.name])
        s = Stream(c, transform=[dn])
        return Confluence(s)

    @classmethod
    def clean(cls):
        """
            执行所有FundOrgMapping 表的清洗;

        Returns:

        """

        c1 = cls.conflu_consultant()
        c2 = cls.conflu_manager()
        for res in [c1, c2]:
            io.to_sql(TABLE, ENGINE_B, res.dataframe)

        w1 = cls.delete_wrong(c1.dataframe, 1)
        w2 = cls.delete_wrong(c2.dataframe, 2)

        for df_del in [w1, w2]:
            io.to_sql("y_fund_org_mapping", ENGINE_C, df_del)
            io.delete(
                TABLE, ENGINE_B,
                df_del[[FundOrgMapping.org_id.name, FundOrgMapping.fund_id.name, FundOrgMapping.org_type_code.name]]
            )
