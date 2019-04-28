from utils.database import config as cfg, io
from utils.database.models.base_private import FundTypeSource
from utils.etlkit.core import transform, common, base
from utils.etlkit.reader.mysqlreader import MysqlInput
from collections import OrderedDict
from utils.decofactory.common import inscache

ENGINE = cfg.load_engine()["2Gb"]


class Streams:
    SQL_102 = "SELECT im.matched_id, xfi.fund_type_allocation_amac, fi.fund_name " \
              "FROM {tb} xfi " \
              "JOIN (SELECT fund_id, MAX(version) latest_ver FROM {tb} GROUP BY fund_id) t " \
              "ON xfi.fund_id = t.fund_id AND xfi.version = t.latest_ver " \
              "JOIN (SELECT matched_id, source_id FROM base.id_match WHERE source = '{sid}' AND id_type = 1 AND is_used = 1) im " \
              "ON xfi.fund_id = im.source_id " \
              "LEFT JOIN fund_info fi ON im.matched_id = fi.fund_id"

    SQL_103 = "SELECT im.matched_id, xfi.fund_type_target_amac, fi.fund_name " \
              "FROM {tb} xfi " \
              "JOIN (SELECT fund_id, MAX(version) latest_ver FROM {tb} GROUP BY fund_id) t " \
              "ON xfi.fund_id = t.fund_id AND xfi.version = t.latest_ver " \
              "JOIN (SELECT matched_id, source_id FROM base.id_match WHERE source = '{sid}' AND id_type = 1 AND is_used = 1) im " \
              "ON xfi.fund_id = im.source_id " \
              "LEFT JOIN fund_info fi ON im.matched_id = fi.fund_id"

    SQL_104 = "SELECT im.matched_id, fi.fund_name " \
              "FROM {tb} xfi " \
              "JOIN (SELECT fund_id, MAX(version) latest_ver FROM {tb} GROUP BY fund_id) t " \
              "ON xfi.fund_id = t.fund_id AND xfi.version = t.latest_ver " \
              "JOIN (SELECT matched_id, source_id FROM base.id_match WHERE source = '{sid}' AND id_type = 1 AND is_used = 1) im " \
              "ON xfi.fund_id = im.source_id " \
              "LEFT JOIN fund_info fi ON im.matched_id = fi.fund_id"

    SK = transform.MapSelectKeys(
        {
            "matched_id": FundTypeSource.fund_id.name,
            "fund_name": FundTypeSource.fund_name.name,
            FundTypeSource.source_id.name: None,
            FundTypeSource.typestandard_code.name: None,
            FundTypeSource.typestandard_name.name: None,
            FundTypeSource.type_code.name: None,
            FundTypeSource.type_name.name: None
        }
    )

    # 2 结构化类型
    TS_MAPPER_102 = {
        "是": ("10201", "是"),
        "否": ("10202", "否"),
    }

    # 3 投资标的
    TS_MAPPER_103 = {
        "债券": ("10301", "债券"),
        "混合": ("10302", "混合"),
        "股票": ("10303", "股票"),
        "现金管理": ("10304", "现金管理"),
        "货币": ("10305", "货币"),
        "混合类产品": ("10306", "混合类产品"),
        "权益类产品": ("10307", "权益类产品"),
        "期货类产品": ("10308", "期货类产品"),
        "固定收益类产品": ("10309", "固定收益类产品"),
        "基金宝产品": ("10310", "基金宝产品"),
        "股票类产品": ("10311", "股票类产品"),
        "其他产品": ("10399", "其他产品"),
        "其它产品": ("10399", "其他产品"),
    }

    # 4 发行主体
    TS_MAPPER_104 = {
        "股权投资基金": ("10401", "股权投资基金"),
        "信托计划": ("10402", "信托计划"),
        "其他私募投资基金": ("10403", "其他私募投资基金"),
        "创业投资基金": ("10404", "创业投资基金"),
        "基金专户": ("10405", "基金专户"),
        "证券公司及其子公司的资产管理计划": ("10406", "证券公司及其子公司的资产管理计划"),
        "私募证券投资基金": ("10407", "私募证券投资基金"),
        "银行理财产品": ("10408", "银行理财产品"),
        # "暂时空缺": ("10409", "暂时空缺"),
        "期货公司及其子公司的资产管理计划": ("10410", "期货公司及其子公司的资产管理计划"),
        "保险公司及其子公司的资产管理计划": ("10411", "保险公司及其子公司的资产管理计划"),
        "基金子公司": ("10412", "基金子公司"),
        "其他": ("10499", "其他"),
    }

    # 102(按结构类型分类)
    @classmethod
    def stream_010002_ts102(cls):
        source_id = "010002"
        tb = "crawl_private.x_fund_info_fundaccount"
        tmpsql = cls.SQL_102.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(
            {
                FundTypeSource.source_id.name: source_id,
                FundTypeSource.typestandard_code.name: "102",
                FundTypeSource.typestandard_name.name: "按结构类型分类",
                FundTypeSource.type_code.name: (lambda x: cls.TS_MAPPER_102.get(x, [None, None])[0], "fund_type_allocation_amac"),
                FundTypeSource.type_name.name: (lambda x: cls.TS_MAPPER_102.get(x, [None, None])[1], "fund_type_allocation_amac"),
            }
        )

        dn = transform.Dropna(subset=[FundTypeSource.fund_id.name, FundTypeSource.type_code.name])

        s = base.Stream(inp, [vm, cls.SK, dn])
        return s

    @classmethod
    def stream_010004_ts102(cls):
        source_id = "010004"
        tb = "crawl_private.x_fund_info_securities"
        tmpsql = cls.SQL_102.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(
            {
                FundTypeSource.source_id.name: source_id,
                FundTypeSource.typestandard_code.name: "102",
                FundTypeSource.typestandard_name.name: "按结构类型分类",
                FundTypeSource.type_code.name: (lambda x: cls.TS_MAPPER_102.get(x, [None, None])[0], "fund_type_allocation_amac"),
                FundTypeSource.type_name.name: (lambda x: cls.TS_MAPPER_102.get(x, [None, None])[1], "fund_type_allocation_amac"),
            }
        )

        dn = transform.Dropna(subset=[FundTypeSource.fund_id.name, FundTypeSource.type_code.name])

        s = base.Stream(inp, [vm, cls.SK, dn])
        return s

    @classmethod
    def stream_010005_ts102(cls):
        source_id = "010005"
        tb = "crawl_private.x_fund_info_futures"
        tmpsql = cls.SQL_102.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(
            {
                FundTypeSource.source_id.name: source_id,
                FundTypeSource.typestandard_code.name: "102",
                FundTypeSource.typestandard_name.name: "按结构类型分类",
                FundTypeSource.type_code.name: (lambda x: cls.TS_MAPPER_102.get(x, [None, None])[0], "fund_type_allocation_amac"),
                FundTypeSource.type_name.name: (lambda x: cls.TS_MAPPER_102.get(x, [None, None])[1], "fund_type_allocation_amac"),
            }
        )

        dn = transform.Dropna(subset=[FundTypeSource.fund_id.name, FundTypeSource.type_code.name])

        s = base.Stream(inp, [vm, cls.SK, dn])
        return s

    # 103(按投资标的分类)
    @classmethod
    def stream_010002_ts103(cls):
        source_id = "010002"
        tb = "crawl_private.x_fund_info_fundaccount"
        tmpsql = cls.SQL_103.format(sid=source_id, tb=tb)
        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(OrderedDict(
            [
                [FundTypeSource.type_code.name, (lambda x: cls.TS_MAPPER_103.get(x, [None, None])[0], "fund_type_target_amac")],
                [FundTypeSource.type_name.name, (lambda x: cls.TS_MAPPER_103.get(x, [None, None])[1], "fund_type_target_amac")],
                [FundTypeSource.typestandard_code.name, "103"],
                [FundTypeSource.typestandard_name.name, "按投资标的分类"],
                [FundTypeSource.source_id.name, source_id]
            ]
        ))

        sk = transform.MapSelectKeys(
            {
                "matched_id": FundTypeSource.fund_id.name,
                "fund_name": FundTypeSource.fund_name.name,
                FundTypeSource.source_id.name: None,
                FundTypeSource.typestandard_name.name: None,
                FundTypeSource.typestandard_code.name: None,
                FundTypeSource.type_code.name: None,
                FundTypeSource.type_name.name: None,
            }
        )

        dn = transform.Dropna(subset=[FundTypeSource.type_code.name])

        s = base.Stream(inp, transform=(vm, sk, dn))
        return s

    @classmethod
    def stream_010004_ts103(cls):
        source_id = "010004"
        tb = "crawl_private.x_fund_info_securities"
        tmpsql = cls.SQL_103.format(sid=source_id, tb=tb)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(OrderedDict(
            [
                [FundTypeSource.type_code.name, (lambda x: cls.TS_MAPPER_103.get(x, [None, None])[0], "fund_type_target_amac")],
                [FundTypeSource.type_name.name, (lambda x: cls.TS_MAPPER_103.get(x, [None, None])[1], "fund_type_target_amac")],
                [FundTypeSource.typestandard_code.name, "103"],
                [FundTypeSource.typestandard_name.name, "按投资标的分类"],
                [FundTypeSource.source_id.name, "010004"]
            ]
        ))

        sk = transform.MapSelectKeys(
            {
                "matched_id": FundTypeSource.fund_id.name,
                "fund_name": FundTypeSource.fund_name.name,
                FundTypeSource.source_id.name: None,
                FundTypeSource.typestandard_name.name: None,
                FundTypeSource.typestandard_code.name: None,
                FundTypeSource.type_code.name: None,
                FundTypeSource.type_name.name: None,
            }
        )

        dn = transform.Dropna(subset=[FundTypeSource.type_code.name])

        s = base.Stream(inp, transform=(vm, sk, dn))
        return s

    @classmethod
    def stream_010005_ts103(cls):
        source_id = "010005"
        tb = "crawl_private.x_fund_info_futures"
        tmpsql = cls.SQL_103.format(sid=source_id, tb=tb)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(OrderedDict(
            [
                [FundTypeSource.type_code.name, (lambda x: cls.TS_MAPPER_103.get(x, [None, None])[0], "fund_type_target_amac")],
                [FundTypeSource.type_name.name, (lambda x: cls.TS_MAPPER_103.get(x, [None, None])[1], "fund_type_target_amac")],
                [FundTypeSource.typestandard_code.name, "103"],
                [FundTypeSource.typestandard_name.name, "按投资标的分类"],
                [FundTypeSource.source_id.name, source_id]
            ]
        ))

        sk = transform.MapSelectKeys(
            {
                "matched_id": FundTypeSource.fund_id.name,
                "fund_name": FundTypeSource.fund_name.name,
                FundTypeSource.source_id.name: None,
                FundTypeSource.typestandard_name.name: None,
                FundTypeSource.typestandard_code.name: None,
                FundTypeSource.type_code.name: None,
                FundTypeSource.type_name.name: None,
            }
        )

        dn = transform.Dropna(subset=[FundTypeSource.type_code.name])

        s = base.Stream(inp, transform=(vm, sk, dn))
        return s

    # 104(按发行主体分类)
    @classmethod
    def stream_010002_ts104(cls):
        source_id = "010002"
        tb = "crawl_private.x_fund_info_fundaccount"
        tmpsql = cls.SQL_104.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(
            {
                FundTypeSource.source_id.name: source_id,
                FundTypeSource.typestandard_code.name: "104",
                FundTypeSource.typestandard_name.name: "按发行主体分类",
                FundTypeSource.type_code.name: cls.TS_MAPPER_104["基金专户"][0],
                FundTypeSource.type_name.name: cls.TS_MAPPER_104["基金专户"][1]
            }
        )

        s = base.Stream(inp, [vm, cls.SK])
        return s

    @classmethod
    def stream_010003_ts104(cls):
        source_id = "010003"
        tb = "crawl_private.x_fund_info_private"

        sql = "SELECT im.matched_id, xfi.type_name_amac, fi.fund_name " \
              "FROM {tb} xfi " \
              "JOIN (SELECT fund_id, MAX(version) latest_ver FROM {tb} GROUP BY fund_id) t " \
              "ON xfi.fund_id = t.fund_id AND xfi.version = t.latest_ver " \
              "JOIN (SELECT matched_id, source_id FROM base.id_match WHERE source = '{sid}' AND id_type = 1 AND is_used = 1) im " \
              "ON xfi.fund_id = im.source_id " \
              "LEFT JOIN fund_info fi ON im.matched_id = fi.fund_id".format(
            tb=tb, sid=source_id
        )

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap(
            {
                FundTypeSource.source_id.name: source_id,
                FundTypeSource.typestandard_code.name: "104",
                FundTypeSource.typestandard_name.name: "按发行主体分类",
                FundTypeSource.type_code.name: (lambda x: cls.TS_MAPPER_104.get(x, [None, None])[0], "type_name_amac"),
                FundTypeSource.type_name.name: (lambda x: cls.TS_MAPPER_104.get(x, [None, None])[1], "type_name_amac")
            }
        )

        dn = transform.Dropna(subset=[FundTypeSource.fund_id.name, FundTypeSource.type_code.name])

        s = base.Stream(inp, [vm, cls.SK, dn])
        return s

    @classmethod
    def stream_010004_ts104(cls):
        source_id = "010004"
        tb = "crawl_private.x_fund_info_securities"
        tmpsql = cls.SQL_104.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(
            {
                FundTypeSource.source_id.name: source_id,
                FundTypeSource.typestandard_code.name: "104",
                FundTypeSource.typestandard_name.name: "按发行主体分类",
                FundTypeSource.type_code.name: cls.TS_MAPPER_104["证券公司及其子公司的资产管理计划"][0],
                FundTypeSource.type_name.name: cls.TS_MAPPER_104["证券公司及其子公司的资产管理计划"][1]
            }
        )

        s = base.Stream(inp, [vm, cls.SK])
        return s

    @classmethod
    def stream_010005_ts104(cls):
        source_id = "010005"
        tb = "crawl_private.x_fund_info_futures"
        tmpsql = cls.SQL_104.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(
            {
                FundTypeSource.source_id.name: source_id,
                FundTypeSource.typestandard_code.name: "104",
                FundTypeSource.typestandard_name.name: "按发行主体分类",
                FundTypeSource.type_code.name: cls.TS_MAPPER_104["期货公司及其子公司的资产管理计划"][0],
                FundTypeSource.type_name.name: cls.TS_MAPPER_104["期货公司及其子公司的资产管理计划"][1]
            }
        )

        s = base.Stream(inp, [vm, cls.SK])
        return s

    @classmethod
    def conflu_ts102(cls):
        s12, s15, s14 = cls.stream_010002_ts102(), cls.stream_010005_ts102(), cls.stream_010004_ts102()
        c = base.Confluence(s12, s15, s14,)
        return c

    @classmethod
    def conflu_ts103(cls):
        s12, s15, s14 = cls.stream_010002_ts103(), cls.stream_010005_ts103(), cls.stream_010004_ts103()
        c = base.Confluence(s12, s15, s14,)
        return c

    @classmethod
    def conflu_ts104(cls):
        s12 = cls.stream_010002_ts104()
        s15 = cls.stream_010005_ts104()
        s14 = cls.stream_010004_ts104()
        s13 = cls.stream_010003_ts104()
        c = base.Confluence(s12, s15, s14, s13)
        return c
