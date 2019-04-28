from utils.database import config as cfg, io, sqlfactory as sf
from utils.database.models.base_private import FundTypeMappingImport
from utils.etlkit.core import transform, base
from utils.etlkit.reader.mysqlreader import MysqlInput
import re


ENGINE = cfg.load_engine()["2Gb"]
CLASSIFIED_BY = "s1"


class Streams602:
    FIXED_FIELDS = {
        "fund_id": FundTypeMappingImport.fund_id.name,
        "fund_name": FundTypeMappingImport.fund_name.name,
        FundTypeMappingImport.typestandard_code.name: None,
        FundTypeMappingImport.typestandard_name.name: None,
        FundTypeMappingImport.type_code.name: None,
        FundTypeMappingImport.type_name.name: None,
        FundTypeMappingImport.stype_code.name: None,
        FundTypeMappingImport.stype_name.name: None,
        FundTypeMappingImport.classified_by.name: None
    }

    @classmethod
    def stream_602_1(cls):
        sql = "SELECT fi.fund_id, fi.fund_name, fi.fund_full_name, fp.portfolio_type " \
              "FROM base.fund_info fi " \
              "JOIN base.fund_portfolio fp ON fi.fund_id = fp.fund_id " \
              "WHERE fi.fund_id NOT IN (SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 602) " \
              "AND fp.portfolio_type = 1"

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 602,
            FundTypeMappingImport.typestandard_name.name: "按结构类型分类",
            FundTypeMappingImport.type_code.name: 60202,
            FundTypeMappingImport.type_name.name: "结构化",
            FundTypeMappingImport.stype_code.name: 6020201,
            FundTypeMappingImport.stype_name.name: "基础份额",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream_602_2(cls):
        sql = "SELECT fi.fund_id, fi.fund_name, fi.fund_full_name " \
              "FROM base.fund_info fi " \
              "WHERE fi.fund_id NOT IN (SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 602) "

        inp = MysqlInput(ENGINE, sql)

        def clean_tc(fund_full_name):
            patt = "优先级|(?<!\w)A级|劣后级|进取级|B级|普通级|次级|风险级|中间级|管理级|结构化|分级"  # A级前面不为数字和字母
            matched = set(re.findall(patt, fund_full_name))

            if ("优先级" in matched) or ("A级" in matched):
                return 60202, "结构化", 6020202, "优先级"
            elif ("劣后级" in matched) or ("进取级" in matched)\
                    or ("B级" in matched) or ("普通级" in matched)\
                    or ("次级" in matched) or ("风险级" in matched):
                return 60202, "结构化", 6020203, "进取级/劣后级"
            elif "中间级" in matched:
                return 60202, "结构化", 6020204, "中间级（夹层）"
            elif "管理级" in matched:
                return 60202, "结构化", 6020205, "管理型份额"
            elif ("结构化" in matched) or ("分级" in matched):
                return 60202, "结构化", None, None

            return None, None, None, None

        vm0 = transform.ValueMap({
            "__tmp": (lambda x: clean_tc(x), "fund_full_name")
        })

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 602,
            FundTypeMappingImport.typestandard_name.name: "按结构类型分类",
            FundTypeMappingImport.type_code.name: (lambda x: x[0], "__tmp"),
            FundTypeMappingImport.type_name.name: (lambda x: x[1], "__tmp"),
            FundTypeMappingImport.stype_code.name: (lambda x: x[2], "__tmp"),
            FundTypeMappingImport.stype_name.name: (lambda x: x[3], "__tmp"),
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        dn = transform.Dropna(subset=[FundTypeMappingImport.type_code.name, FundTypeMappingImport.stype_code.name], how="all")

        s = base.Stream(inp, [vm0, vm, sk, dn])
        return s

    @classmethod
    def stream_602_3(cls):
        sql = "SELECT fi.fund_id, fi.fund_name, fts.type_code FROM fund_info fi " \
              "JOIN fund_type_source fts ON fi.fund_id = fts.fund_id " \
              "WHERE fi.fund_id NOT IN (" \
              "SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 602)" \
              "AND fts.type_code IN (10201, 10202, 30201, 30202)"

        inp = MysqlInput(ENGINE, sql)

        structured, unstructured = (60202, "结构化", None, None), (60201, "非结构化", None, None)

        d = {
            10201: structured,
            10202: unstructured,
            30201: structured,
            30202: unstructured
        }

        vm0 = transform.ValueMap({
            "__tmp": (lambda x:  d.get(x, (None, None, None, None)), "type_code")
        })

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 602,
            FundTypeMappingImport.typestandard_name.name: "按结构类型分类",
            FundTypeMappingImport.type_code.name: (lambda x: x[0], "__tmp"),
            FundTypeMappingImport.type_name.name: (lambda x: x[1], "__tmp"),
            FundTypeMappingImport.stype_code.name: (lambda x: x[2], "__tmp"),
            FundTypeMappingImport.stype_name.name: (lambda x: x[3], "__tmp"),
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        dn = transform.Dropna(subset=[FundTypeMappingImport.type_code.name, FundTypeMappingImport.stype_code.name], how="all")

        s = base.Stream(inp, [vm0, vm, sk, dn])
        return s

    @classmethod
    def conflu_123(cls):
        s1, s2, s3 = cls.stream_602_1(), cls.stream_602_2(), cls.stream_602_3()
        c = base.Confluence(s1, s2, s3, on=[FundTypeMappingImport.fund_id.name, FundTypeMappingImport.typestandard_code.name])
        return c

    @classmethod
    def stream_602_4(cls, c_cleaned):
        sql = "SELECT fi.fund_id, fi.fund_name " \
              "FROM base.fund_info fi " \
              "WHERE fi.fund_id NOT IN (SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 602) "

        inp = MysqlInput(ENGINE, sql)

        # 非惰性求值
        fids = set(c_cleaned.dataframe["fund_id"])

        vm0 = transform.ValueMap({
            "__tmp": (lambda x: (60201, "非结构化", None, None) if x not in fids else (None, None, None, None), "fund_id")
        })

        vm1 = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 602,
            FundTypeMappingImport.typestandard_name.name: "按结构类型分类",
            FundTypeMappingImport.type_code.name: (lambda x: x[0], "__tmp"),
            FundTypeMappingImport.type_name.name: (lambda x: x[1], "__tmp"),
            FundTypeMappingImport.stype_code.name: (lambda x: x[2], "__tmp"),
            FundTypeMappingImport.stype_name.name: (lambda x: x[3], "__tmp"),
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        dn = transform.Dropna(subset=[FundTypeMappingImport.type_code.name, FundTypeMappingImport.stype_code.name], how="all")

        s = base.Stream(inp, [vm0, vm1, sk, dn])

        return s

    @classmethod
    def conflu(cls):
        c123 = cls.conflu_123()
        c4 = cls.stream_602_4(c123)

        c = base.Confluence(c123, c4, on=[FundTypeMappingImport.fund_id.name, FundTypeMappingImport.typestandard_code.name])
        return c


class Streams603:
    SQL_BASE1 = "SELECT fi.fund_id, fi.fund_name FROM fund_type_source fts " \
                "JOIN fund_info fi ON fts.fund_id = fi.fund_id " \
                "WHERE fi.fund_id NOT IN (" \
                "SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 603) " \
                "AND fts.type_code IN {tc}"

    FIXED_FIELDS = {
        "fund_id": FundTypeMappingImport.fund_id.name,
        "fund_name": FundTypeMappingImport.fund_name.name,
        FundTypeMappingImport.typestandard_code.name: None,
        FundTypeMappingImport.typestandard_name.name: None,
        FundTypeMappingImport.type_code.name: None,
        FundTypeMappingImport.type_name.name: None,
        FundTypeMappingImport.classified_by.name: None
    }

    @classmethod
    def stream1_60301(cls):
        tc = (10303, 10311)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 603,
            FundTypeMappingImport.typestandard_name.name: "按投资标的分类",
            FundTypeMappingImport.type_code.name: 60301,
            FundTypeMappingImport.type_name.name: "股票",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream2_60302(cls):
        tc = (10308,)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 603,
            FundTypeMappingImport.typestandard_name.name: "按投资标的分类",
            FundTypeMappingImport.type_code.name: 60302,
            FundTypeMappingImport.type_name.name: "期货",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream3_60303(cls):
        tc = (10301, 10309)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 603,
            FundTypeMappingImport.typestandard_name.name: "按投资标的分类",
            FundTypeMappingImport.type_code.name: 60303,
            FundTypeMappingImport.type_name.name: "债券等固定收益",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream4_60305(cls):
        tc = (10302, 10306)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 603,
            FundTypeMappingImport.typestandard_name.name: "按投资标的分类",
            FundTypeMappingImport.type_code.name: 60305,
            FundTypeMappingImport.type_name.name: "混合型",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream5_60307(cls):
        tc = (10408, 10304, 10305)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 603,
            FundTypeMappingImport.typestandard_name.name: "按投资标的分类",
            FundTypeMappingImport.type_code.name: 60307,
            FundTypeMappingImport.type_name.name: "货币型",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream6_60309(cls):
        tc = (10401, 10404)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 603,
            FundTypeMappingImport.typestandard_name.name: "按投资标的分类",
            FundTypeMappingImport.type_code.name: 60309,
            FundTypeMappingImport.type_name.name: "股权",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream7_60311(cls):
        tc = (10399,)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 603,
            FundTypeMappingImport.typestandard_name.name: "按投资标的分类",
            FundTypeMappingImport.type_code.name: 60311,
            FundTypeMappingImport.type_name.name: "其他",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream8_60308(cls):
        tmp_sql = "SELECT fund_id, fund_name FROM base.fund_info " \
                  "WHERE fund_id NOT IN (" \
                  "SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 603) " \
                  "AND fund_full_name LIKE '%%指数型%%'"

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 603,
            FundTypeMappingImport.typestandard_name.name: "按投资标的分类",
            FundTypeMappingImport.type_code.name: 60308,
            FundTypeMappingImport.type_name.name: "指数型",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream9_60310(cls):
        tmp_sql = "SELECT fund_id, fund_name FROM base.fund_info " \
                  "WHERE fund_id NOT IN (" \
                  "SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 603) " \
                  "AND fund_full_name LIKE '%%新三板%%'"

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 603,
            FundTypeMappingImport.typestandard_name.name: "按投资标的分类",
            FundTypeMappingImport.type_code.name: 60310,
            FundTypeMappingImport.type_name.name: "新三板",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def conflu(cls):
        streams = [
            cls.stream1_60301(), cls.stream2_60302(), cls.stream3_60303(), cls.stream4_60305(),
            cls.stream5_60307(), cls.stream6_60309(), cls.stream7_60311(), cls.stream8_60308(),
            cls.stream9_60310()
        ]
        on = [FundTypeMappingImport.fund_id.name, FundTypeMappingImport.typestandard_code.name]
        conflus = [base.Confluence(*streams[i: i+4], on=on) for i in range(0, len(streams), 4)]

        c = base.Confluence(*conflus, on=on)
        return c


class Streams604:
    FIXED_FIELDS = {
        "fund_id": FundTypeMappingImport.fund_id.name,
        "fund_name": FundTypeMappingImport.fund_name.name,
        FundTypeMappingImport.typestandard_code.name: None,
        FundTypeMappingImport.typestandard_name.name: None,
        FundTypeMappingImport.type_code.name: None,
        FundTypeMappingImport.type_name.name: None,
        FundTypeMappingImport.classified_by.name: None
    }

    SQL_BASE1 = "SELECT fi.fund_id, fi.fund_name FROM fund_type_source fts " \
                "JOIN fund_info fi ON fts.fund_id = fi.fund_id " \
                "WHERE fi.fund_id NOT IN (" \
                "SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 604) " \
                "AND fts.type_code IN {tc}"

    SQL_BASE2 = "SELECT fi.fund_id, fi.fund_name " \
                "FROM fund_type_source fts " \
                "JOIN fund_info fi ON fts.fund_id = fi.fund_id WHERE fi.fund_id NOT IN (" \
                "SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 604) " \
                "AND fi.fund_id IN (" \
                "SELECT fund_id FROM fund_type_source WHERE type_code IN {tc} " \
                "GROUP BY fund_id HAVING COUNT(type_code) = {n})"

    @classmethod
    def stream1_60401(cls):
        tc = (10402,)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60401,
            FundTypeMappingImport.type_name.name: "信托",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream2_60401(cls):
        sql = "SELECT fi.fund_id, fi.fund_name, fi.fund_full_name FROM fund_type_source fts " \
              "JOIN fund_info fi ON fts.fund_id = fi.fund_id " \
              "WHERE fi.fund_id NOT IN (" \
              "SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 604) " \
              "AND fi.fund_full_name LIKE '%%信托计划'" \
              "AND fts.type_code = '10407'"

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60401,
            FundTypeMappingImport.type_name.name: "信托",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream3_60402(cls):
        tc = (10407,)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60402,
            FundTypeMappingImport.type_name.name: "自主发行",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream4_60403(cls):
        tc = (10405, 10412)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60403,
            FundTypeMappingImport.type_name.name: "公募专户及子公司资管计划",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream5_60404(cls):
        tc = (10406,)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60404,
            FundTypeMappingImport.type_name.name: "券商资管",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream6_60405(cls):
        tc = (10410,)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60405,
            FundTypeMappingImport.type_name.name: "期货资管",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream7_60406(cls):
        tc = (10411,)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60406,
            FundTypeMappingImport.type_name.name: "保险公司及子公司资管计划",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream8_60401(cls):
        sql = "SELECT fund_id, fund_name FROM base.fund_info " \
              "WHERE fund_full_name LIKE '%%信托计划' " \
              "AND fund_id NOT IN (SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 604)"

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60401,
            FundTypeMappingImport.type_name.name: "信托",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream9_60408(cls):
        sql = "SELECT fund_id, fund_name FROM base.fund_info " \
              "WHERE (fund_full_name LIKE '%%有限合伙%%' " \
              "OR fund_full_name LIKE '%%普通合伙%%') " \
              "AND fund_id NOT IN (SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 604)"

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60408,
            FundTypeMappingImport.type_name.name: "有限合伙（合伙制私募基金）",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream10_60401(cls):
        tc = (20401, 30403, 40401,)
        tmp_sql = cls.SQL_BASE2.format(tc=sf.SQL.values4sql(tc), n=len(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60401,
            FundTypeMappingImport.type_name.name: "信托",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream11_60402(cls):
        tc = (20407, 30401, 40403,)
        tmp_sql = cls.SQL_BASE2.format(tc=sf.SQL.values4sql(tc), n=len(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60402,
            FundTypeMappingImport.type_name.name: "自主发行",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream12_60403(cls):
        tmp_sql = "SELECT fi.fund_id, fi.fund_name " \
                  "FROM fund_type_source fts " \
                  "JOIN fund_info fi ON fts.fund_id = fi.fund_id WHERE fi.fund_id NOT IN (" \
                  "SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 604) " \
                  "AND fi.fund_id IN (" \
                  "SELECT DISTINCT fund_id FROM fund_type_source WHERE " \
                  "type_code IN (30407, 30408)) " \
                  "AND fi.fund_id IN (" \
                  "SELECT DISTINCT fund_id FROM fund_type_source WHERE " \
                  "type_code IN (20403, 40404) GROUP BY fund_id HAVING COUNT(type_code) = 2)"

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60403,
            FundTypeMappingImport.type_name.name: "公募专户及子公司资管计划",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream13_60404(cls):
        tc = (20404, 30404, 40402,)
        tmp_sql = cls.SQL_BASE2.format(tc=sf.SQL.values4sql(tc), n=len(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60404,
            FundTypeMappingImport.type_name.name: "券商资管",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream14_60405(cls):
        tc = (20405, 30406, 40405,)
        tmp_sql = cls.SQL_BASE2.format(tc=sf.SQL.values4sql(tc), n=len(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60405,
            FundTypeMappingImport.type_name.name: "期货资管",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream15_60406(cls):
        tc = (20406, 30412,)
        tmp_sql = cls.SQL_BASE2.format(tc=sf.SQL.values4sql(tc), n=len(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60406,
            FundTypeMappingImport.type_name.name: "保险公司及子公司资管计划",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream16_60407(cls):
        tc = (40409,)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60407,
            FundTypeMappingImport.type_name.name: "海外基金",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream17_60408(cls):
        tc = (30402, 40408,)
        tmp_sql = cls.SQL_BASE2.format(tc=sf.SQL.values4sql(tc), n=len(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60408,
            FundTypeMappingImport.type_name.name: "有限合伙",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream18_60409(cls):
        tmp_sql = "SELECT fi.fund_id, fi.fund_name " \
                  "FROM fund_type_source fts " \
                  "JOIN fund_info fi ON fts.fund_id = fi.fund_id WHERE fi.fund_id NOT IN (" \
                  "SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 604) " \
                  "AND fi.fund_id IN (" \
                  "SELECT DISTINCT fund_id FROM fund_type_source WHERE " \
                  "type_code IN (30410, 30411)) " \
                  "AND fi.fund_id IN (" \
                  "SELECT DISTINCT fund_id FROM fund_type_source WHERE " \
                  "type_code = 40407)"

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60409,
            FundTypeMappingImport.type_name.name: "单账户",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream19_60410(cls):
        tc = (30405, 40499,)
        tmp_sql = cls.SQL_BASE2.format(tc=sf.SQL.values4sql(tc), n=len(tc))

        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 604,
            FundTypeMappingImport.typestandard_name.name: "按发行主体分类",
            FundTypeMappingImport.type_code.name: 60410,
            FundTypeMappingImport.type_name.name: "其他",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)

        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def conflu(cls):
        streams = [cls.stream1_60401(), cls.stream2_60401(), cls.stream3_60402(), cls.stream4_60403(),
                   cls.stream5_60404(), cls.stream6_60405(), cls.stream7_60406(), cls.stream8_60401(),
                   cls.stream9_60408(), cls.stream10_60401(), cls.stream11_60402(), cls.stream12_60403(),
                   cls.stream13_60404(), cls.stream14_60405(), cls.stream15_60406(), cls.stream16_60407(),
                   cls.stream17_60408(), cls.stream18_60409(), cls.stream19_60410()]
        on = [FundTypeMappingImport.fund_id.name, FundTypeMappingImport.typestandard_code.name]
        conflus = [base.Confluence(*streams[i:i+4]) for i in range(0, len(streams), 4)]

        c = base.Confluence(*conflus, on=on)
        return c


class Streams605:
    SQL_BASE1 = "SELECT fi.fund_id, fi.fund_name FROM fund_type_source fts " \
                "JOIN fund_info fi ON fts.fund_id = fi.fund_id " \
                "WHERE fi.fund_id NOT IN (" \
                "SELECT DISTINCT fund_id FROM base.fund_type_mapping_import WHERE typestandard_code = 605) " \
                "AND fts.type_code IN {tc}"

    FIXED_FIELDS = {
        "fund_id": FundTypeMappingImport.fund_id.name,
        "fund_name": FundTypeMappingImport.fund_name.name,
        FundTypeMappingImport.typestandard_code.name: None,
        FundTypeMappingImport.typestandard_name.name: None,
        FundTypeMappingImport.type_code.name: None,
        FundTypeMappingImport.type_name.name: None,
        FundTypeMappingImport.classified_by.name: None
    }

    @classmethod
    def stream1_60501(cls):
        tc = (10407,)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 605,
            FundTypeMappingImport.typestandard_name.name: "按基金类型分类",
            FundTypeMappingImport.type_code.name: 60501,
            FundTypeMappingImport.type_name.name: "证券投资基金",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream2_60502(cls):
        tc = (10401,)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 605,
            FundTypeMappingImport.typestandard_name.name: "按基金类型分类",
            FundTypeMappingImport.type_code.name: 60502,
            FundTypeMappingImport.type_name.name: "股权投资基金",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream3_60503(cls):
        tc = (10404,)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 605,
            FundTypeMappingImport.typestandard_name.name: "按基金类型分类",
            FundTypeMappingImport.type_code.name: 60503,
            FundTypeMappingImport.type_name.name: "创业投资基金",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def stream4_60504(cls):
        tc = (10403,)
        tmp_sql = cls.SQL_BASE1.format(tc=sf.SQL.values4sql(tc))
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            FundTypeMappingImport.typestandard_code.name: 605,
            FundTypeMappingImport.typestandard_name.name: "按基金类型分类",
            FundTypeMappingImport.type_code.name: 60504,
            FundTypeMappingImport.type_name.name: "其他投资基金",
            FundTypeMappingImport.classified_by.name: CLASSIFIED_BY
        })

        sk = transform.MapSelectKeys(cls.FIXED_FIELDS)
        s = base.Stream(inp, [vm, sk])
        return s

    @classmethod
    def conflu(cls):
        streams = [
            cls.stream1_60501(), cls.stream2_60502(), cls.stream3_60503(), cls.stream4_60504()
        ]
        on = [FundTypeMappingImport.fund_id.name, FundTypeMappingImport.typestandard_code.name]
        c = base.Confluence(*streams, on=on)
        return c


def main():
    for c in [Streams602.conflu(), Streams603.conflu(), Streams604.conflu(), Streams605.conflu()]:
        io.to_sql("base.fund_type_mapping_import", ENGINE, c.dataframe)


if __name__ == "__main__":
    main()
