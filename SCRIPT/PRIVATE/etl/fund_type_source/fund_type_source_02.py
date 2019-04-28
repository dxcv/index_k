from utils.database import config as cfg, io
from utils.database.models.base_private import FundTypeSource
from utils.etlkit.core import transform, common, base
from utils.etlkit.reader.mysqlreader import MysqlInput
from collections import OrderedDict
from utils.decofactory.common import inscache

ENGINE = cfg.load_engine()["2Gb"]

"""
01-按投资策略分类；
02-按结构类型分类；
03-按投资标的分类；
04-按发行主体分类；
"""


class Streams:
    # 投资策略
    TS_MAPPER_201 = {
        ("股票多头", "股票多头"): ("20101", "2010101", "股票多头", "股票多头"),
        ("股票多头", "并购重组"): ("20101", "2010103", "股票多头", "并购重组"),
        ("股票多头", "大宗交易"): ("20101", "2010104", "股票多头", "大宗交易"),
        ("股票多头", "其他事件驱动"): ("20101", "2010105", "股票多头", "其他事件驱动"),
        ("股票多头", "股票策略"): ("20101", "2010106", "股票多头", "股票策略"),
        ("股票多头", "事件驱动"): ("20101", "2010107", "股票多头", "事件驱动"),
        ("股票多头", "其他策略"): ("20101", "2010199", "股票多头", "其他策略"),

        ("股票多空", "股票多空"): ("20102", "2010201", "股票多空", "股票多空"),

        ("股票市场中性", "股票市场中性"): ("20103", "2010301", "股票市场中性", "股票市场中性"),

        ("债券基金", "债券基金"): ("20104", "2010401", "债券基金", "债券基金"),

        ("多策略", "多策略"): ("20105", "2010501", "多策略", "多策略"),

        ("管理期货", "趋势"): ("20106", "2010601", "管理期货", "趋势"),
        ("管理期货", "管理期货"): ("20106", "2010602", "管理期货", "管理期货"),
        ("管理期货", "复合"): ("20106", "2010603", "管理期货", "复合"),
        ("管理期货", "套利"): ("20106", "2010604", "管理期货", "套利"),

        ("套利策略", "套利策略"): ("20107", "2010701", "套利策略", "套利策略"),
        ("套利策略", "ETF套利"): ("20107", "2010702", "套利策略", "ETF套利"),
        ("套利策略", "其他套利策略"): ("20107", "2010703", "套利策略", "其他套利策略"),
        ("套利策略", "股指期货套利"): ("20107", "2010704", "套利策略", "股指期货套利"),
        ("套利策略", "复合套利策略"): ("20107", "2010705", "套利策略", "复合套利策略"),

        ("组合基金", "组合基金"): ("20108", "2010801", "组合基金", "组合基金"),

        ("定向增发", "定向增发"): ("20109", "2010901", "定向增发", "定向增发"),

        ("新三板", "股票多头"): ("20110", "2011001", "新三板", "股票多头"),
        ("新三板", "多策略"): ("20110", "2011003", "新三板", "多策略"),
        ("新三板", "股票多空"): ("20110", "2011004", "新三板", "股票多空"),
        ("新三板", "定向增发"): ("20110", "2011005", "新三板", "定向增发"),
        ("新三板", "宏观策略"): ("20110", "2011006", "新三板", "宏观策略"),
        ("新三板", "套利策略"): ("20110", "2011007", "新三板", "套利策略"),
        ("新三板", "组合基金"): ("20110", "2011008", "新三板", "组合基金"),
        ("新三板", "其他事件驱动"): ("20110", "2011009", "新三板", "其他事件驱动"),
        ("新三板", "趋势"): ("20110", "2011010", "新三板", "趋势"),
        ("新三板", "并购重组"): ("20110", "2011011", "新三板", "并购重组"),
        ("新三板", "债券基金"): ("20110", "2011012", "新三板", "债券基金"),
        ("新三板", "其他策略"): ("20110", "2011099", "新三板", "其他策略"),

        ("宏观策略", "宏观策略"): ("20111", "2011101", "宏观策略", "宏观策略"),
    }

    # 发行主体
    TS_MAPPER_204 = {
        "信托": ("20401", "信托"),
        "私募公司": ("20402", "私募公司"),
        "公募专户及子公司": ("20403", "公募专户及子公司"),
        "券商集合理财": ("20404", "券商集合理财"),
        "期货资管": ("20405", "期货资管"),
        "保险公司及其子公司的资产管理计划": ("20406", "保险公司及其子公司的资产管理计划"),
        "私募证券投资基金": ("20407", "私募证券投资基金"),
        "私募股权投资基金": ("20408", "私募股权投资基金"),
        "私募创业投资基金": ("20409", "私募创业投资基金"),
        "私募其他投资基金": ("20410", "私募其他投资基金"),
    }

    # 投资策略
    TS_MAPPER_301 = {
        "股票型": ("30101", "股票型"),
        "管理期货": ("30102", "管理期货"),
        "市场中性": ("30103", "市场中性"),
        "套利型": ("30104", "套利型"),
        "定向增发": ("30105", "定向增发"),
        "多空仓型": ("30106", "多空仓型"),
        "多策略": ("30107", "多策略"),
        "货币型": ("30108", "货币型"),
        "债券型": ("30109", "债券型"),
        "宏观策略": ("30110", "宏观策略"),
        "组合型": ("30111", "组合型"),
        "其他": ("30199", "其他"),
    }

    # 结构化类型
    TS_MAPPER_302 = {
        "结构化": ("30201", "结构化"),
        "非结构化": ("30202", "非结构化"),
    }

    # 发行主体
    TS_MAPPER_304 = {
        "自主发行": ("30401", "自主发行"),
        "有限合伙": ("30402", "有限合伙"),
        "信托": ("30403", "信托"),
        "券商资管": ("30404", "券商资管"),
        "期货资管": ("30405", "期货资管"),
        "公募专户": ("30406", "公募专户"),
        "公募子公司专户": ("30407", "公募子公司专户"),
        "复合型": ("30408", "复合型"),
        "单账户证券": ("30409", "单账户证券"),
        "单账户期货": ("30410", "单账户期货"),
        "保险": ("30411", "保险"),
        "其他": ("30499", "其他"),
    }

    # 投资策略
    TS_MAPPER_401 = {
        ("股票策略", "股票多头"): ("40101", "4010101", "股票策略", "股票多头"),
        ("股票策略", "股票复合策略"): ("40101", "4010102", "股票策略", "股票复合策略"),
        ("股票策略", "股票行业策略"): ("40101", "4010103", "股票策略", "股票行业策略"),
        ("股票策略", "股票量化"): ("40101", "4010104", "股票策略", "股票量化"),
        ("股票策略", "股票多/空"): ("40101", "4010105", "股票策略", "股票多/空"),
        ("股票策略", "其他策略"): ("40101", "4010106", "股票策略", "其他策略"),
        ("股票策略", ""): ("40101", None, "股票策略", None),

        ("宏观策略", "宏观策略"): ("40102", "4010201", "宏观策略", "宏观策略"),
        ("宏观策略", ""): ("40102", None, "宏观策略", None),

        ("管理期货", "趋势交易"): ("40103", "4010301", "管理期货", "趋势交易"),
        ("管理期货", "管理期货复合策略"): ("40103", "4010302", "管理期货", "管理期货复合策略"),
        ("管理期货", "系统化趋势"): ("40103", "4010303", "管理期货", "系统化趋势"),
        ("管理期货", "日内交易"): ("40103", "4010304", "管理期货", "日内交易"),
        ("管理期货", "系统化套利"): ("40103", "4010305", "管理期货", "系统化套利"),
        ("管理期货", "高频交易"): ("40103", "4010306", "管理期货", "高频交易"),
        ("管理期货", "波段交易"): ("40103", "4010307", "管理期货", "波段交易"),
        ("管理期货", "套利"): ("40103", "4010308", "管理期货", "套利"),

        ("事件驱动", "事件驱动复合策略"): ("40104", "4010401", "事件驱动", "事件驱动复合策略"),
        ("事件驱动", "定向增发-新三板"): ("40104", "4010402", "事件驱动", "定向增发-新三板"),
        ("事件驱动", "定向增发-主板"): ("40104", "4010403", "事件驱动", "定向增发-主板"),
        ("事件驱动", "并购重组"): ("40104", "4010404", "事件驱动", "并购重组"),
        ("事件驱动", "定向增发"): ("40104", "4010405", "事件驱动", "定向增发"),
        ("事件驱动", "大宗交易"): ("40104", "4010406", "事件驱动", "大宗交易"),
        ("事件驱动", "类固定收益"): ("40104", "4010407", "事件驱动", "类固定收益"),
        ("事件驱动", "特殊情况"): ("40104", "4010408", "事件驱动", "特殊情况"),

        ("相对价值策略", "股票市场中性"): ("40105", "4010501", "相对价值策略", "股票市场中性"),
        ("相对价值策略", "套利"): ("40105", "4010502", "相对价值策略", "套利"),
        ("相对价值策略", "相对价值复合策略"): ("40105", "4010503", "相对价值策略", "相对价值复合策略"),
        ("相对价值策略", ""): ("40105", None, "相对价值策略", None),

        ("债券策略", "债券-强债策略"): ("40106", "4010601", "债券策略", "债券-强债策略"),
        ("债券策略", "固定收益复合策略"): ("40106", "4010602", "债券策略", "固定收益复合策略"),
        ("债券策略", "债券-纯债策略"): ("40106", "4010603", "债券策略", "债券-纯债策略"),
        ("债券策略", "债券策略"): ("40106", "4010604", "债券策略", "债券策略"),
        ("债券策略", "类固定收益"): ("40106", "4010605", "债券策略", "类固定收益"),
        ("债券策略", "定向增发-主板"): ("40106", "4010607", "债券策略", "定向增发-主板"),
        ("债券策略", "套利"): ("40106", "4010608", "债券策略", "套利"),
        ("债券策略", "其他策略"): ("40106", "4010699", "债券策略", "其他策略"),

        ("组合基金", "FOF"): ("40107", "4010701", "组合基金", "FOF"),
        ("组合基金", "MOM"): ("40107", "4010702", "组合基金", "MOM"),

        ("复合策略", "复合策略"): ("40108", "4010801", "复合策略", "复合策略"),
        ("复合策略", "股票市场中性"): ("40108", "4010804", "复合策略", "股票市场中性"),
        ("复合策略", "套利"): ("40108", "4010805", "复合策略", "套利"),
        ("复合策略", "股票多头"): ("40108", "4010806", "复合策略", "股票多头"),
        ("复合策略", "固定收益复合策略"): ("40108", "4010807", "复合策略", "固定收益复合策略"),
        ("复合策略", "其他策略"): ("40108", "4010899", "复合策略", "其他策略"),
        ("复合策略", ""): ("40108", None, "复合策略", None),

        ("", "其他策略"): ("40198", "4019899", "主策略未定义", "其他策略"),

        ("其它策略", "股票多头"): ("40199", "4019903", "其它策略", "股票多头"),
        ("其它策略", "其他策略"): ("40199", "4019999", "其它策略", "其他策略"),
        ("其它策略", ""): ("40199", None, "其它策略", None),
    }
    
    # 发行主体
    TS_MAPPER_404 = {
        "信托": ("40401", "信托"),
        "券商资管": ("40402", "券商资管"),
        "自主发行": ("40403", "自主发行"),
        "公募专户": ("40404", "公募专户"),
        "期货专户": ("40405", "期货专户"),
        "单账户": ("40407", "单账户"),
        "有限合伙": ("40408", "有限合伙"),
        "海外基金": ("40409", "海外基金"),
        "其他": ("40499", "其他"),
    }

    # 投资策略
    TS_MAPPER_501 = {
        ("股票型", "股票多头"): ("50101", "5010101", "股票型", "股票多头"),
        ("股票型", "股票多空"): ("50101", "5010102", "股票型", "股票多空"),
        ("股票型", "行业基金"): ("50101", "5010103", "股票型", "行业基金"),
        ("股票型", "大宗交易"): ("50101", "5010104", "股票型", "大宗交易"),
        ("股票型", "复合策略"): ("50101", "5010105", "股票型", "复合策略"),
        ("股票型", "定向增发"): ("50101", "5010106", "股票型", "定向增发"),

        ("债券型", "股票多头"): ("50102", "5010201", "债券型", "股票多头"),
        ("债券型", "复合策略"): ("50102", "5010202", "债券型", "复合策略"),
        ("债券型", "大宗交易"): ("50102", "5010203", "债券型", "大宗交易"),
        ("债券型", "程序化期货"): ("50102", "5010204", "债券型", "程序化期货"),
        ("债券型", "主观期货"): ("50102", "5010205", "债券型", "主观期货"),
        ("债券型", ""): ("50102", None, "债券型", None),

        ("管理期货", "主观期货"): ("50103", "5010301", "管理期货", "主观期货"),
        ("管理期货", "程序化期货"): ("50103", "5010302", "管理期货", "程序化期货"),
        ("管理期货", "复合策略"): ("50103", "5010303", "管理期货", "复合策略"),
        ("管理期货", ""): ("50103", None, "管理期货", None),

        ("相对价值", "阿尔法策略"): ("50104", "5010401", "相对价值", "阿尔法策略"),
        ("相对价值", "套利策略"): ("50104", "5010402", "相对价值", "套利策略"),
        ("相对价值", "量化复合策略"): ("50104", "5010403", "相对价值", "量化复合策略"),
        ("相对价值", None): ("50104", None, "相对价值", None),

        ("宏观对冲", "复合策略"): ("50105", "5010501", "宏观对冲", "复合策略"),
        ("宏观对冲", "股票多头"): ("50105", "5010502", "宏观对冲", "股票多头"),
        ("宏观对冲", "主观期货"): ("50105", "5010503", "宏观对冲", "主观期货"),
        ("宏观对冲", None): ("50105", None, "宏观对冲", None),

        ("组合基金", "股票多头"): ("50106", "5010601", "组合基金", "股票多头"),
        ("组合基金", "套利策略"): ("50106", "5010602", "组合基金", "套利策略"),
        ("组合基金", "复合策略"): ("50106", "5010603", "组合基金", "复合策略"),
        ("组合基金", "阿尔法策略"): ("50106", "5010604", "组合基金", "阿尔法策略"),
        ("组合基金", None): ("50106", None, "组合基金", None),

        ("事件驱动", "定向增发"): ("50107", "5010701", "事件驱动", "定向增发"),
        ("事件驱动", "参与新股"): ("50107", "5010702", "事件驱动", "参与新股"),
        ("事件驱动", "大宗交易"): ("50107", "5010703", "事件驱动", "大宗交易"),
        ("事件驱动", "复合策略"): ("50107", "5010704", "事件驱动", "复合策略"),
        ("事件驱动", None): ("50107", None, "事件驱动", None),

        ("海外基金", "股票多头"): ("50108", "5010801", "海外基金", "股票多头"),
        ("海外基金", "复合策略"): ("50108", "5010802", "海外基金", "复合策略"),
        ("海外基金", None): ("50108", None, "海外基金", None),

        ("复合策略", "复合策略"): ("50109", "5010901", "复合策略", "复合策略"),
        ("复合策略", "量化复合策略"): ("50109", "5010902", "复合策略", "量化复合策略"),
        ("复合策略", "股票多头"): ("50109", "5010903", "复合策略", "股票多头"),
        ("复合策略", None): ("50109", None, "复合策略", None),

        ("", "股票多头"): ("50198", "5019801", "主策略未定义", "股票多头"),

        ("其他策略", "股票多头"): ("50199", "5019901", "其他策略", "股票多头"),
        ("其他策略", "复合策略"): ("50199", "5019902", "其他策略", "复合策略"),
        ("其他策略", "定向增发"): ("50199", "5019903", "其他策略", "定向增发"),
        ("其他策略", "阿尔法策略"): ("50199", "5019904", "其他策略", "阿尔法策略"),
        ("其他策略", "大宗交易"): ("50199", "5019905", "其他策略", "大宗交易"),
        ("其他策略", "主观期货"): ("50199", "5019906", "其他策略", "主观期货"),

    }

    # 投资策略
    SQL_X01 = "SELECT im.matched_id, fi.fund_name, dfi.type_name, dfi.stype_name " \
              "FROM {tb} dfi " \
              "JOIN (SELECT fund_id, MAX(version) latest_ver FROM {tb} GROUP BY fund_id) t " \
              "ON dfi.fund_id = t.fund_id AND dfi.version = t.latest_ver " \
              "JOIN (SELECT matched_id, source_id FROM base.id_match WHERE source = '{sid}' AND id_type = 1 AND is_used = 1) im " \
              "ON dfi.fund_id = im.source_id " \
              "LEFT JOIN fund_info fi ON im.matched_id = fi.fund_id"

    # 结构化类型
    SQL_X02 = "SELECT im.matched_id, fi.fund_name, dfi.fund_type_structure " \
              "FROM {tb} dfi " \
              "JOIN (SELECT fund_id, MAX(version) latest_ver FROM {tb} GROUP BY fund_id) t " \
              "ON dfi.fund_id = t.fund_id AND dfi.version = t.latest_ver " \
              "JOIN (SELECT matched_id, source_id FROM base.id_match WHERE source = '{sid}' AND id_type = 1 AND is_used = 1) im " \
              "ON dfi.fund_id = im.source_id " \
              "LEFT JOIN fund_info fi ON im.matched_id = fi.fund_id"

    # 发行主体
    SQL_X04 = "SELECT im.matched_id, fi.fund_name, dfi.fund_type_issuance " \
              "FROM {tb} dfi " \
              "JOIN (SELECT fund_id, MAX(version) latest_ver FROM {tb} GROUP BY fund_id) t " \
              "ON dfi.fund_id = t.fund_id AND dfi.version = t.latest_ver " \
              "JOIN (SELECT matched_id, source_id FROM base.id_match WHERE source = '{sid}' AND id_type = 1 AND is_used = 1) im " \
              "ON dfi.fund_id = im.source_id " \
              "LEFT JOIN fund_info fi ON im.matched_id = fi.fund_id"

    @classmethod
    def stream_020004_ts201(cls):
        source_id = "020004"
        tb = "crawl_private.d_fund_info"
        tmpsql = cls.SQL_X01.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(OrderedDict([
            [FundTypeSource.source_id.name, source_id],
            [FundTypeSource.typestandard_code.name, "101"],
            [FundTypeSource.typestandard_name.name, "按投资策略分类"],
            ["__cached",  (lambda tn, stn: cls.TS_MAPPER_201.get((tn, stn), [None] * 4), "type_name", "stype_name")],
            [FundTypeSource.type_code.name, (lambda x: x[0], "__cached")],
            [FundTypeSource.type_name.name, (lambda x: x[2], "__cached")],
            [FundTypeSource.stype_code.name, (lambda x: x[1], "__cached")],
            [FundTypeSource.stype_name.name, (lambda x: x[3], "__cached")],
        ]))

        sk = transform.MapSelectKeys({
            "matched_id": FundTypeSource.fund_id.name,
            "fund_name": FundTypeSource.fund_name.name,
            FundTypeSource.source_id.name: None,
            FundTypeSource.typestandard_code.name: None,
            FundTypeSource.typestandard_name.name: None,
            FundTypeSource.type_code.name: None,
            FundTypeSource.type_name.name: None,
            FundTypeSource.stype_code.name: None,
            FundTypeSource.stype_name.name: None,
        })
        dn = transform.Dropna(subset=[FundTypeSource.type_code.name, FundTypeSource.stype_code.name], how="all")

        s = base.Stream(inp, [vm, sk, dn])

        return s

    @classmethod
    def stream_020004_ts204(cls):
        source_id = "020004"
        tb = "crawl_private.d_fund_info"
        tmpsql = cls.SQL_X04.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(OrderedDict([
            [FundTypeSource.source_id.name, source_id],
            [FundTypeSource.typestandard_code.name, "104"],
            [FundTypeSource.typestandard_name.name, "按发行主体分类"],
            ["__cached",  (lambda tn: cls.TS_MAPPER_204.get(tn, [None] * 2), "fund_type_issuance")],
            [FundTypeSource.type_code.name, (lambda x: x[0], "__cached")],
            [FundTypeSource.type_name.name, (lambda x: x[1], "__cached")],
        ]))

        sk = transform.MapSelectKeys({
            "matched_id": FundTypeSource.fund_id.name,
            "fund_name": FundTypeSource.fund_name.name,
            FundTypeSource.source_id.name: None,
            FundTypeSource.typestandard_code.name: None,
            FundTypeSource.typestandard_name.name: None,
            FundTypeSource.type_code.name: None,
            FundTypeSource.type_name.name: None,
        })
        dn = transform.Dropna(subset=[FundTypeSource.type_code.name], how="all")

        s = base.Stream(inp, [vm, sk, dn])

        return s

    @classmethod
    def stream_020001_ts301(cls):
        source_id = "020001"
        tb = "crawl_private.d_fund_info"
        tmpsql = cls.SQL_X01.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(OrderedDict([
            [FundTypeSource.source_id.name, source_id],
            [FundTypeSource.typestandard_code.name, "101"],
            [FundTypeSource.typestandard_name.name, "按投资策略分类"],
            ["__cached",  (lambda x: cls.TS_MAPPER_301.get(x, [None, None]), "type_name")],
            [FundTypeSource.type_code.name, (lambda x: x[0], "__cached")],
            [FundTypeSource.type_name.name, (lambda x: x[1], "__cached")],
        ]))

        sk = transform.MapSelectKeys({
            "matched_id": FundTypeSource.fund_id.name,
            "fund_name": FundTypeSource.fund_name.name,
            FundTypeSource.source_id.name: None,
            FundTypeSource.typestandard_code.name: None,
            FundTypeSource.typestandard_name.name: None,
            FundTypeSource.type_code.name: None,
            FundTypeSource.type_name.name: None,
        })
        dn = transform.Dropna(subset=[FundTypeSource.type_code.name])

        s = base.Stream(inp, [vm, sk, dn])

        return s

    @classmethod
    def stream_020001_ts302(cls):
        source_id = "020001"
        tb = "crawl_private.d_fund_info"
        tmpsql = cls.SQL_X02.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(OrderedDict([
            [FundTypeSource.source_id.name, source_id],
            [FundTypeSource.typestandard_code.name, "102"],
            [FundTypeSource.typestandard_name.name, "按结构类型分类"],
            ["__cached",  (lambda x: cls.TS_MAPPER_302.get(x, [None, None]), "fund_type_structure")],
            [FundTypeSource.type_code.name, (lambda x: x[0], "__cached")],
            [FundTypeSource.type_name.name, (lambda x: x[1], "__cached")],
        ]))

        sk = transform.MapSelectKeys({
            "matched_id": FundTypeSource.fund_id.name,
            "fund_name": FundTypeSource.fund_name.name,
            FundTypeSource.source_id.name: None,
            FundTypeSource.typestandard_code.name: None,
            FundTypeSource.typestandard_name.name: None,
            FundTypeSource.type_code.name: None,
            FundTypeSource.type_name.name: None,
        })
        dn = transform.Dropna(subset=[FundTypeSource.type_code.name])

        s = base.Stream(inp, [vm, sk, dn])

        return s

    @classmethod
    def stream_020001_ts304(cls):
        source_id = "020001"
        tb = "crawl_private.d_fund_info"
        tmpsql = cls.SQL_X04.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(OrderedDict([
            [FundTypeSource.source_id.name, source_id],
            [FundTypeSource.typestandard_code.name, "104"],
            [FundTypeSource.typestandard_name.name, "按发行主体分类"],
            ["__cached",  (lambda x: cls.TS_MAPPER_304.get(x, [None, None]), "fund_type_issuance")],
            [FundTypeSource.type_code.name, (lambda x: x[0], "__cached")],
            [FundTypeSource.type_name.name, (lambda x: x[1], "__cached")],
        ]))

        sk = transform.MapSelectKeys({
            "matched_id": FundTypeSource.fund_id.name,
            "fund_name": FundTypeSource.fund_name.name,
            FundTypeSource.source_id.name: None,
            FundTypeSource.typestandard_code.name: None,
            FundTypeSource.typestandard_name.name: None,
            FundTypeSource.type_code.name: None,
            FundTypeSource.type_name.name: None,
        })
        dn = transform.Dropna(subset=[FundTypeSource.type_code.name])

        s = base.Stream(inp, [vm, sk, dn])

        return s

    @classmethod
    def stream_020002_ts401(cls):
        source_id = "020002"
        tb = "crawl_private.d_fund_info"
        tmpsql = cls.SQL_X01.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm0 = transform.ValueMap(
            {
                "type_name": lambda x: "" if x.replace("-", "") == "" else x,
                "stype_name": lambda x: "" if x.replace("-", "") == "" else x,
            }
        )

        vm = transform.ValueMap(OrderedDict([
            [FundTypeSource.source_id.name, source_id],
            [FundTypeSource.typestandard_code.name, "101"],
            [FundTypeSource.typestandard_name.name, "按投资策略分类"],
            ["__cached",  (lambda tn, stn: cls.TS_MAPPER_401.get((tn, stn), [None] * 4), "type_name", "stype_name")],
            [FundTypeSource.type_code.name, (lambda x: x[0], "__cached")],
            [FundTypeSource.type_name.name, (lambda x: x[2], "__cached")],
            [FundTypeSource.stype_code.name, (lambda x: x[1], "__cached")],
            [FundTypeSource.stype_name.name, (lambda x: x[3], "__cached")],
        ]))

        sk = transform.MapSelectKeys({
            "matched_id": FundTypeSource.fund_id.name,
            "fund_name": FundTypeSource.fund_name.name,
            FundTypeSource.source_id.name: None,
            FundTypeSource.typestandard_code.name: None,
            FundTypeSource.typestandard_name.name: None,
            FundTypeSource.type_code.name: None,
            FundTypeSource.type_name.name: None,
            FundTypeSource.stype_code.name: None,
            FundTypeSource.stype_name.name: None,
        })
        dn = transform.Dropna(subset=[FundTypeSource.type_code.name, FundTypeSource.stype_code.name], how="all")

        s = base.Stream(inp, [vm0, vm, sk, dn])

        return s

    @classmethod
    def stream_020002_ts404(cls):
        source_id = "020002"
        tb = "crawl_private.d_fund_info"
        tmpsql = cls.SQL_X04.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(OrderedDict([
            [FundTypeSource.source_id.name, source_id],
            [FundTypeSource.typestandard_code.name, "104"],
            [FundTypeSource.typestandard_name.name, "按发行主体分类"],
            ["fund_type_issuance", lambda x: "" if x.replace("-", "") == "" else x],
            ["__cached",  (lambda tn: cls.TS_MAPPER_404.get(tn, [None] * 2), "fund_type_issuance")],
            [FundTypeSource.type_code.name, (lambda x: x[0], "__cached")],
            [FundTypeSource.type_name.name, (lambda x: x[1], "__cached")],
        ]))

        sk = transform.MapSelectKeys({
            "matched_id": FundTypeSource.fund_id.name,
            "fund_name": FundTypeSource.fund_name.name,
            FundTypeSource.source_id.name: None,
            FundTypeSource.typestandard_code.name: None,
            FundTypeSource.typestandard_name.name: None,
            FundTypeSource.type_code.name: None,
            FundTypeSource.type_name.name: None,
        })
        dn = transform.Dropna(subset=[FundTypeSource.type_code.name], how="all")

        s = base.Stream(inp, [vm, sk, dn])

        return s

    @classmethod
    def stream_020003_ts501(cls):
        source_id = "020003"
        tb = "crawl_private.d_fund_info"
        tmpsql = cls.SQL_X01.format(tb=tb, sid=source_id)

        inp = MysqlInput(ENGINE, tmpsql)

        vm = transform.ValueMap(OrderedDict([
            [FundTypeSource.source_id.name, source_id],
            [FundTypeSource.typestandard_code.name, "101"],
            [FundTypeSource.typestandard_name.name, "按投资策略分类"],
            ["__cached",  (lambda tn, stn: cls.TS_MAPPER_501.get((tn, stn), [None] * 4), "type_name", "stype_name")],
            [FundTypeSource.type_code.name, (lambda x: x[0], "__cached")],
            [FundTypeSource.type_name.name, (lambda x: x[2], "__cached")],
            [FundTypeSource.stype_code.name, (lambda x: x[1], "__cached")],
            [FundTypeSource.stype_name.name, (lambda x: x[3], "__cached")],
        ]))

        sk = transform.MapSelectKeys({
            "matched_id": FundTypeSource.fund_id.name,
            "fund_name": FundTypeSource.fund_name.name,
            FundTypeSource.source_id.name: None,
            FundTypeSource.typestandard_code.name: None,
            FundTypeSource.typestandard_name.name: None,
            FundTypeSource.type_code.name: None,
            FundTypeSource.type_name.name: None,
            FundTypeSource.stype_code.name: None,
            FundTypeSource.stype_name.name: None,
        })
        dn = transform.Dropna(subset=[FundTypeSource.type_code.name], how="all")

        s = base.Stream(inp, [vm, sk, dn])

        return s

    @classmethod
    def conflu_ts20X(cls):
        s21, s24 = cls.stream_020004_ts201(), cls.stream_020004_ts204()

        c = base.Confluence(s21, s24)
        return c

    @classmethod
    def conflu_ts30X(cls):
        s31, s32, s34 = cls.stream_020001_ts301(), cls.stream_020001_ts302(), cls.stream_020001_ts304()

        c = base.Confluence(s31, s32, s34)
        return c

    @classmethod
    def conflu_ts40X(cls):
        s41, s44 = cls.stream_020002_ts401(), cls.stream_020002_ts404()

        c = base.Confluence(s41, s44)
        return c

    @classmethod
    def conflu_ts501(cls):
        s51 = cls.stream_020003_ts501()

        c = base.Confluence(s51)
        return c


def test():
    c = Streams.conflu_ts30X()
    c = Streams.conflu_ts40X()
    c.dataframe
    io.to_sql("base_test.fund_type_source_test", ENGINE, c.dataframe)

    s = Streams.stream_020002_ts401()  # 75882
    s = Streams.stream_020002_ts404()  # 75882
    s = Streams.stream_020003_ts501()  # 75882

    s.dataframe
    s.flow()
    io.to_sql("base_test.fund_type_source_test", ENGINE, s.dataframe)
