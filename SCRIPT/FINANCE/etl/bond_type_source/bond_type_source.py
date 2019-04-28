from utils.database import config as cfg
from utils.etlkit.core.base import Frame, Stream, Confluence
from utils.etlkit.reader.mysqlreader import MysqlInput, MysqlNativeInput
from utils.etlkit.core import transform
from collections import OrderedDict
from utils.database import io
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker, Session
from utils.database.models.crawl_finance import DBondInfo
from utils.database.models.base_finance import BondTypeSource

import re

engine_r = cfg.load_engine()["etl_finance"]
engine_w = cfg.load_engine()["etl_base_finance"]
dbsession = sessionmaker()


def stream_010001():
    session = dbsession(bind=engine_r)
    stmt = session.query(DBondInfo).filter(
        DBondInfo.source_id == "010001"
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.bond_full_name, DBondInfo.bond_type, DBondInfo.consigner
    )
    inp = MysqlInput(session.bind, stmt)

    def clean_bondtype(bond_type, bond_full_name, consigner):
        from functools import reduce
        bond_type = bond_type or ""
        bond_full_name = bond_full_name or ""
        consigner = consigner or ""
        DIRECT_TYPE_MAP = {
            "记账式国债": ("国债", None),
            "金融债": ("金融债", "政策性银行债"),
            "地方政府债券": ("地方政府债", None),
            "可交换公司债券": ("可交换债", None),
            "企业债券": ("企业债", None),
            "企业资产支持证券": ("资产支持证券", None),
            "信贷资产支持证券": ("资产支持证券", None),
            "中小企业私募债券": ("公司债", "中小企业私募债"),
        }

        if bond_type in DIRECT_TYPE_MAP.keys():
            type_l1, type_l2 = DIRECT_TYPE_MAP[bond_type]
        else:
            # bond_type = "公司债券"
            # bond_full_name, consigner = "招商证券股份有限公司2012年公司债券（3+2）", "招商证券股份有限公司"
            patts_subname = "".join(
                ".*?(%s)?" % x for x in ["|".join(x) for x in (
                    ("可转换",), ("证券", "期货", "保险", "中国国际金融"), ("中国铁路建设",)
                )]
            )
            patts_subcsg = "".join(
                ".*?(%s)?" % x for x in ["|".join(x) for x in (
                    ("银行",), ("证券", "期货", "保险", "中国国际金融"), ("铁道部",)
                )]
            )
            subname = reduce(lambda x, y: [(x[i] or y[i]) for i in range(len(x))],
                             re.findall(patts_subname, bond_full_name))
            subcsg = reduce(lambda x, y: [(x[i] or y[i]) for i in range(len(x))], re.findall(patts_subcsg, consigner))

            if bond_type == "次级债券":
                if subcsg[0]:
                    type_l1, type_l2 = "金融债", None
                else:
                    type_l1, type_l2 = "金融债", "非银金融机构债"
            elif bond_type == "非公开发行公司债券":
                if subname[0]:
                    type_l1, type_l2 = "可转债", None
                elif subname[1] and subcsg[1]:
                    type_l1, type_l2 = "金融债", "非银金融机构债"
                elif subname[2] or subcsg[2]:
                    type_l1, type_l2 = "政府支持机构债", None
                else:
                    type_l1, type_l2 = "公司债", "非公开发行公司债"
            elif bond_type == "公司债券":
                if subname[1] and subcsg[1]:
                    type_l1, type_l2 = "金融债", "非银金融机构债"
                else:
                    type_l1, type_l2 = "公司债", "一般公司债"
            else:
                type_l1, type_l2 = None, None
        return type_l1, type_l2

    vm = transform.ValueMap(
        OrderedDict([
            ("__tmpcol", (lambda bondtype, bondfname, consigner: clean_bondtype(bondtype, bondfname, consigner),
                          DBondInfo.bond_type.name, DBondInfo.bond_full_name.name, DBondInfo.consigner.name)),
            (BondTypeSource.type.name, (lambda x: x[0], "__tmpcol")),
            (BondTypeSource.stype.name, (lambda x: x[1], "__tmpcol")),
        ])
    )

    ac = transform.AddConst(
        {
            BondTypeSource.dimension.name: "债券品种",
            BondTypeSource.source_id.name: "010001",
        }
    )

    dp = transform.Dropna(subset=[BondTypeSource.type.name])

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondTypeSource.bond_id.name,
            BondTypeSource.source_id.name: None,
            BondTypeSource.dimension.name: None,
            BondTypeSource.type.name: None,
            BondTypeSource.stype.name: None,
        }
    )

    s = Stream(inp, [vm, ac, dp, km])
    return s


def stream_020001():
    session = dbsession(bind=engine_r)
    stmt = session.query(DBondInfo).filter(
        DBondInfo.source_id == "020001"
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.bond_name, DBondInfo.bond_full_name, DBondInfo.bond_type, DBondInfo.consigner
    )
    inp = MysqlInput(session.bind, stmt)

    def clean_bondtype(bond_type, bond_name, bond_full_name, consigner):
        from functools import reduce
        bond_type, bond_name, bond_full_name, consigner = [
            (x or "") for x in (bond_type, bond_name, bond_full_name, consigner)
        ]

        # bond_type, bond_name, bond_full_name, consigner = ["深国债", "山西16Z1", "2016年山西省政府一般债券(六期)", "山西省政府"]
        DIRECT_TYPE_MAP = {
            "短期融资券": ("国债", None),
            "金融债": ("金融债", "政策性银行债"),
            "商业银行债": ("金融债", "商业银行债"),
            "资产证券化": ("资产支持证券", None),
        }

        if bond_type in DIRECT_TYPE_MAP.keys():
            type_l1, type_l2 = DIRECT_TYPE_MAP[bond_type]
        else:
            patts_subfname = "".join(
                ".*?(%s)?" % x for x in ["|".join(x) for x in (
                    ("国债",), ("政府",), ("证券", "期货", "保险", "中国国际金融"), ("非公开",), ("企业债",),
                    ("公司债",), ("中国铁路建设",), ("可交换",)
                )]
            )
            patts_subname = "".join(
                ".*?(%s)?" % x for x in ["|".join(x) for x in (
                    ("ppn",), ("scp",), ("mtn",), ("cp",), ("eb",),
                    ("mtn", "smecn", "prn", "gn"), ("npb",)
                )]
            )
            patts_subcsg = "".join(
                ".*?(%s)?" % x for x in ["|".join(x) for x in (
                    ("政府",), ("证券", "期货", "保险", "中国国际金融"), ("铁道部", "中央汇金")
                )]
            )
            subfname = reduce(lambda x, y: [(x[i] or y[i]) for i in range(len(x))], re.findall(patts_subfname, bond_full_name.lower()))
            subcsg = reduce(lambda x, y: [(x[i] or y[i]) for i in range(len(x))], re.findall(patts_subcsg, consigner))
            subname = reduce(lambda x, y: [(x[i] or y[i]) for i in range(len(x))], re.findall(patts_subname, bond_name.lower()))

            if subname[0]:
                return "定向工具", None

            elif subname[1]:
                return "短期融资券", None

            elif subname[5]:
                return "中期票据", None

            elif subname[2]:
                return "中期票据", None

            elif subname[3]:
                return "短期融资券", None

            elif subname[6]:
                return "企业债", None

            elif bond_type == "沪国债":
                if subfname[0]:
                    type_l1, type_l2 = "国债", None
                elif subfname[1] or subcsg[1]:
                    type_l1, type_l2 = "地方政府债", None
                else:
                    type_l1, type_l2 = None, None

            elif bond_type == "沪企债":
                if subfname[2] and subcsg[1]:
                    type_l1, type_l2 = "金融债", "非银金融机构债"
                elif subfname[3]:
                    type_l1, type_l2 = "公司债", "非公开发行公司债"
                elif subfname[4]:
                    type_l1, type_l2 = "企业债", None
                # elif subname[5]:
                #     type_l1, type_l2 = "公司债", None
                elif subfname[6] or subcsg[2]:
                    type_l1, type_l2 = "政府支持机构债", None
                else:
                    type_l1, type_l2 = None, None

            elif bond_type == "沪转债":
                if subfname[7] or subname[4]:
                    type_l1, type_l2 = "可交换债", None
                else:
                    type_l1, type_l2 = "可转债", None

            elif bond_type == "企业债":
                if subfname[6] or subcsg[2]:
                    type_l1, type_l2 = "政府支持机构债", None
                else:
                    type_l1, type_l2 = "企业债", None

            elif bond_type == "深国债":
                if subfname[0]:
                    type_l1, type_l2 = "国债", None
                elif subfname[1] or subcsg[0]:
                    type_l1, type_l2 = "地方政府债", None

            elif bond_type == "深企债":
                if subfname[2] and subcsg[1]:
                    type_l1, type_l2 = "金融债", "非银金融机构债"
                elif subfname[3]:
                    type_l1, type_l2 = "公司债", "非公开发行公司债"
                elif subfname[4]:
                    type_l1, type_l2 = "企业债", None
                # elif subname[5]:
                #     type_l1, type_l2 = "公司债", "一般公司债"
                else:
                    type_l1, type_l2 = None, None

            elif bond_type == "深转债":
                if subfname[7] or subname[4]:
                    type_l1, type_l2 = "可交换债", None
                else:
                    type_l1, type_l2 = "可转债", None

            else:
                type_l1, type_l2 = None, None

        return type_l1, type_l2

    vm = transform.ValueMap(
        OrderedDict([
            ("__tmpcol", (lambda bondtype, bondname, bondfname, consigner: clean_bondtype(bondtype, bondname, bondfname, consigner),
                          DBondInfo.bond_type.name, DBondInfo.bond_name.name, DBondInfo.bond_full_name.name, DBondInfo.consigner.name)),
            (BondTypeSource.type.name, (lambda x: x[0], "__tmpcol")),
            (BondTypeSource.stype.name, (lambda x: x[1], "__tmpcol")),
        ])
    )

    ac = transform.AddConst(
        {
            BondTypeSource.dimension.name: "债券品种",
            BondTypeSource.source_id.name: "020001",
        }
    )

    dp = transform.Dropna(subset=[BondTypeSource.type.name])

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondTypeSource.bond_id.name,
            BondTypeSource.source_id.name: None,
            BondTypeSource.dimension.name: None,
            BondTypeSource.type.name: None,
            BondTypeSource.stype.name: None,
        }
    )

    s = Stream(inp, [vm, ac, dp, km])
    return s


def stream_020002():
    session = dbsession(bind=engine_r)
    stmt = session.query(DBondInfo).filter(
        DBondInfo.source_id == "020002"
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.bond_name, DBondInfo.bond_full_name, DBondInfo.bond_type
    )
    inp = MysqlInput(session.bind, stmt)

    def clean_bondtype(bond_type, bond_name, bond_full_name):
        from functools import reduce
        bond_type, bond_name, bond_full_name = [
            (x or "") for x in (bond_type, bond_name, bond_full_name)
        ]

        DIRECT_TYPE_MAP = {
            "国债": ("国债", None),
            "政策性银行债": ("金融债", "政策性银行债"),
            "可转换债券": ("可转债", None),
            "政府支持机构债": ("政府支持机构债", None),
            "企业债": ("企业债", None),
            "证券公司债": ("金融债", "非银金融机构债"),
            "可交换债券": ("可交换债", None),
            "证券公司次级债": ("金融债", "非银金融机构债"),
            "券商专项资产管理": ("资产支持证券", None),
            "不动产投资信托(REITs)": ("资产支持证券", None),
            "中小企业集合债券": ("企业债", None),
            "项目收益债券": ("企业债", None),
        }

        if bond_type in DIRECT_TYPE_MAP.keys():
            type_l1, type_l2 = DIRECT_TYPE_MAP[bond_type]
        else:
            patts_subfname = "".join(
                ".*?(%s)?" % x for x in ["|".join(x) for x in (
                    ("中小企业私募债",), ("非公开发行",)
                )]
            )

            patts_subname = "".join(
                ".*?(%s)?" % x for x in ["|".join(x) for x in (
                    ("ppn",), ("scp",), ("mtn",), ("cp",), ("mtn", "smecn", "prn", "gn"),
                    ("npb",)
                )]
            )

            subfname = reduce(lambda x, y: [(x[i] or y[i]) for i in range(len(x))], re.findall(patts_subfname, bond_full_name.lower()))
            subname = reduce(lambda x, y: [(x[i] or y[i]) for i in range(len(x))], re.findall(patts_subname, bond_name.lower()))

            if subname[0]:
                return "定向工具", None

            elif subname[1]:
                return "短期融资券", None

            elif subname[4]:
                return "中期票据", None

            elif subname[2]:
                return "中期票据", None

            elif subname[3]:
                return "短期融资券", None

            elif subname[5]:
                return "企业债", None

            elif bond_type == "公司债":
                if subfname[0]:
                    type_l1, type_l2 = "公司债", "中小企业私募债"
                elif subfname[1]:
                    type_l1, type_l2 = "公司债", "非公开发行公司债"
                else:
                    type_l1, type_l2 = "公司债", "一般公司债"
            else:
                type_l1, type_l2 = None, None
        return type_l1, type_l2

    vm = transform.ValueMap(
        OrderedDict([
            ("__tmpcol", (lambda bondtype, bondname, bondfname: clean_bondtype(bondtype, bondname, bondfname),
                          DBondInfo.bond_type.name, DBondInfo.bond_name.name, DBondInfo.bond_full_name.name)),
            (BondTypeSource.type.name, (lambda x: x[0], "__tmpcol")),
            (BondTypeSource.stype.name, (lambda x: x[1], "__tmpcol")),
        ])
    )

    ac = transform.AddConst(
        {
            BondTypeSource.dimension.name: "债券品种",
            BondTypeSource.source_id.name: "020002",
        }
    )

    dp = transform.Dropna(subset=[BondTypeSource.type.name])

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondTypeSource.bond_id.name,
            BondTypeSource.source_id.name: None,
            BondTypeSource.dimension.name: None,
            BondTypeSource.type.name: None,
            BondTypeSource.stype.name: None,
        }
    )

    s = Stream(inp, [vm, ac, dp, km])
    return s


def stream_020003():
    session = dbsession(bind=engine_r)
    stmt = session.query(DBondInfo).filter(
        DBondInfo.source_id == "020003"
    ).with_entities(
        DBondInfo.bond_id, DBondInfo.bond_name, DBondInfo.bond_full_name, DBondInfo.bond_type, DBondInfo.consigner
    )
    inp = MysqlInput(session.bind, stmt)

    def clean_bondtype(bond_type, bond_name, bond_full_name, consigner, bond_id):
        from functools import reduce
        bond_type, bond_name, bond_full_name, consigner = [
            (x or "") for x in (bond_type, bond_name, bond_full_name, consigner)
        ]

        # bond_type, bond_name, bond_full_name, consigner = ["深国债", "山西16Z1", "2016年山西省政府一般债券(六期)", "山西省政府"]
        DIRECT_TYPE_MAP = {
            "短期融资券": ("短期融资券", None),
            "资产证券化": ("资产支持证券", None),
            "可转债": ("可转债", None),
        }

        if bond_type in DIRECT_TYPE_MAP.keys():
            type_l1, type_l2 = DIRECT_TYPE_MAP[bond_type]

        else:
            patts_subfname = "".join(
                ".*?(%s)?" % x for x in ["|".join(x) for x in (
                    ("国债",), ("政府",), ("银行",), ("PPN",), ("SCP",),
                    ("MTN",), ("CP",), ("中国铁路建设",), ("可交换",), ("企业债",),
                    ("项目收益债券",), ("证券", "期货", "保险", "中国国际金融"), ("中小企业私募债",), ("非公开发行",), ("建设债", ),
                    ("集合债",), ("公开发行"), ("公司债",), ("中央汇金",),
                )]
            )
            patts_subname = "".join(
                ".*?(%s)?" % x for x in ["|".join(x) for x in (
                    ("ppn",), ("scp",), ("mtn",), ("cp",), ("mtn", "smecn", "prn", "gn"),
                    ("npb",)
                )]
            )
            patts_subcsg = "".join(
                ".*?(%s)?" % x for x in ["|".join(x) for x in (
                    ("政府",), ("国家开发银行", "中国农业发展银行", "中国进出口银行"), ("银行",), ("铁道部",), ("证券", "期货", "保险", "中国国际金融"),
                    ("城市建设",), ("中央汇金",)
                )]
            )
            subfname = reduce(lambda x, y: [(x[i] or y[i]) for i in range(len(x))], re.findall(patts_subfname, bond_full_name.lower()))
            subcsg = reduce(lambda x, y: [(x[i] or y[i]) for i in range(len(x))], re.findall(patts_subcsg, consigner))
            subname = reduce(lambda x, y: [(x[i] or y[i]) for i in range(len(x))], re.findall(patts_subname, bond_name.lower()))

            if subname[0]:
                return "定向工具", None

            elif subname[1]:
                return "短期融资券", None

            elif subname[4]:
                return "中期票据", None

            elif subname[2]:
                return "中期票据", None

            elif subname[3]:
                return "短期融资券", None

            elif subname[5]:
                return "企业债", None

            elif bond_type == "国债":
                if subfname[0]:
                    type_l1, type_l2 = "国债", None
                elif subfname[1] or subcsg[0]:
                    type_l1, type_l2 = "地方政府债", None
                else:
                    type_l1, type_l2 = None, None

            elif bond_type == "金融债":
                if subcsg[1]:
                    type_l1, type_l2 = "金融债", "政策性银行债"
                elif subfname[18] or subcsg[6]:
                    type_l1, type_l2 = "政府支持机构债", None
                elif subfname[2] or subcsg[2]:
                    type_l1, type_l2 = "金融债", "商业银行债"
                else:
                    type_l1, type_l2 = "金融债", "非银金融机构债"

            elif bond_type == "企债":

                if subfname[7] or subcsg[3]:
                    type_l1, type_l2 = "政府支持机构债", None
                elif subfname[8]:
                    type_l1, type_l2 = "可交换债", None
                elif subfname[9] or subfname[10] or subfname[14] or subfname[15] or (bond_id[-2:] == "IB") or subcsg[5]:
                    type_l1, type_l2 = "企业债", None
                elif subfname[11] and subcsg[4]:
                    type_l1, type_l2 = "金融债", "非银金融机构债"
                elif subfname[12]:
                    type_l1, type_l2 = "公司债", "中小企业私募债"
                elif subfname[13]:
                    type_l1, type_l2 = "公司债", "非公开发行公司债"
                elif subfname[16] and subfname[17]:
                    type_l1, type_l2 = "公司债", "一般公司债"

                else:
                    type_l1, type_l2 = None, None

            else:
                type_l1, type_l2 = None, None

        return type_l1, type_l2

    vm = transform.ValueMap(
        OrderedDict([
            ("__tmpcol", (lambda bondtype, bondname, bondfname, consigner, bond_id: clean_bondtype(bondtype, bondname, bondfname, consigner, bond_id),
                          DBondInfo.bond_type.name, DBondInfo.bond_name.name, DBondInfo.bond_full_name.name, DBondInfo.consigner.name, DBondInfo.bond_id.name)),
            (BondTypeSource.type.name, (lambda x: x[0], "__tmpcol")),
            (BondTypeSource.stype.name, (lambda x: x[1], "__tmpcol")),
        ])
    )

    ac = transform.AddConst(
        {
            BondTypeSource.dimension.name: "债券品种",
            BondTypeSource.source_id.name: "020003",
        }
    )

    dp = transform.Dropna(subset=[BondTypeSource.type.name])

    km = transform.MapSelectKeys(
        {
            DBondInfo.bond_id.name: BondTypeSource.bond_id.name,
            BondTypeSource.source_id.name: None,
            BondTypeSource.dimension.name: None,
            BondTypeSource.type.name: None,
            BondTypeSource.stype.name: None,
        }
    )

    s = Stream(inp, [vm, ac, dp, km])
    return s


def main():
    s11, s21, s22, s23 = stream_010001(), stream_020001(), stream_020002(), stream_020003()
    c = Confluence(s11, s21, s22, s23)
    io.to_sql(BondTypeSource.__tablename__, engine_w, c.dataframe)

if __name__ == "__main__":
    main()
