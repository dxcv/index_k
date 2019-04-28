import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundInfo
from utils.database.models.base_public import IdMatch, FundInfo, FundTypeMappingSource
from utils.etlkit.core import base, transform
from utils.etlkit.reader.mysqlreader import MysqlInput
from collections import OrderedDict

ENGINE = cfg.load_engine()["2Gbp"]
dbsession = sessionmaker()


def stream_020001():
    session = dbsession(bind=ENGINE)

    query_oh = session.query(IdMatch).join(
        DFundInfo, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundInfo.fund_id, IdMatch.data_source == DFundInfo.data_source, IdMatch.data_source == "020001")
    ).outerjoin(
        FundInfo, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundInfo.fund_id)
    ).filter(
        IdMatch.is_used == 1
    ).with_entities(
        FundInfo.fund_id, FundInfo.fund_name, IdMatch.data_source, DFundInfo.fund_type
    )
    inp = MysqlInput(session.bind, query_oh)

    vm = transform.ValueMap(
        OrderedDict([
            [FundTypeMappingSource.type_code.name, (lambda ds, tp: etl.EnumMap.DFundInfo.type_code2.get((ds, tp)), DFundInfo.data_source.name, DFundInfo.fund_type.name)],
            [FundTypeMappingSource.type_name.name, (lambda tc: etl.Enum.TypeMapping.type_name.get(tc), FundTypeMappingSource.type_code.name)],
            [FundTypeMappingSource.typestandard_code.name, (lambda tc: tc[:-2], FundTypeMappingSource.type_code.name)],
            [FundTypeMappingSource.typestandard_name.name, (lambda tsc: etl.Enum.TypeMapping.typestandard_name.get(tsc), FundTypeMappingSource.typestandard_code.name)]
        ])
    )

    dn = transform.Dropna(subset=[FundInfo.fund_id.name, FundTypeMappingSource.typestandard_code.name])

    sk = transform.MapSelectKeys(
        {
            FundInfo.fund_id.name: None,
            FundInfo.fund_name.name: None,
            IdMatch.data_source.name: FundTypeMappingSource.data_source.name,
            FundTypeMappingSource.typestandard_code.name: None,
            FundTypeMappingSource.typestandard_name.name: None,
            FundTypeMappingSource.type_code.name: None,
            FundTypeMappingSource.type_name.name: None,
        }
    )

    s = base.Stream(inp, transform=(vm, dn, sk))
    return s


def stream_020002():
    session = dbsession(bind=ENGINE)

    query_oh = session.query(IdMatch).join(
        DFundInfo, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundInfo.fund_id, IdMatch.data_source == DFundInfo.data_source, IdMatch.data_source == "020002")
    ).outerjoin(
        FundInfo, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundInfo.fund_id)
    ).filter(
        IdMatch.is_used == 1
    ).with_entities(
        FundInfo.fund_id, FundInfo.fund_name, FundInfo.fund_full_name, IdMatch.data_source, DFundInfo.fund_type
    )
    inp = MysqlInput(session.bind, query_oh)

    def clean_typecode(ds, tp, fn):
        if tp != "ETF-场内":
            return etl.EnumMap.DFundInfo.type_code2.get((ds, tp))
        else:
            if "债" in fn:
                return "0202"
            else:
                return "0201"
            return None

    vm = transform.ValueMap(
        OrderedDict([
            [FundTypeMappingSource.type_code.name, (lambda ds, tp, fn: clean_typecode(ds, tp, fn),
                                                    DFundInfo.data_source.name, DFundInfo.fund_type.name, FundInfo.fund_full_name.name)],
            [FundTypeMappingSource.type_name.name, (lambda tc: etl.Enum.TypeMapping.type_name.get(tc), FundTypeMappingSource.type_code.name)],
            [FundTypeMappingSource.typestandard_code.name, (lambda tc: tc[:-2], FundTypeMappingSource.type_code.name)],
            [FundTypeMappingSource.typestandard_name.name, (lambda tsc: etl.Enum.TypeMapping.typestandard_name.get(tsc), FundTypeMappingSource.typestandard_code.name)]
        ])
    )

    dn = transform.Dropna(subset=[FundInfo.fund_id.name, FundTypeMappingSource.typestandard_code.name])

    sk = transform.MapSelectKeys(
        {
            FundInfo.fund_id.name: None,
            FundInfo.fund_name.name: None,
            IdMatch.data_source.name: FundTypeMappingSource.data_source.name,
            FundTypeMappingSource.typestandard_code.name: None,
            FundTypeMappingSource.typestandard_name.name: None,
            FundTypeMappingSource.type_code.name: None,
            FundTypeMappingSource.type_name.name: None,
        }
    )

    s = base.Stream(inp, transform=(vm, dn, sk))
    return s


def stream_020003():
    session = dbsession(bind=ENGINE)

    query_oh = session.query(IdMatch).join(
        DFundInfo, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundInfo.fund_id, IdMatch.data_source == DFundInfo.data_source, IdMatch.data_source == "020003")
    ).outerjoin(
        FundInfo, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundInfo.fund_id)
    ).filter(
        IdMatch.is_used == 1
    ).with_entities(
        FundInfo.fund_id, FundInfo.fund_name, FundInfo.fund_full_name, IdMatch.data_source, DFundInfo.fund_type
    )
    inp = MysqlInput(session.bind, query_oh)

    def clean_typecode(ds, tp, fn):
        if tp != "指数型":
            return etl.EnumMap.DFundInfo.type_code2.get((ds, tp))
        else:
            if "股债" in fn:
                return "0203"
            elif "债" in fn:
                return "0202"
            else:
                return "0201"

    vm = transform.ValueMap(
        OrderedDict([
            [FundTypeMappingSource.type_code.name, (lambda ds, tp, fn: clean_typecode(ds, tp, fn), DFundInfo.data_source.name, DFundInfo.fund_type.name, FundInfo.fund_full_name.name)],
            [FundTypeMappingSource.type_name.name, (lambda tc: etl.Enum.TypeMapping.type_name.get(tc), FundTypeMappingSource.type_code.name)],
            [FundTypeMappingSource.typestandard_code.name, (lambda tc: tc[:-2], FundTypeMappingSource.type_code.name)],
            [FundTypeMappingSource.typestandard_name.name, (lambda tsc: etl.Enum.TypeMapping.typestandard_name.get(tsc), FundTypeMappingSource.typestandard_code.name)]
        ])
    )

    dn = transform.Dropna(subset=[FundInfo.fund_id.name, FundTypeMappingSource.typestandard_code.name])

    sk = transform.MapSelectKeys(
        {
            FundInfo.fund_id.name: None,
            FundInfo.fund_name.name: None,
            IdMatch.data_source.name: FundTypeMappingSource.data_source.name,
            FundTypeMappingSource.typestandard_code.name: None,
            FundTypeMappingSource.typestandard_name.name: None,
            FundTypeMappingSource.type_code.name: None,
            FundTypeMappingSource.type_name.name: None,
        }
    )

    s = base.Stream(inp, transform=(vm, dn, sk))
    return s


def confluence():
    s21 = stream_020001()
    s22 = stream_020002()
    s23 = stream_020003()
    c = base.Confluence(s21, s22, s23)
    return c


def confluence_merged():
    s21 = stream_020001()
    s22 = stream_020002()
    s23 = stream_020003()
    c = base.Confluence(s21, s22, s23, on=[FundInfo.fund_id.name])
    return c
