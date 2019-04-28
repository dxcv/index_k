import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DOrgFund
from utils.database.models.base_public import IdMatch, FundInfo, OrgInfo, FundOrgMapping

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]


_entities_map = [
    (DOrgFund.data_source, FundOrgMapping.data_source),
    (DOrgFund.org_id, FundOrgMapping.org_id), (DOrgFund.fund_id, FundOrgMapping.fund_id),
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = [FundOrgMapping.fund_name, FundOrgMapping.org_name]
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_org_info():
    query = _session.query(IdMatch).filter(
        IdMatch.id_type == 2
    ).join(
        OrgInfo, IdMatch.matched_id == OrgInfo.org_id
    ).with_entities(
        IdMatch.data_source, IdMatch.source_id, IdMatch.matched_id, OrgInfo.org_name
    )
    orgs = pd.DataFrame(query.all())
    org_id = dict(zip(
        [tuple(x) for x in orgs[[IdMatch.data_source.name, IdMatch.source_id.name]].as_matrix()],
        orgs[IdMatch.matched_id.name]

    ))
    org_name = dict(zip(
        [tuple(x) for x in orgs[[IdMatch.data_source.name, IdMatch.source_id.name]].as_matrix()],
        orgs[OrgInfo.org_name.name]

    ))
    return org_id, org_name


def fetch_fund_info():
    query = _session.query(IdMatch).filter(
        IdMatch.id_type == 1
    ).join(
        FundInfo, and_(IdMatch.matched_id == FundInfo.fund_id, IdMatch.is_used == 1)
    ).with_entities(
        IdMatch.data_source, IdMatch.source_id, IdMatch.matched_id, FundInfo.fund_name
    )
    funds = pd.DataFrame(query.all())
    fund_id = dict(zip(
        [tuple(x) for x in funds[[IdMatch.data_source.name, IdMatch.source_id.name]].as_matrix()],
        funds[IdMatch.matched_id.name]

    ))
    fund_name = dict(zip(
        [tuple(x) for x in funds[[IdMatch.data_source.name, IdMatch.source_id.name]].as_matrix()],
        funds[FundInfo.fund_name.name]

    ))
    return fund_id, fund_name


def fetch_multisource_fom(update_time):
    query_fom = _session.query(DOrgFund).filter(
        DOrgFund.update_time >= update_time
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_fom.all())
    df.columns = [x.name for x in _map_entities]
    return df


def transform():
    df = fetch_multisource_fom(UPDATE_TIME)
    fid, fname = fetch_fund_info()
    oid, oname = fetch_org_info()

    df[FundOrgMapping.fund_name.name] = list(map(lambda x, y: fname.get((x, y)), df[FundOrgMapping.data_source.name], df[FundOrgMapping.fund_id.name]))
    df[FundOrgMapping.fund_id.name] = list(map(lambda x, y: fid.get((x, y)), df[FundOrgMapping.data_source.name], df[FundOrgMapping.fund_id.name]))

    df[FundOrgMapping.org_name.name] = list(map(lambda x, y: oname.get((x, y)), df[FundOrgMapping.data_source.name], df[FundOrgMapping.org_id.name]))
    df[FundOrgMapping.org_id.name] = list(map(lambda x, y: oid.get((x, y)), df[FundOrgMapping.data_source.name], df[FundOrgMapping.org_id.name]))
    df = df.dropna(subset=[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name])
    df.index = df[[FundOrgMapping.fund_id.name, FundOrgMapping.org_id.name]]

    df_020001 = df.ix[df[OrgInfo.data_source.name] == "020001"]
    df_020002 = df.ix[df[OrgInfo.data_source.name] == "020002"]
    df_020003 = df.ix[df[OrgInfo.data_source.name] == "020003"]

    result = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    ).fillna(df_020002).fillna(df_020003)[df_020001.columns]
    result = result.drop(FundOrgMapping.data_source.name, axis=1)
    result["type_code"] = 1
    result["type_name"] = "基金管理人"

    return result


def main():
    io.to_sql(FundOrgMapping.__tablename__, _engine_wt, transform())

if __name__ == "__main__":
    main()


# def split_source(dataframe, source_key):
#     sources = set(dataframe[source_key])
#     dfs = {source: dataframe.ix[dataframe[source_key] == source] for source in sources}
#     return dfs
#
#
# def merge_source(dataframe, source_key, main):
#     dfs = split_source(dataframe, source_key)
#     df_main = dfs[main]
#
#     keys = {set}