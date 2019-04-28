import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DOrgInfo
from utils.database.models.base_public import IdMatch, OrgInfo

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]


_entities_map = [
    (IdMatch.matched_id, OrgInfo.org_id), (DOrgInfo.org_name, OrgInfo.org_name), (DOrgInfo.org_full_name, OrgInfo.org_full_name),
    (DOrgInfo.org_name_en, OrgInfo.org_name_en), (DOrgInfo.org_type_code, OrgInfo.org_type_code), (DOrgInfo.data_source, OrgInfo.data_source),
    (DOrgInfo.foundation_date, OrgInfo.foundation_date), (DOrgInfo.form, OrgInfo.form), (DOrgInfo.legal_person, OrgInfo.legal_person),
    (DOrgInfo.chairman, OrgInfo.chairman), (DOrgInfo.general_manager, OrgInfo.general_manager), (DOrgInfo.reg_capital, OrgInfo.reg_capital),
    (DOrgInfo.reg_address, OrgInfo.reg_address), (DOrgInfo.address, OrgInfo.address), (DOrgInfo.tel, OrgInfo.tel),
    (DOrgInfo.fax, OrgInfo.fax), (DOrgInfo.email, OrgInfo.email), (DOrgInfo.website, OrgInfo.website),
    (DOrgInfo.tel_service, OrgInfo.tel_service), (DOrgInfo.scale, OrgInfo.scale), (DOrgInfo.scale_mgt, OrgInfo.scale_mgt)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = [OrgInfo.org_type]
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_org_info(update_time):
    """
    Fetch records of DOrgInfo table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """
    query_oi = _session.query(IdMatch).join(
        DOrgInfo, and_(IdMatch.source_id == DOrgInfo.org_id, IdMatch.data_source == DOrgInfo.data_source)
    ).filter(
        and_(DOrgInfo.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oi.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[OrgInfo.org_id.name]
    return df


def transform():
    df = fetch_multisource_org_info(UPDATE_TIME)
    df[OrgInfo.org_type.name] = df[OrgInfo.org_type_code.name].apply(lambda x: etl.EnumMap.DOrgInfo.org_type.get(x))
    df[OrgInfo.form.name] = list(map(lambda x, y: etl.EnumMap.DOrgInfo.form.get((x, y)), df[OrgInfo.data_source.name], df[OrgInfo.form.name]))
    df[OrgInfo.scale.name] = df[OrgInfo.scale.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x, "亿"))
    df[OrgInfo.scale_mgt.name] = df[OrgInfo.scale_mgt.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x, "亿"))

    df_020001 = df.ix[df[OrgInfo.data_source.name] == "020001"]
    df_020002 = df.ix[df[OrgInfo.data_source.name] == "020002"]
    df_020003 = df.ix[df[OrgInfo.data_source.name] == "020003"]

    result_main = df_020002.join(
        df_020001, how="outer", rsuffix="_020001"
    ).join(
        df_020002, how="outer", rsuffix="_020003"
    ).fillna(df_020001).fillna(df_020003)[df_020002.columns]

    use_020001 = [OrgInfo.form.name]
    result_020001 = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    ).fillna(df_020002).fillna(df_020003)[use_020001]

    result_main[use_020001] = result_020001
    return result_main


def main():
    io.to_sql(OrgInfo.__tablename__, _engine_wt, transform())

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