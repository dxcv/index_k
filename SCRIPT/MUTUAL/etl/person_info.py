import pandas as pd
import re
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DPersonInfo, DPersonDescription
from utils.database.models.base_public import IdMatch, PersonInfo

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]


_entities_map = [
    (IdMatch.matched_id, PersonInfo.person_id), (DPersonInfo.person_name, PersonInfo.person_name),
    (DPersonInfo.data_source, PersonInfo.data_source), (DPersonInfo.master_strategy, PersonInfo.master_strategy),
    (DPersonInfo.gender, PersonInfo.gender), (DPersonInfo.education, PersonInfo.education), (DPersonDescription.introduction, PersonInfo.resume),
    (DPersonInfo.investment_period, PersonInfo.investment_period)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_person_info(update_time):
    """
    Fetch records of DPersonInfo table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """
    query_pi = _session.query(IdMatch).join(
        DPersonInfo, and_(IdMatch.source_id == DPersonInfo.person_id, IdMatch.data_source == DPersonInfo.data_source, IdMatch.id_type == 3)
    ).outerjoin(
        DPersonDescription, and_(IdMatch.source_id == DPersonDescription.person_id, IdMatch.data_source == DPersonDescription.data_source, IdMatch.id_type == 3)
    ).filter(
        and_(DPersonInfo.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_pi.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[PersonInfo.person_id.name]
    return df


def transform():
    df = fetch_multisource_person_info(UPDATE_TIME)
    df[PersonInfo.master_strategy.name] = list(
        map(
            lambda ds, ms: etl.Enum.TypeMapping.type_name.get(etl.EnumMap.DPersonInfo.master_strategy.get((ds, ms))), df[PersonInfo.data_source.name], df[PersonInfo.master_strategy.name]
        )
    )
    df[PersonInfo.gender.name] = df[PersonInfo.gender.name].apply(lambda x: x if x in {"男", "女"} else None)

    df_020001 = df.ix[df[PersonInfo.data_source.name] == "020001"].drop_duplicates(subset=["person_id"])
    df_020001[PersonInfo.investment_period.name] = df_020001[PersonInfo.investment_period.name].fillna("").apply(lambda x: re.search("((\d*年)?(\d*天))", x)).apply(lambda x: x.groups()[0] if x is not None else None)
    df_020002 = df.ix[df[PersonInfo.data_source.name] == "020002"].drop_duplicates(subset=["person_id"])
    df_020003 = df.ix[df[PersonInfo.data_source.name] == "020003"].drop_duplicates(subset=["person_id"])

    result = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    ).fillna(df_020002).fillna(df_020003)[df_020001.columns]

    return result


def main():
    # _engine_wt.execute("DELETE FROM {tb}".format(tb=PersonInfo.__tablename__))
    # transform().to_sql(PersonInfo.__tablename__, _engine_wt, index=False, if_exists="append")
    io.to_sql(PersonInfo.__tablename__, _engine_wt, transform())

if __name__ == "__main__":
    main()
