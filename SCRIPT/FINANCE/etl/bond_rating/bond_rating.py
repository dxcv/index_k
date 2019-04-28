from utils.database import config as cfg
from utils.etlkit.core import transform
from utils.etlkit.core.base import Frame, Stream, Confluence
from utils.etlkit.reader.mysqlreader import MysqlNativeInput, MysqlOrmInput
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker, Session
from utils.database.models.crawl_finance import DBondRating
from utils.database.models.base_finance import BondRating
from utils.database import io
import re


engine_r = cfg.load_engine()["etl_finance"]
engine_w = cfg.load_engine()["etl_base_finance"]
dbsession = sessionmaker()


def stream_020004():
    """
    债券评级信息(万得)
    """
    session = dbsession(bind=engine_r)

    stmt = Session().query(DBondRating).filter(
        and_(DBondRating.source_id == "020004", DBondRating.is_used == 1)
    ).with_entities(
        DBondRating.bond_id, DBondRating.source_id, DBondRating.statistic_date, DBondRating.rating_type,
        DBondRating.credit_rating, DBondRating.rating_agency, DBondRating.rating_outlook
    )

    vm = transform.ValueMap(
        {
            DBondRating.credit_rating.name: lambda x: re.sub("\s", "", x)
        }
    )

    inp = MysqlOrmInput(session.bind, stmt)

    km = transform.MapSelectKeys(
        {
            DBondRating.bond_id.name: BondRating.bond_id.name,
            DBondRating.statistic_date.name: BondRating.statistic_date.name,
            DBondRating.rating_type.name: BondRating.rating_type.name,
            DBondRating.credit_rating.name: BondRating.credit_rating.name,
            DBondRating.rating_agency.name: BondRating.rating_agency.name,
            DBondRating.rating_outlook.name: BondRating.rating_outlook.name,
        }
    )

    stream = Stream(
        inp, (km, vm), name="d_bond_rating_wind"
    )

    return stream


def main():
    s = stream_020004()
    c = Confluence(s)
    io.to_sql(BondRating.__tablename__, engine_w, c.dataframe, chunksize=500)


if __name__ == "__main__":
    main()
