from sqlalchemy.orm import sessionmaker
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput
from utils.database.models.base_private import FundInfo
from pypinyin import pinyin as py, Style as py_style
import re


ENGINE_RD = cfg.load_engine()["etl_base_private"]
ENGINE_WT = cfg.load_engine()["etl_base_private"]
dbsession = sessionmaker()


def stream():
    session = dbsession(bind=ENGINE_RD)
    query = session.query(FundInfo).with_entities(
        FundInfo.fund_id, FundInfo.fund_name, FundInfo.fund_name_py
    )
    inp = MysqlInput(session.bind, query)

    vm1 = transform.ValueMap(
        {
            FundInfo.fund_name.name: lambda x: re.sub("（.*）|\(.*\)", "", x)
        }
    )

    vm2 = transform.ValueMap(
        {
            FundInfo.fund_name_py.name: (
                lambda x: "".join([x[0] for x in py(x, style=py_style.FIRST_LETTER)]).upper(), FundInfo.fund_name.name
            )
        }
    )

    sk = transform.MapSelectKeys(
        {
            FundInfo.fund_id.name: None,
            FundInfo.fund_name_py.name: None
        }
    )
    s = Stream(inp, transform=(vm1, vm2, sk))
    return s


def main():
    s = stream()
    s.flow()
    io.to_sql(FundInfo.__tablename__, ENGINE_WT, s.dataframe)


if __name__ == "__main__":
    main()
