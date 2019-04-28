from sqlalchemy.orm import sessionmaker
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput
from utils.database.models.base_private import OrgInfo
from pypinyin import pinyin as py, Style as py_style
import re


ENGINE_RD = cfg.load_engine()["etl_base_private"]
ENGINE_WT = cfg.load_engine()["etl_base_private"]
dbsession = sessionmaker()


def stream():
    session = dbsession(bind=ENGINE_RD)
    query = session.query(OrgInfo).with_entities(
        OrgInfo.org_id, OrgInfo.org_name, OrgInfo.org_name_py
    )
    inp = MysqlInput(session.bind, query)

    vm1 = transform.ValueMap(
        {
            OrgInfo.org_name.name: lambda x: re.sub("（.*）|\(.*\)", "", x)
        }
    )

    vm2 = transform.ValueMap(
        {
            OrgInfo.org_name_py.name: (
                lambda x: "".join([x[0] for x in py(x, style=py_style.FIRST_LETTER)]).upper(), OrgInfo.org_name.name
            )
        }
    )

    sk = transform.MapSelectKeys(
        {
            OrgInfo.org_id.name: None,
            OrgInfo.org_name_py.name: None
        }
    )
    s = Stream(inp, transform=(vm1, vm2, sk))
    return s


def main():
    s = stream()
    s.flow()
    io.to_sql(OrgInfo.__tablename__, ENGINE_WT, s.dataframe)


if __name__ == "__main__":
    main()
