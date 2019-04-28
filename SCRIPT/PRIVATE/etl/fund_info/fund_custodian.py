import numpy as np
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlNativeInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    @classmethod
    def stream_custodian(cls):

        sql = "select ff.fund_id,GROUP_CONCAT(o.org_name SEPARATOR ',') as org_name from   \
                ((SELECT DISTINCT matched_id as fund_id from base.id_match where id_type=1 and is_used=1) as ff \
                JOIN   \
                (SELECT fund_id,org_id from base.fund_org_mapping WHERE org_type_code = 5) as fom \
                ON ff.fund_id = fom.fund_id    \
                LEFT JOIN \
                (SELECT org_id,org_name from base.org_info) as o \
                on fom.org_id = o.org_id) \
                GROUP BY ff.fund_id"

        inp = MysqlNativeInput(ENGINE_RD, sql)

        km = transform.MapSelectKeys(
            {
                "fund_id": "fund_id",
                "org_name": "fund_custodian"
            }
        )

        stream = Stream(inp, transform=[km])
        return stream

    @classmethod
    def stream_is_reg(cls):

        sql = "SELECT idm.matched_id, idd.matched_id AS is_reg FROM \
                (SELECT DISTINCT matched_id FROM base.id_match WHERE id_type = 1 AND is_used = 1) AS idm \
                LEFT JOIN \
                (SELECT DISTINCT matched_id FROM base.id_match WHERE  \
                source in  ('010002','010003','010004','010005') and is_used =1 and id_type=1 ) AS idd \
                ON idm.matched_id = idd.matched_id"

        inp = MysqlNativeInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "is_reg": lambda x:  1 if type(x) is str else 0
        })

        km = transform.MapSelectKeys(
            {
                "matched_id": "fund_id",
                "is_reg": "is_reg"
            }
        )

        stream = Stream(inp, transform=[vm, km])
        return stream

    @classmethod
    def confluence(cls):
        streams = [cls.stream_custodian(), cls.stream_is_reg()]
        c = Confluence(*streams, on=["fund_id"])
        return c

    @classmethod
    def write(cls):
        b = cls.confluence()
        io.to_sql("base.fund_info", ENGINE_RD, b.dataframe, type="update")
        # io.to_sql("data_test.fund_info_test001", ENGINE_RD, b.dataframe, type="update")


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()





