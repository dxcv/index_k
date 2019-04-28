from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    @classmethod
    def stream_manager_nominal(cls):
        """
            清洗 fund_manager_nominal;

        """

        sql = " \
                SELECT idm.matched_id, fom.org_name FROM (SELECT DISTINCT matched_id FROM base_public.id_match \
                WHERE id_type = 1 AND is_used = 1) AS idm \
                JOIN base_public.fund_org_mapping \
                AS fom ON idm.matched_id = fom.fund_id WHERE fom.type_code = 1"

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "org_name": "fund_manager_nominal",
            'matched_id': 'fund_id',

        })
        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def stream_custodian(cls):
        """
            清洗 fund_custodian;

        """

        sql = " \
                SELECT idm.matched_id, fom.org_name FROM (SELECT DISTINCT matched_id FROM base_public.id_match \
                WHERE id_type = 1 AND is_used = 1) AS idm \
                JOIN base_public.fund_org_mapping \
                AS fom ON idm.matched_id = fom.fund_id WHERE fom.type_code = 2"

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "org_name": "fund_custodian",
            'matched_id': 'fund_id',

        })
        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.stream_manager_nominal(), cls.stream_custodian()]
        c = Confluence(*streams, on=["fund_id"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base_public.fund_info", ENGINE_RD, df.dataframe, type="update")


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()



