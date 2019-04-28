from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    @classmethod
    def stream_type(cls):
        """
            清洗 fund_type;

        """

        sql = " \
                SELECT * FROM (SELECT DISTINCT matched_id FROM base_public.id_match WHERE id_type = 1 AND is_used = 1) AS idm \
                JOIN (SELECT fund_id,type_name FROM base_public.fund_type_mapping WHERE typestandard_code = '02') \
                AS ftm ON idm.matched_id = ftm.fund_id \
"

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "type_name": "fund_type",
            'fund_id': 'fund_id',

        })
        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.stream_type()]
        c = Confluence(*streams, on=["fund_id"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base_public.fund_info", ENGINE_RD, df.dataframe, type="update")
        print(df.dataframe)


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()



