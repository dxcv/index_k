from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    @classmethod
    def stream_recommendation_end(cls):
        """
            清洗 recommendation_end;

        """

        sql = " \
                SELECT idm.matched_id, dff.recommendation_end, dff.recommendation_start FROM \
                (SELECT DISTINCT matched_id from base_public.id_match WHERE id_type = 1 AND is_used =1) AS idm \
                JOIN \
                (SELECT * FROM ( \
                SELECT fund_id, recommendation_end, recommendation_start FROM crawl_public.d_fund_info  \
                WHERE recommendation_end <> '0000-00-00' and recommendation_start <> '0000-00-00') AS dfi  \
                GROUP BY dfi.fund_id) as dff \
                ON idm.matched_id = dff.fund_id"

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "recommendation_end": "recommendation_end",
            "recommendation_start" : "recommendation_start",
            'matched_id': 'fund_id',

        })
        s = Stream(inp, transform=[sk])
        return s


    @classmethod
    def confluence(cls):
        streams = [cls.stream_recommendation_end()]
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


