from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput

ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    @classmethod
    def stream_manager(cls):
        """
            清洗 fund_manager_mapping;

        """

        sql = 'SELECT fi.fund_id, GROUP_CONCAT(fmm.person_name SEPARATOR ",") as person_name \
                FROM base_public.fund_info fi \
                JOIN base_public.fund_manager_mapping fmm ON fi.fund_id = fmm.fund_id \
                WHERE fmm.is_current = 1 \
                GROUP BY fund_id'

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "person_name": "fund_manager",
            'fund_id': 'fund_id'

        })
        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base_public.fund_info", ENGINE_RD, df.dataframe, type="update")
        print(df.dataframe)

    @classmethod
    def confluence(cls):
        streams = [cls.stream_manager()]
        c = Confluence(*streams, on=["fund_id"])
        return c


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()



