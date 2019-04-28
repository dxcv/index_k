import datetime
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput

ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    datetime = datetime.datetime.now().strftime("%Y-%m-%d")

    @classmethod
    def stream_manager(cls):
        """
            清洗 fund_manager_mapping;

        """

        sql = 'SELECT fi.matched_id, GROUP_CONCAT(fmm.person_name SEPARATOR ",") as person_name  \
                FROM (SELECT DISTINCT matched_id FROM base.id_match WHERE id_type = 1 AND is_used = 1) fi  \
                JOIN base.fund_manager_mapping fmm ON fi.matched_id = fmm.fund_id  \
                WHERE fmm.is_current is NULL  or fmm.is_current > 0 \
                GROUP BY fund_id '

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "person_name": "fund_member",
            'matched_id': 'fund_id'

        })
        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def end_date(cls):
        """
            清洗 fund_manager_mapping;

        """

        sql = "SELECT idm.matched_id, fmg.end_date FROM (SELECT DISTINCT matched_id FROM base.id_match) as idm \
              JOIN base.fund_manager_mapping AS fmg ON idm.matched_id = fmg.fund_id \
              WHERE end_date> '{time}' GROUP BY fund_id".format(time=cls.datetime)

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "end_date": "end_date",
            'matched_id': 'fund_id'

        })
        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base.fund_info", ENGINE_RD, df.dataframe, type="update")

    @classmethod
    def confluence(cls):
        streams = [cls.stream_manager(), cls.end_date()]
        c = Confluence(*streams, on=["fund_id"])
        return c


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()



