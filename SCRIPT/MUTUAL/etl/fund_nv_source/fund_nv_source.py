from datetime import timedelta
from datetime import datetime
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


def now_time(a=0):
    now = datetime.now()
    delta = timedelta(minutes=a)
    n_days = now + delta
    cc = n_days.strftime('%Y-%m-%d %H:%M:%S')
    return cc


class StreamsMain:

    end = now_time()
    start = now_time(-125)

    @classmethod
    def stream_010xxx(cls):


        sql = " \
                SELECT idm.matched_id, df.data_source, df.statistic_date, \
                df.nav, df.added_nav, fi.fund_name \
                FROM (SELECT matched_id,source_id,data_source \
                FROM base_public.id_match WHERE id_type = 1 and is_used = 1) AS idm \
                JOIN \
                crawl_public.i_fund_nv AS df \
                on idm.source_id = df.fund_id \
                AND idm.data_source = df.data_source \
                JOIN base_public.fund_info as fi \
                ON fi.fund_id = df.fund_id \
                WHERE df.update_time BETWEEN '{start}' \
                AND '{end}' and is_used = 1".format(start=cls.start, end=cls.end)

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            'data_source': 'data_source',
            'statistic_date': 'statistic_date',
            'nav': 'nav',
            'added_nav': 'added_nav',
            "fund_name": "fund_name"
        })
        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def stream_02000x(cls):


        sql = " \
                SELECT idm.matched_id, df.data_source, df.statistic_date, \
                df.nav, df.added_nav, fi.fund_name \
                FROM (SELECT matched_id,source_id,data_source \
                FROM base_public.id_match WHERE id_type = 1 and is_used = 1) AS idm \
                JOIN \
                crawl_public.d_fund_nv AS df \
                on idm.source_id = df.fund_id \
                AND idm.data_source = df.data_source \
                JOIN base_public.fund_info as fi \
                ON fi.fund_id = df.fund_id \
                WHERE df.update_time BETWEEN '{start}' \
                AND '{end}' and is_used = 1".format(start=cls.start, end=cls.end)

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            'data_source': 'data_source',
            'statistic_date': 'statistic_date',
            'nav': 'nav',
            'added_nav': 'added_nav',
            "fund_name": "fund_name"

        })

        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.stream_02000x(), cls.stream_010xxx()]
        c = Confluence(*streams, on=["fund_id", "statistic_date", "data_source"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("data_test.fund_nv_source_test", ENGINE_RD, df.dataframe, type="update")


def main():
    """

    取当前时间2小时05分之前更新的净值

    """
    StreamsMain.write()


if __name__ == "__main__":
    main()






