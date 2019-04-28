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


        sql = "SELECT fi.fund_id, fi.fund_name, fns.data_source, \
                fns.statistic_date, fns.nav, fns.added_nav \
                FROM base_public.fund_info fi \
                JOIN data_test.fund_nv_source_test fns ON fi.fund_id = fns.fund_id \
                JOIN ( \
                SELECT fund_id, statistic_date  \
                FROM data_test.fund_nv_source_test WHERE update_time BETWEEN '{start}' AND '{end}' \
                GROUP BY fund_id, statistic_date \
                ) fupt ON fns.fund_id = fupt.fund_id AND fns.statistic_date = fupt.statistic_date \
                WHERE fns.data_source like '010%%' AND fns.is_used = 1".format(start=cls.start, end=cls.end)

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            'data_source': 'data_source',
            'statistic_date': 'statistic_date',
            'nav': 'nav',
            'added_nav': 'added_nav',
            "fund_name": "fund_name"
        })
        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def stream_020001(cls):


        sql = "SELECT fi.fund_id, fi.fund_name, fns.data_source, \
                fns.statistic_date, fns.nav, fns.added_nav \
                FROM base_public.fund_info fi \
                JOIN data_test.fund_nv_source_test fns ON fi.fund_id = fns.fund_id \
                JOIN ( \
                SELECT fund_id, statistic_date  \
                FROM data_test.fund_nv_source_test WHERE update_time BETWEEN '{start}' AND '{end}' \
                GROUP BY fund_id, statistic_date \
                ) fupt ON fns.fund_id = fupt.fund_id AND fns.statistic_date = fupt.statistic_date \
                WHERE fns.data_source = '020001' AND fns.is_used = 1".format(start=cls.start, end=cls.end)

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            'data_source': 'data_source',
            'statistic_date': 'statistic_date',
            'nav': 'nav',
            'added_nav': 'added_nav',
            "fund_name": "fund_name"
        })
        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def stream_020002(cls):


        sql = "SELECT fi.fund_id, fi.fund_name, fns.data_source, \
                fns.statistic_date, fns.nav, fns.added_nav \
                FROM base_public.fund_info fi \
                JOIN data_test.fund_nv_source_test fns ON fi.fund_id = fns.fund_id \
                JOIN ( \
                SELECT fund_id, statistic_date  \
                FROM data_test.fund_nv_source_test WHERE update_time BETWEEN '{start}' AND '{end}' \
                GROUP BY fund_id, statistic_date \
                ) fupt ON fns.fund_id = fupt.fund_id AND fns.statistic_date = fupt.statistic_date \
                WHERE fns.data_source = '020002' AND fns.is_used = 1".format(start=cls.start, end=cls.end)

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            'data_source': 'data_source',
            'statistic_date': 'statistic_date',
            'nav': 'nav',
            'added_nav': 'added_nav',
            "fund_name": "fund_name"
        })
        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def stream_020003(cls):


        sql = "SELECT fi.fund_id, fi.fund_name, fns.data_source, \
                fns.statistic_date, fns.nav, fns.added_nav \
                FROM base_public.fund_info fi \
                JOIN data_test.fund_nv_source_test fns ON fi.fund_id = fns.fund_id \
                JOIN ( \
                SELECT fund_id, statistic_date  \
                FROM data_test.fund_nv_source_test WHERE update_time BETWEEN '{start}' AND '{end}' \
                GROUP BY fund_id, statistic_date \
                ) fupt ON fns.fund_id = fupt.fund_id AND fns.statistic_date = fupt.statistic_date \
                WHERE fns.data_source = '020003' AND fns.is_used = 1".format(start=cls.start, end=cls.end)

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
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
        streams = [cls.stream_010xxx(), cls.stream_020001(), cls.stream_020002(), cls.stream_020003()]
        c = Confluence(*streams, on=["fund_id", "statistic_date"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("data_test.fund_nv_test", ENGINE_RD, df.dataframe, type="update")


def main():
    """

    取当前时间2小时05分之前fund_nv_source更新的净值

    """
    StreamsMain.write()


if __name__ == "__main__":
    main()






