import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    @classmethod
    def sub_wrong_to_none(cls, x):
        s = re.sub("\s|-| |--|---|万股|%", "", x)
        if s == "":
            return None
        else:
            return s

    @classmethod
    def stream_020001(cls):
        """
            清洗 d_fund_position（020001）;

        """

        sql = " \
               SELECT fps.fund_id,fps.statistic_date,fps.subject_id,dfp.quantity, fps.data_source  \
              FROM base_public.fund_position_stock AS fps \
              JOIN crawl_public.d_fund_position AS dfp \
              ON fps.fund_id = dfp.fund_id AND fps.statistic_date = dfp.statistic_date \
              AND fps.subject_id = dfp.subject_id \
              WHERE dfp.data_source = '020001' AND dfp.quantity is not NULL"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "quantity": lambda x: cls.sub_wrong_to_none(x)
        })

        vm2 = transform.ValueMap({
            "quantity": lambda x: float(x)
        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "data_source": "data_source",
            "statistic_date": "statistic_date",
            "subject_id": "subject_id",
            "quantity": "quantity",


        })

        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def stream_020003(cls):
        """
            清洗 d_fund_position（020003）;

        """

        sql = " \
               SELECT fps.fund_id,fps.statistic_date,fps.subject_id,dfp.quantity, fps.data_source  \
              FROM base_public.fund_position_stock AS fps \
              JOIN crawl_public.d_fund_position AS dfp \
              ON fps.fund_id = dfp.fund_id AND fps.statistic_date = dfp.statistic_date \
              AND fps.subject_id = dfp.subject_id \
              WHERE dfp.data_source = '020001' AND dfp.quantity is not NULL"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "quantity": lambda x: cls.sub_wrong_to_none(x)
        })

        vm2 = transform.ValueMap({
            "quantity": lambda x: float(x)
        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "data_source": "data_source",
            "statistic_date": "statistic_date",
            "subject_id": "subject_id",
            "quantity": "quantity"
        })

        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.stream_020001(), cls.stream_020003()]
        c = Confluence(*streams, on=["fund_id", "data_source", "statistic_date", "subject_id"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base_public.fund_position_stock", ENGINE_RD, df.dataframe, type="update")


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()

