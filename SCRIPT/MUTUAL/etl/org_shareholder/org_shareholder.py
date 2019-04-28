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
            清洗 d_org_shareholder;

        """

        sql = " \
                SELECT idm.matched_id, doi.data_source, doi.statistic_date, doi.shareholder_name, \
                doi.shareholder_num, doi.capital_stock, doi.stock_held, doi.proportion_held \
                FROM (SELECT matched_id,source_id from base_public.id_match \
                WHERE data_source = '020001' AND is_used = 1 and id_type = 2) as idm \
                JOIN crawl_public.d_org_shareholder AS doi \
                ON doi.org_id = idm.source_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "capital_stock": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "stock_held": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "proportion_held": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
        })

        vm2 = transform.ValueMap({
            "capital_stock": lambda x: float(x) if type(x) is str else x,
            "stock_held": lambda x: float(x) if type(x) is str else x,
            "proportion_held": lambda x: round(float(x)/100, 6) if type(x) is str else x,


        })

        sk = transform.MapSelectKeys({
            "matched_id": "org_id",
            'data_source': 'data_source',
            'statistic_date': 'statistic_date',
            'capital_stock': 'capital_stock',
            'stock_held': 'stock_held',
            'proportion_held': 'proportion_held',
            'shareholder_num': 'shareholder_num',
            'shareholder_name': 'shareholder_name'
        })
        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def org_name(cls):
        sql = "UPDATE base_public.org_shareholder AS fh , \
        (SELECT org_id, org_name from base_public.org_info) AS fi \
        SET fh.org_name = fi.org_name \
        WHERE fh.org_id = fi.org_id"
        ENGINE_RD.execute(sql)

    @classmethod
    def confluence(cls):
        streams = [cls.stream_020001()]
        c = Confluence(*streams, on=["org_id", "statistic_date", "data_source", "shareholder_name"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base_public.org_shareholder", ENGINE_RD, df.dataframe, type="update")
        cls.org_name()


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.org_shareholder_test", "base_public.org_shareholder", ENGINE_RD,
                            cols_excluded={'org_id', 'statistic_date', 'org_name', 'entry_time', 'update_time',
                                           'data_source'})
    t.result.to_csv(os.path.expanduser("~/Desktop/org_shareholder__{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()



