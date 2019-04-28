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
            清洗 d_org_position_stock;

        """

        sql = " \
                SELECT idm.matched_id, dog.statistic_date, dog.subject_id, dog.subject_name, \
                dog.proportion, dog.quantity, dog.data_source, ops.asset_scale FROM \
                (SELECT matched_id, source_id FROM base_public.id_match \
                WHERE id_type = 2 AND is_used = 1 and data_source = '020001') as idm \
                JOIN crawl_public.d_org_position as dog \
                ON idm.source_id = dog.org_id \
                LEFT JOIN (SELECT DISTINCT org_id,statistic_date,asset_scale \
								FROM crawl_public.d_org_portfolio_industry) as ops \
                ON ops.statistic_date = dog.statistic_date \
                AND dog.org_id = ops.org_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "quantity": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "proportion": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "asset_scale": lambda x: float(x) / 10e7 if type(x) is str else x,
        })

        vm2 = transform.ValueMap({
            "quantity": lambda x: float(x) if type(x) is str else x,
            "proportion": lambda x: float(x) / 100 if type(x) is str else x,
        })

        vm3 = transform.ValueMap({
            "scale": (lambda x, y: x * y, "asset_scale", "proportion"),
        })

        vm4 = transform.ValueMap({
            "proportion": lambda x: round(x, 6) if type(x) is float else x,
            "scale": lambda x: round(x, 6) if type(x) is float else x,
            "asset_scale": lambda x: round(x, 6) if type(x) is float else x
        })

        sk = transform.MapSelectKeys({
            "matched_id": "org_id",
            'subject_id': 'subject_id',
            'subject_name': 'subject_name',
            'data_source': 'data_source',
            'statistic_date': 'statistic_date',
            'quantity': 'quantity',
            'asset_scale': 'asset_scale',
            'scale': 'scale',
            'proportion': 'proportion'
        })
        s = Stream(inp, transform=[vm, vm2, vm3, vm4, sk])
        return s


    @classmethod
    def confluence(cls):
        streams = [cls.stream_020001()]
        c = Confluence(*streams, on=["org_id", "statistic_date", "subject_id"])
        return c

    @classmethod
    def org_name(cls):
        sql = "UPDATE base_public.org_position_stock AS fh , \
        (SELECT org_id, org_name from base_public.org_info) AS fi \
        SET fh.org_name = fi.org_name \
        WHERE fh.org_id = fi.org_id"
        ENGINE_RD.execute(sql)

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base_public.org_position_stock", ENGINE_RD, df.dataframe, type="update")
        cls.org_name()


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.org_position_stock_test", "base_public.org_position_stock", ENGINE_RD,
                            cols_excluded={'org_id', 'statistic_date', 'org_name', 'entry_time',
                                           'update_time', 'data_source'})
    t.result.to_csv(os.path.expanduser("~/Desktop/org_position_stock_{tm}.csv".format
                                       (tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()



