import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    @classmethod
    def sub_wrong_to_none(cls, x):
        s = re.sub("\s|-| |--|---|亿元|亿", "", x)
        if s == "":
            return None
        else:
            return s

    @classmethod
    def stream_020001(cls):
        """
            清洗 d_org_asset_scale;

        """

        sql = " \
                select idm.matched_id, doa.org_name, doa.data_source, doa.statistic_date, doa.total_asset, doa.funds_num \
                FROM crawl_public.d_org_asset_scale as doa \
                JOIN base_public.id_match as idm \
                ON doa.org_id = idm.source_id \
                WHERE idm.id_type = 2 AND idm.is_used = 1 AND doa.data_source = '020001' \
                AND idm.data_source = '020001'"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "total_asset": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x
        })

        sk = transform.MapSelectKeys({
            "matched_id": "org_id",
            # 'org_name': 'org_name',
            'data_source': 'data_source',
            'statistic_date': 'statistic_date',
            'total_asset': 'total_asset',
            'funds_num': 'funds_num'
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020002(cls):
        """
            清洗 d_org_asset_scale;

        """

        sql = " \
                select idm.matched_id, doa.org_name, doa.data_source, doa.statistic_date, doa.total_asset, doa.funds_num \
                FROM crawl_public.d_org_asset_scale as doa \
                JOIN base_public.id_match as idm \
                ON doa.org_id = idm.source_id \
                WHERE idm.id_type = 2 AND idm.is_used = 1 AND doa.data_source = '020002' \
                AND idm.data_source = '020002'"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "total_asset": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x
        })

        sk = transform.MapSelectKeys({
            "matched_id": "org_id",
            # 'org_name': 'org_name',
            'data_source': 'data_source',
            'statistic_date': 'statistic_date',
            'total_asset': 'total_asset',
            'funds_num': 'funds_num'
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.stream_020001(), cls.stream_020002()]
        c = Confluence(*streams, on=["org_id", "statistic_date"])
        return c

    @classmethod
    def org_name(cls):
        sql = "UPDATE base_public.org_asset_scale AS fh , \
        (SELECT org_id, org_name from base_public.org_info) AS fi \
        SET fh.org_name = fi.org_name \
        WHERE fh.org_id = fi.org_id"
        ENGINE_RD.execute(sql)

    @classmethod
    def write(cls):
        time = {'03-31': '保留',
                '06-30': '保留',
                '09-30': '保留',
                '12-31': '保留'}
        df = cls.confluence()
        a = df.dataframe
        a["time"] = a["statistic_date"].apply(lambda x: str(x)[5:])
        a["flag"] = a["time"].apply(lambda x: time.get(x))
        dataframe = a[a["flag"] == "保留"]
        df_last = dataframe.iloc[:, :5]
        io.to_sql("base_public.org_asset_scale", ENGINE_RD, df_last, type="update")
        cls.org_name()


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.org_asset_scale_test", "base_public.org_asset_scale", ENGINE_RD,
                            cols_excluded={'org_id', 'statistic_date', 'org_name', 'entry_time', 'update_time', 'data_source'})
    t.result.to_csv(os.path.expanduser("~/Desktop/org_asset_scale_{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()



