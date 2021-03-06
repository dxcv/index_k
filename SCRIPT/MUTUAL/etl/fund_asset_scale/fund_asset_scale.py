from utils.database import config as cfg, io
from utils.etlkit.core import base
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput
import re
import datetime as dt


class StreamMain:
    engine = cfg.load_engine()["2Gb"]
    UPT_SINCE = dt.datetime.now() - dt.timedelta(365)
    BASE_SQL = "SELECT fi.fund_id, fi.fund_name, statistic_date, purchase_amount, redemption_amount, total_asset, total_share " \
               "FROM crawl_public.d_fund_asset_scale fas " \
               "JOIN base_public.id_match im ON fas.fund_id = im.source_id AND fas.data_source = im.data_source " \
               "JOIN base_public.fund_info fi ON fi.fund_id = im.matched_id " \
               "WHERE im.id_type = 1 AND im.data_source = '{sid}' AND im.is_used = 1 " \
               "AND fas.fund_id IN (SELECT DISTINCT fund_id FROM crawl_public.d_fund_asset_scale " \
               "WHERE update_time >= '{upt}')"

    @classmethod
    def parse(cls, string):
        if type(string) is not str:
            return None
        string = re.sub(",", "", string)
        num, unit = re.search("(\d*\.?\d*)(亿|万)?", string).groups()
        unit_trans = {
            "万": 1e4,
            "亿": 1,
        }
        try:
            num = float(num) / unit_trans.get(unit, 1)  # 转化为`亿`
        except:
            return None
        return num

    @classmethod
    def stream_020001(cls):
        sql = cls.BASE_SQL.format(sid='020001', upt=cls.UPT_SINCE)
        inp = MysqlInput(cls.engine, sql)

        vm = transform.ValueMap({
            "total_share": lambda x: cls.parse(x),
            "total_asset": lambda x: cls.parse(x),
            "purchase_amount": lambda x: cls.parse(x),
            "redemption_amount": lambda x: cls.parse(x),
        })
        sk = transform.MapSelectKeys({
            "fund_id": None,
            "fund_name": None,
            "statistic_date": None,
            "total_share": None,
            "total_asset": None,
            "purchase_amount": None,
            "redemption_amount": None,
        })
        s = base.Stream(inp, transform=[vm, sk])

        return s

    @classmethod
    def stream_020002(cls):
        sql = cls.BASE_SQL.format(sid='020002', upt=cls.UPT_SINCE)
        inp = MysqlInput(cls.engine, sql)

        vm = transform.ValueMap({
            "total_share": lambda x: cls.parse(x),
            "total_asset": lambda x: cls.parse(x),
            "purchase_amount": lambda x: cls.parse(x),
            "redemption_amount": lambda x: cls.parse(x),
        })
        sk = transform.MapSelectKeys({
            "fund_id": None,
            "fund_name": None,
            "statistic_date": None,
            "total_share": None,
            "total_asset": None,
            "purchase_amount": None,
            "redemption_amount": None,
        })
        s = base.Stream(inp, transform=[vm, sk])

        return s

    @classmethod
    def conflu(cls):
        streams = [cls.stream_020002(), cls.stream_020001()]
        return base.Confluence(*streams, on=["fund_id", "statistic_date"])

    @classmethod
    def clean(cls):
        a = cls.conflu().dataframe
        time = {'03-31': '保留',
                '06-30': '保留',
                '09-30': '保留',
                '12-31': '保留'}
        a["time"] = a["statistic_date"].apply(lambda x: str(x)[5:])
        a["flag"] = a["time"].apply(lambda x: time.get(x))
        dataframe = a[a["flag"] == "保留"]
        df_last = dataframe.iloc[:, :5]
        io.to_sql("base_public.fund_asset_scale", cls.engine, df_last, type="update")


def test():
    from utils.etlkit.ext.tools import TableComparer
    import os
    cols = {"purchase_amount", "redemption_amount", "total_share", "total_asset"}
    t = TableComparer("base_test.fund_asset_scale_test_20180515", "base_public.fund_asset_scale", StreamMain.engine, cols_included=cols)
    t.result.to_csv(os.path.expanduser("~/Desktop/fund_asset_scale.csv"), encoding="gbk")


def main():
    StreamMain.clean()


if __name__ == "__main__":
    main()
