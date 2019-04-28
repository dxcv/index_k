import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    @classmethod
    def sub_wrong_to_none(cls, x):
        s = re.sub("\s|-| |0.0%|---|%", "", x)
        if s == "":
            return None
        else:
            return s

    @classmethod
    def stream_020001(cls):
        """
            清洗 d_org_asset_scale;

        """
        asset_type = {"买入返售证券余额": "买入返售金融资产",
                "债券": "固定收益投资",
                "其他资产": "其它",
                "基金": "基金投资",
                "存托凭证": "权益投资",
                "权证投资市值": "金融衍生品投资",
                "股票": "权益投资",
                "货币市场工具": "货币市场工具",
                "资产支持证券": "固定收益投资",
                "金融衍生品": "金融衍生品投资",
                "银行存款": "现金",
                "货币资金": "现金"}

        stype = {"买入返售证券余额": "买入返售金融资产",
                 "债券": "债券",
                 "其他资产": "其它",
                 "基金": "基金",
                 "存托凭证": "存托凭证",
                 "权证投资市值": "权证",
                 "股票": "股票",
                 "货币市场工具": "货币市场工具",
                 "资产支持证券": "资产支持证券",
                 "金融衍生品": "",
                 "银行存款": "现金",
                 "货币资金": "现金"}

        sql = "select idm.matched_id, doa.org_name, doa.data_source, doa.statistic_date, \
                doa.asset_type, doa.proportion, doa.asset_scale \
                FROM crawl_public.d_org_portfolio_asset as doa \
                JOIN base_public.id_match as idm \
                ON doa.org_id = idm.source_id \
                WHERE idm.id_type = 2 AND idm.is_used = 1 AND doa.data_source = '020001' \
                AND idm.data_source = '020001'"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "proportion": lambda x: cls.sub_wrong_to_none(x),
            "asset_stype": (lambda x: stype.get(x), "asset_type"),
            "asset_scale": lambda x: float(x)/10e7
        })

        vm2 = transform.ValueMap({
            "proportion": lambda x: float(x)/100 if type(x) is str else x,
            "asset_type": lambda x: asset_type.get(x),
        })

        vm3 = transform.ValueMap({
            "scale": (lambda x, y: round(x * y, 6), "proportion", "asset_scale"),
            "proportion": lambda x: None if type(x) is float and x < 0.0000001 else x,
            "asset_scale": lambda x: round(x, 6)
        })
        vm4 = transform.ValueMap({
            "scale": lambda x: None if type(x) is float and x < 0.0000001 else x,

        })

        sk = transform.MapSelectKeys({
            "matched_id": "org_id",
            'data_source': 'data_source',
            'statistic_date': 'statistic_date',
            'scale': 'scale',
            'proportion': 'proportion',
            'asset_type': 'asset_type',
            'asset_stype': 'asset_stype',
            "asset_scale": "asset_scale"
        })
        s = Stream(inp, transform=[vm, vm2, vm3,vm4, sk])
        return s


    @classmethod
    def confluence(cls):
        streams = [cls.stream_020001()]
        c = Confluence(*streams, on=["org_id", "statistic_date", "asset_type", "asset_stype"])
        return c

    @classmethod
    def org_name(cls):
        sql = "UPDATE base_public.org_portfolio_asset AS fh , \
        (SELECT org_id, org_name from base_public.org_info) AS fi \
        SET fh.org_name = fi.org_name \
        WHERE fh.org_id = fi.org_id"
        ENGINE_RD.execute(sql)

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base_public.org_portfolio_asset", ENGINE_RD, df.dataframe, type="update")
        cls.org_name()


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.org_portfolio_asset_test", "base_public.org_portfolio_asset", ENGINE_RD,
                            cols_excluded={'org_id', 'statistic_date', 'org_name', 'entry_time', 'update_time', 'data_source'})
    t.result.to_csv(os.path.expanduser("~/Desktop/org_portfolio_asset_{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()



