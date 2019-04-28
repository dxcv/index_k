import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput

ENGINE_RD = cfg.load_engine()["2Gb"]


def sub_wrong_to_none(x):
    s = re.sub("\s|-| |--|---|1-|2-|3-|4-", "", x)
    if s == "":
        return None
    else:
        return s


class StreamsMain:

    @classmethod
    def fund_security_data0507(cls):
        """
            清洗 data_test.fund_security_data0507;

        """

        sql = "SELECT fund_id,fund_name,statistic_date,security_category,id,`name`,`sum`,ratio,`value` \
                FROM data_test.`fund_security_data0507` WHERE source_code=1"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "security_category": lambda x: sub_wrong_to_none(x),
            "source_id": "030063",
            "source": "托管机构"
        })

        sk = transform.MapSelectKeys({
            'fund_id': 'fund_id',
            'fund_name': 'fund_name',
            'statistic_date': 'statistic_date',
            'security_category': 'security_category',
            'id': 'id',
            'name': 'name',
            'sum': 'sum',
            'ratio': 'ratio',
            'value': 'value',
            'source_id': 'source_id',
            'source': 'source'
        })

        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020001(cls):
        """
            清洗d_fund_se(020001)

        """

        sql = "SELECT * FROM  \
                (SELECT * from ( \
                SELECT fund_id,fund_name,source_id,stock_id,stock_name,statistic_date,stock_ratio,stock_sum, \
                stock_change,variable_rate,change_num from crawl_private.d_fund_security \
                where source_id = '020001' ORDER BY version DESC) AS T \
                GROUP BY T.fund_id,T.stock_id,T.statistic_date) as dfs \
                LEFT JOIN \
                (SELECT matched_id,source_id as source_id2 from base.id_match where id_type=1 and source = '020001') as i \
                ON dfs.fund_id = i.source_id2 \
                WHERE i.matched_id is not NULL"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            'security_category': '股票',
            'source': '第三方',
            'source_id': '020001'

        })

        sk = transform.MapSelectKeys({
            'statistic_date': 'statistic_date',
            'matched_id': 'fund_id',
            'fund_name': 'fund_name',
            'source_id': 'source_id',
            'security_category': 'security_category',
            'stock_id': 'id',
            'stock_name': 'name',
            'stock_sum': 'sum',
            'stock_ratio': 'ratio',
            'value': 'value',
            'stock_change': 'change',
            'variable_rate': 'change_rate',
            'source': 'source'

        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.fund_security_data0507(), cls.stream_020001()]
        c = Confluence(*streams, on=['fund_id', 'statistic_date', 'id', 'source_id', 'security_category'])
        return c


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("base.fund_position","data_test.fund_security_data_test", ENGINE_RD,
                            cols_excluded={'fund_id', 'fund_name', 'entry_time', 'update_time',
                                           'source_id', 'source', 'statistic_date'})
    t.result.to_csv(os.path.expanduser("~/Desktop/fund_position_{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


def main():
    a = StreamsMain.confluence()
    df = a.dataframe
    df2 = df.reset_index(drop=True)
    io.to_sql("base.fund_position", ENGINE_RD, df2, type="update")


if __name__ == "__main__":
    main()

