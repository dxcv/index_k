import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput

ENGINE_RD = cfg.load_engine()["2Gb"]




def sub_wrong_to_none(x):
    s = re.sub("\s|-| |--|---", "", x)
    if s == "":
        return None
    else:
        return s


class StreamsMain:
    FUND_STATUS_020001 = {'封闭期': '封闭期',
                          '正常': '正常',
                          '认购期': '认购期',
                          '发行失败': '发行失败',
                          '基金终止': '终止'}

    FUND_STATUS_020002 = {'场内交易': '正常',
                          '封闭期': '封闭期',
                          '封闭期（单日投资上限10000元）': '封闭期',
                          '开放申购封闭期': '封闭期',
                          '认购期': '认购期',
                          '封闭期（单日投资上限1000元）': '封闭期',
                          '暂停申购场内交易': '正常',
                          '暂停申购暂停交易': '封闭期',
                          '限大额场内交易': '正常',
                          '封闭期（单日投资上限30万元）': '封闭期',
                          '限大额封闭期': '封闭期'}

    PURCHASE_STATUS_020001 = {'申购关闭': '暂停申购',
                              '申购打开': '开放申购',
                              '限额申购': '限额申购'}
    PURCHASE_STATUS_020002 = {'封闭期': '封闭期',
                              '开放申购': '开放申购',
                              '暂停申购': '暂停申购',
                              '暂停申购（单日投资上限1000万元）': '暂停申购(限额)',
                              '暂停申购（单日投资上限100万元）': '暂停申购(限额)',
                              '暂停申购（单日投资上限10万元）': '暂停申购(限额)',
                              '暂停申购（单日投资上限50万元）': '暂停申购(限额)',
                              '暂停申购（单日投资上限5万元）': '暂停申购(限额)',
                              '限大额': '限额申购',
                              '限大额（单账户累计限额5万元）': '限额申购(限额)'
                              }
    PURCHASE_STATUS_020003 = {'限大额': '限额申购',
                              '开放申购': '开放申购',
                              '暂停申购': '暂停申购'}
    REDEMPTION_STATUS_020001 = {'赎回关闭': '暂停赎回',
                                '赎回打开': '开放赎回'}

    @classmethod
    def stream_020001(cls):
        """
            清洗公募好买源 fund_info;

        """

        sql = "select * FROM (SELECT fund_id,fund_name,fund_full_name,data_source, \
                    foundation_date,fund_status,purchase_status,redemption_status, \
                    aip_status,	recommendation_start,recommendation_end,init_raise \
                    FROM crawl_public.d_fund_info  \
                    WHERE data_source = '020001' ORDER BY version DESC ) AS T \
                    GROUP  BY T.fund_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "fund_status": lambda x: sub_wrong_to_none(x),
            "purchase_status": lambda x: cls.PURCHASE_STATUS_020001.get(x),
            "redemption_status": lambda x: cls.REDEMPTION_STATUS_020001.get(x),
            "init_raise": lambda x: sub_wrong_to_none(x),
        })

        vm2 = transform.ValueMap({"fund_status": lambda x: cls.FUND_STATUS_020001.get(x),
                                  "init_raise": lambda x: float(re.sub("亿", "", x))
                                  })

        sk = transform.MapSelectKeys({
            "data_source": "source_id",
            'fund_id': 'fund_id',
            'fund_name': 'fund_name',
            'fund_full_name': 'fund_full_name',
            'foundation_date': 'foundation_date',
            'fund_status': 'fund_status',
            'purchase_status': 'purchase_status',
            'redemption_status': 'redemption_status',
            'aip_status': 'aip_status',
            'recommendation_start': 'recommendation_start',
            'recommendation_end': 'recommendation_end',
            'init_raise': 'init_raise',

        })

        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def stream_020002(cls):
        """
            清洗公募天天 fund_info;

        """

        sql = "select * FROM (SELECT fund_id,fund_name,fund_full_name,data_source, \
                    foundation_date,fund_status,purchase_range,purchase_status,redemption_status, \
                    aip_status,	recommendation_start,recommendation_end,init_raise \
                    FROM crawl_public.d_fund_info  \
                    WHERE data_source = '020002' ORDER BY version DESC ) AS T \
                    GROUP  BY T.fund_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "fund_status": lambda x: sub_wrong_to_none(x),
            "init_raise": lambda x: sub_wrong_to_none(x),
            "purchase_status": lambda x: cls.PURCHASE_STATUS_020002.get(x)

        })
        vm2 = transform.ValueMap({
            "fund_status": lambda x: cls.FUND_STATUS_020002.get(x),
            "init_raise": lambda x: float(re.sub("亿", "", x)),
            "purchase_range": (lambda x, y:  x if y == "限额申购" else None, "purchase_range", "purchase_status")

        })

        sk = transform.MapSelectKeys({
            "data_source": "source_id",
            'fund_id': 'fund_id',
            'fund_name': 'fund_name',
            'fund_full_name': 'fund_full_name',
            'foundation_date': 'foundation_date',
            'fund_status': 'fund_status',
            'purchase_status': 'purchase_status',
            'purchase_range': 'purchase_range',
            'redemption_status': 'redemption_status',
            'aip_status': 'aip_status',
            'recommendation_start': 'recommendation_start',
            'recommendation_end': 'recommendation_end',
            'init_raise': 'init_raise',

        })
        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def stream_020003(cls):
        """
            清洗公募数米 fund_info;

        """

        sql = "select * FROM (SELECT fund_id,fund_name,fund_full_name,data_source, \
                    foundation_date,fund_status,	purchase_status,redemption_status, \
                    aip_status,	recommendation_start,recommendation_end,init_raise \
                    FROM crawl_public.d_fund_info  \
                    WHERE data_source = '020003' ORDER BY version DESC ) AS T \
                    GROUP  BY T.fund_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "fund_status": lambda x: sub_wrong_to_none(x),
            "purchase_status": lambda x: cls.PURCHASE_STATUS_020003.get(x),
            "init_raise": lambda x: sub_wrong_to_none(x)
        })

        vm2 = transform.ValueMap({"init_raise": lambda x: float(re.sub("亿", "", x))})

        sk = transform.MapSelectKeys({
            "data_source": "source_id",
            'fund_id': 'fund_id',
            'fund_name': 'fund_name',
            'fund_full_name': 'fund_full_name',
            'foundation_date': 'foundation_date',
            'fund_status': 'fund_status',
            'purchase_status': 'purchase_status',
            'redemption_status': 'redemption_status',
            'aip_status': 'aip_status',
            'recommendation_start': 'recommendation_start',
            'recommendation_end': 'recommendation_end',
            'init_raise': 'init_raise',

        })
        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def confluence(cls):
        prio_1 = {
            0: {"purchase_status": ("source_id", "020002")},
            1: {"purchase_status": ("source_id", "020001")}
        }
        streams = [cls.stream_020001(), cls.stream_020002(), cls.stream_020003()]

        c = Confluence(*streams, on=["fund_id"], prio_l1=prio_1)
        return c

    @classmethod
    def fund_status(cls):
        sql = "SELECT DISTINCT fund_id FROM crawl_public.d_fund_info " \
              "WHERE data_source not in ('020001','020002') " \
              "AND fund_id NOT IN (SELECT DISTINCT fund_id " \
              "FROM crawl_public.d_fund_info WHERE data_source  in ('020001','020002'))"

        inp = MysqlInput(ENGINE_RD, sql)
        vm = transform.ValueMap({
            "fund_status": "终止"
        })
        sk = transform.MapSelectKeys({
            "fund_status": "fund_status",
            'fund_id': 'fund_id',

        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def confluence_2(cls):
        streams = [cls.fund_status()]
        c = Confluence(*streams, on=["fund_id"])
        return c



def main():
    c = StreamsMain.confluence()
    io.to_sql("base_public.fund_info", ENGINE_RD, c.dataframe.drop(["source_id"], axis=1), type='update')
    c2 = StreamsMain.confluence_2()
    io.to_sql("base_public.fund_info", ENGINE_RD, c2.dataframe, type='update')


if __name__ == "__main__":
    main()



