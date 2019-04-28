import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput
from SCRIPT.MUTUAL.etl.fund_info import nvfreq, recommendation_start, \
    fund_manager, fund_manager_nominal, fund_type


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
                              '限大额': '限额申购',
                              '限大额（单账户累计限额5万元）': '限额申购',
                              '暂停申购（单日投资上限1000万元）': '暂停申购',
                              '暂停申购（单日投资上限100万元）': '暂停申购',
                              '暂停申购（单日投资上限10万元）': '暂停申购',
                              '暂停申购（单日投资上限50万元）': '暂停申购',
                              '暂停申购（单日投资上限5万元）': '暂停申购',
                              '暂停申购（单日投资上限30万元）': '暂停申购'}

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

        sql = "SELECT idm.fund_id , dfi2.fund_name ,dfi2.fund_full_name, dfi2.data_source,  \
                dfi2.foundation_date, dfi2.fund_status, dfi2.purchase_status, dfi2.redemption_status,   \
                dfi2.aip_status, dfi2.recommendation_start, dfi2.recommendation_end, dfi2.init_raise  \
                FROM (SELECT matched_id as fund_id from base_public.id_match  \
                 WHERE id_type =1 AND is_used =1 AND data_source = '020001') AS idm \
                JOIN \
                (SELECT mdfi.fund_id , dfi.fund_name ,dfi.fund_full_name, dfi.data_source,  \
                dfi.foundation_date, dfi.fund_status, dfi.purchase_status, dfi.redemption_status, \
                dfi.aip_status,	dfi.recommendation_start, dfi.recommendation_end, dfi.init_raise   \
                FROM \
                (SELECT MAX(version) as mm,fund_id FROM crawl_public.d_fund_info GROUP BY fund_id) AS mdfi \
                JOIN \
                crawl_public.d_fund_info AS dfi \
                ON mdfi.fund_id = dfi.fund_id  \
                and mdfi.mm = dfi.version  \
                WHERE dfi.data_source = '020001') as dfi2 \
                ON idm.fund_id = dfi2.fund_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "fund_status": lambda x: sub_wrong_to_none(x),
            "purchase_status": lambda x: cls.PURCHASE_STATUS_020001.get(x),
            "redemption_status": lambda x: cls.REDEMPTION_STATUS_020001.get(x),
            "init_raise": lambda x: sub_wrong_to_none(x)
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
            'init_raise': 'init_raise'

        })

        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def stream_020002(cls):
        """
            清洗公募天天 fund_info;

        """

        sql = "\
              SELECT idm.fund_id , dfi2.fund_name ,dfi2.fund_full_name, dfi2.data_source, dfi2.purchase_range,  \
                dfi2.foundation_date, dfi2.fund_status, dfi2.purchase_status, dfi2.redemption_status,   \
                dfi2.aip_status, dfi2.recommendation_start, dfi2.recommendation_end, dfi2.init_raise  \
                FROM (SELECT matched_id as fund_id from base_public.id_match  \
                WHERE id_type =1 AND is_used =1 AND data_source = '020002') AS idm  \
                JOIN \
                (SELECT mdfi.fund_id , dfi.fund_name ,dfi.fund_full_name, dfi.data_source,dfi.purchase_range,  \
                dfi.foundation_date, dfi.fund_status, dfi.purchase_status, dfi.redemption_status, \
                dfi.aip_status,	dfi.recommendation_start, dfi.recommendation_end, dfi.init_raise \
                FROM \
                (SELECT MAX(version) as mm,fund_id FROM crawl_public.d_fund_info GROUP BY fund_id) AS mdfi \
                JOIN \
                crawl_public.d_fund_info AS dfi \
                ON mdfi.fund_id = dfi.fund_id  \
                and mdfi.mm = dfi.version \
                WHERE dfi.data_source = '020002') as dfi2 \
                ON idm.fund_id = dfi2.fund_id "

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

        vm3 = transform.ValueMap({
            "purchase_range": lambda x: x + '万' if type(x) is str and x[-1] != '万' else x
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
        s = Stream(inp, transform=[vm, vm2, vm3, sk])
        return s

    @classmethod
    def stream_020003(cls):
        """
            清洗公募数米 fund_info;

        """

        sql = "SELECT idm.fund_id , dfi2.fund_name ,dfi2.fund_full_name, dfi2.data_source,  \
                dfi2.foundation_date, dfi2.fund_status, dfi2.purchase_status, dfi2.redemption_status,   \
                dfi2.aip_status, dfi2.recommendation_start, dfi2.recommendation_end, dfi2.init_raise  \
                FROM (SELECT matched_id as fund_id from base_public.id_match  \
                 WHERE id_type =1 AND is_used =1 AND data_source = '020003') AS idm \
                JOIN \
                (SELECT mdfi.fund_id , dfi.fund_name ,dfi.fund_full_name, dfi.data_source,  \
                dfi.foundation_date, dfi.fund_status, dfi.purchase_status, dfi.redemption_status, \
                dfi.aip_status,	dfi.recommendation_start, dfi.recommendation_end, dfi.init_raise   \
                FROM \
                (SELECT MAX(version) as mm,fund_id FROM crawl_public.d_fund_info GROUP BY fund_id) AS mdfi \
                JOIN \
                crawl_public.d_fund_info AS dfi \
                ON mdfi.fund_id = dfi.fund_id  \
                and mdfi.mm = dfi.version  \
                WHERE dfi.data_source = '020003') as dfi2 \
                ON idm.fund_id = dfi2.fund_id"

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
            # 'recommendation_start': 'recommendation_start',
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
    def confluence_fund_status(cls):
        sql = "SELECT matched_id FROM base_public.id_match  \
                WHERE data_source ='020003' and is_used =1 and id_type=1 \
                AND matched_id NOT IN (SELECT DISTINCT matched_id  \
                FROM base_public.id_match WHERE data_source  in ('020001','020002') and is_used =1)"

        inp = MysqlInput(ENGINE_RD, sql)
        a = inp.dataframe

        if a.empty:
            c = None
            return c
        else:
            vm = transform.ValueMap({
                "fund_status": "终止"
            })
            sk = transform.MapSelectKeys({
                "fund_status": "fund_status",
                'matched_id': 'fund_id',

            })
            s = Stream(inp, transform=[vm, sk])
            return s


def main():
    c = StreamsMain.confluence()
    io.to_sql("base_public.fund_info", ENGINE_RD, c.dataframe.drop(["source_id"], axis=1), type='update')
    c2 = StreamsMain.confluence_fund_status()
    if c2 is None:
        print("020003 独有的基金为空")
    else:
        bb = c2.flow()[0]
        io.to_sql("base_public.fund_info", ENGINE_RD, bb, type='update')
    nvfreq.main()
    recommendation_start.main()
    fund_manager_nominal.main()
    fund_manager.main()
    fund_type.main()




def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.fund_info_test", "base_public.fund_info", ENGINE_RD,
                            cols_excluded={"fund_id", "data_source", "entry_time", "fund_custodian", "fund_manager",
                                           "fund_manager_nominal", "fund_type", "source_id", "update_time"})
    t.result.to_csv(os.path.expanduser("~/Desktop/fund_info_{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


if __name__ == "__main__":
    main()




