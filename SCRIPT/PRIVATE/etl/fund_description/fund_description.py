import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput

ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    @classmethod
    def sub_wrong_to_none(cls, x):
        s = re.sub("\s|--| |---|-|完|.00|<span>|</span>", "", x)
        if s == "":
            return None
        elif s in ["0", "0元", "万(人民币)", "无", ".0", "万", "万元"]:
            return None
        else:
            return s

    @classmethod
    def clean_01(cls, x):
        try:
            s = re.search('[0-9]*\.?[0-9]+', x).group()
        except AttributeError:
            s = ""
        else:
            pass
        if s == "":
            return None
        else:
            if float(s) > 2000:
                return None
            else:
                z = str(int(float(s)))
                if "美" in x:
                    return z + "万(美元）"
                elif "港" in x:
                    return z + "万(港币）"
                else:
                    return z + "万（人民币）"

    @classmethod
    def clean_02(cls, x):
        try:
            s = re.search('[0-9]*\.?[0-9]+\%', x).group()
        except AttributeError:
            s = ""
        else:
            pass
        if s == "":
            return None
        else:
            return s


    @classmethod
    def stream_020001(cls):
        """
            清洗 d_org_info;

        """

        sql = " \
                SELECT idm.matched_id, dfi.open_date, dfi.locked_time_limit, dfi.min_purchase_amount, \
                dfi.min_append_amount,dfi.fee_subscription,dfi.fee_redeem, dfi.fee_manage, \
                dfi.fee_pay, dfi.source_id \
                FROM \
                (SELECT matched_id, source_id FROM base.id_match where id_type=1 and is_used=1 AND source='020001' GROUP BY matched_id) as idm \
                JOIN \
                (SELECT MAX(version) maxversion, fund_id FROM crawl_private.d_fund_info WHERE source_id = '020001' GROUP BY fund_id) as ma \
                ON idm.source_id = ma.fund_id \
                JOIN crawl_private.d_fund_info as dfi \
                on dfi.version = ma.maxversion and dfi.fund_id = ma.fund_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "open_date": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "locked_time_limit": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "min_purchase_amount": lambda x: cls.clean_01(x) if type(x) is str else None,
            "min_append_amount": lambda x: cls.clean_01(x) if type(x) is str else x,
            "fee_subscription": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fee_redeem": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fee_manage": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fee_pay": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x


        })

        vm2 = transform.ValueMap({
            "min_append_amount_remark": (lambda x, y: ','.join([str for str in [x, y] if str not in [None]]) if type(x)
                                         or type(y) is str else None, "min_purchase_amount", "min_append_amount")

        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            'locked_time_limit': 'locked_time_limit',
            'min_purchase_amount': 'min_purchase_amount',
            "min_append_amount": "min_append_amount",
            "min_append_amount_remark": "min_append_amount_remark",
            'fee_subscription': 'fee_subscription',
            'fee_redeem': 'fee_redeem',
            'fee_manage ': 'fee_manage',
            'open_date': 'open_date',
            "fee_pay": "fee_pay",
            "source_id": 'source_id'
        })
        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def stream_020002(cls):
        """
            清洗 d_org_info;

        """

        sql = " \
                SELECT idm.matched_id, dfi.open_date, dfi.locked_time_limit, dfi.min_purchase_amount, \
                dfi.min_append_amount,dfi.fee_subscription,dfi.fee_redeem, dfi.fee_manage, \
                dfi.fee_pay, dfi.source_id  \
                FROM \
                (SELECT matched_id, source_id FROM base.id_match where id_type=1 and is_used=1 AND source='020002' GROUP BY matched_id) as idm \
                JOIN \
                (SELECT MAX(version) maxversion, fund_id FROM crawl_private.d_fund_info WHERE source_id = '020002' GROUP BY fund_id) as ma \
                ON idm.source_id = ma.fund_id \
                JOIN crawl_private.d_fund_info as dfi \
                on dfi.version = ma.maxversion and dfi.fund_id = ma.fund_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "open_date": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "locked_time_limit": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "min_purchase_amount": lambda x: cls.clean_01(x) if type(x) is str else None,
            "min_append_amount": lambda x: cls.clean_01(x) if type(x) is str else x,
            "fee_subscription": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fee_redeem": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fee_manage": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fee_pay": lambda x: cls.clean_02(x) if type(x) is str else x,


        })

        vm2 = transform.ValueMap({
            "min_append_amount_remark": (lambda x, y: ','.join([str for str in [x, y] if str not in [None]]) if type(x)
                                         or type(y) is str else None, "min_purchase_amount", "min_append_amount")

        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            'locked_time_limit': 'locked_time_limit',
            'min_purchase_amount': 'min_purchase_amount',
            "min_append_amount": "min_append_amount",
            "min_append_amount_remark": "min_append_amount_remark",
            'fee_subscription': 'fee_subscription',
            'fee_redeem': 'fee_redeem',
            'open_date': 'open_date',
            'fee_pay': 'fee_pay',
            'fee_manage': 'fee_manage',
            'source_id': 'source_id'
        })
        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def stream_020003(cls):
        """
            清洗 d_org_info;

        """

        sql = " \
                SELECT idm.matched_id, dfi.open_date, dfi.locked_time_limit, dfi.min_purchase_amount, \
                dfi.min_append_amount,dfi.fee_subscription,dfi.fee_redeem, dfi.fee_manage, \
                dfi.fee_pay, dfi.source_id \
                FROM \
                (SELECT matched_id, source_id FROM base.id_match where id_type=1 and is_used=1 AND source='020003' GROUP BY matched_id) as idm \
                JOIN \
                (SELECT MAX(version) maxversion, fund_id FROM crawl_private.d_fund_info WHERE source_id = '020003' GROUP BY fund_id) as ma \
                ON idm.source_id = ma.fund_id \
                JOIN crawl_private.d_fund_info as dfi \
                on dfi.version = ma.maxversion and dfi.fund_id = ma.fund_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "open_date": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "min_purchase_amount": lambda x: cls.clean_01(x) if type(x) is str else None,
            "min_append_amount": lambda x: cls.clean_01(x) if type(x) is str else x,
            "fee_subscription": lambda x: cls.clean_02(x) if type(x) is str else x,
            "fee_redeem": lambda x: cls.clean_02(x) if type(x) is str else x,
            "fee_pay": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,


        })

        vm2 = transform.ValueMap({
            "min_append_amount_remark": (lambda x, y: ','.join([str for str in [x, y] if str not in [None]]) if type(x)
                                         or type(y) is str else None, "min_purchase_amount", "min_append_amount")

        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            'min_purchase_amount': 'min_purchase_amount',
            "min_append_amount": "min_append_amount",
            "min_append_amount_remark": "min_append_amount_remark",
            'fee_subscription': 'fee_subscription',
            'fee_redeem': 'fee_redeem',
            'open_date': 'open_date',
            "fee_pay": "fee_pay",
            'source_id': 'source_id'
        })
        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def stream_020004(cls):
        """
            清洗 d_org_info;

        """

        sql = " \
                SELECT idm.matched_id, dfi.open_date, dfi.locked_time_limit, dfi.min_purchase_amount, \
                dfi.min_append_amount,dfi.fee_subscription,dfi.fee_redeem, dfi.fee_manage, \
                dfi.fee_pay, dfi.source_id \
                FROM \
                (SELECT matched_id, source_id FROM base.id_match where id_type=1 and is_used=1 AND source='020004' GROUP BY matched_id) as idm \
                JOIN \
                (SELECT MAX(version) maxversion, fund_id FROM crawl_private.d_fund_info WHERE source_id = '020004' GROUP BY fund_id) as ma \
                ON idm.source_id = ma.fund_id \
                JOIN crawl_private.d_fund_info as dfi \
                on dfi.version = ma.maxversion and dfi.fund_id = ma.fund_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "open_date": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "min_purchase_amount": lambda x: cls.clean_01(x) if type(x) is str else None,
            "min_append_amount": lambda x: cls.clean_01(x) if type(x) is str else x,
            "fee_subscription": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fee_redeem": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,


        })

        vm2 = transform.ValueMap({
            "min_append_amount_remark": (lambda x, y: ','.join([str for str in [x, y] if str not in [None]]) if type(x)
                                         or type(y) is str else None, "min_purchase_amount", "min_append_amount")

        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            'min_purchase_amount': 'min_purchase_amount',
            "min_append_amount": "min_append_amount",
            "min_append_amount_remark": "min_append_amount_remark",
            'fee_subscription': 'fee_subscription',
            'fee_redeem': 'fee_redeem',
            'open_date': 'open_date',
            'source_id': 'source_id'
        })
        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def stream_020005(cls):
        """
            清洗 d_org_info;

        """

        sql = " \
                SELECT idm.matched_id, dfi.open_date, dfi.locked_time_limit, dfi.min_purchase_amount, \
                dfi.min_append_amount,dfi.fee_subscription,dfi.fee_redeem, dfi.fee_manage,dfi.duration, \
                dfi.fee_pay, dfi.source_id \
                FROM \
                (SELECT matched_id, source_id FROM base.id_match where id_type=1 and is_used=1 AND source='020001' GROUP BY matched_id) as idm \
                JOIN \
                (SELECT MAX(version) maxversion, fund_id FROM crawl_private.d_fund_info WHERE source_id = '020001' GROUP BY fund_id) as ma \
                ON idm.source_id = ma.fund_id \
                JOIN crawl_private.d_fund_info as dfi \
                on dfi.version = ma.maxversion and dfi.fund_id = ma.fund_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "open_date": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "locked_time_limit": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x
        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            'locked_time_limit': 'locked_time_limit',
            'open_date': 'open_date',
            'source_id': 'source_id'
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020008(cls):
        """
            清洗 d_org_info;

        """

        sql = " \
                SELECT idm.matched_id, dfi.open_date, dfi.locked_time_limit, dfi.min_purchase_amount, \
                dfi.min_append_amount,dfi.fee_subscription,dfi.fee_redeem, dfi.fee_manage, dfi.duration, \
                dfi.fee_pay, dfi.source_id, dfi.precautious_line, dfi.stop_loss_line \
                FROM \
                (SELECT matched_id, source_id FROM base.id_match where id_type=1 and is_used=1 AND source='020008' GROUP BY matched_id) as idm \
                JOIN \
                (SELECT MAX(version) maxversion, fund_id FROM crawl_private.d_fund_info WHERE source_id = '020008' GROUP BY fund_id) as ma \
                ON idm.source_id = ma.fund_id \
                JOIN crawl_private.d_fund_info as dfi \
                on dfi.version = ma.maxversion and dfi.fund_id = ma.fund_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "min_purchase_amount": lambda x: cls.clean_01(x) if type(x) is str else None,
            "min_append_amount": lambda x: cls.clean_01(x) if type(x) is str else x,
            "fee_subscription": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fee_redeem": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fee_manage": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fee_pay": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
        })

        vm2 = transform.ValueMap({
            "min_append_amount_remark": (lambda x, y: ','.join([str for str in [x, y] if str not in [None]]) if type(x)
                                         or type(y) is str else None, "min_purchase_amount", "min_append_amount")

        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            'min_purchase_amount': 'min_purchase_amount',
            "min_append_amount": "min_append_amount",
            "min_append_amount_remark": "min_append_amount_remark",
            'fee_subscription': 'fee_subscription',
            'fee_redeem': 'fee_redeem',
            'duration': 'duration',
            'fee_pay': 'fee_pay',
            'fee_manage': 'fee_manage',
            'source_id': 'source_id',
            'precautious_line': 'precautious_line',
            'stop_loss_line': 'stop_loss_line'
        })
        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def fund_trust(cls):
        sql = "UPDATE base.fund_description AS fh , \
        (SELECT fund_id, fund_name, fee_trust from base.fund_info) AS fi \
        SET fh.fund_name = fi.fund_name, \
        fh.fee_trust = fi.fee_trust \
        WHERE fh.fund_id = fi.fund_id"
        ENGINE_RD.execute(sql)

    @classmethod
    def confluence(cls):
        p = {
            0: {
                "locked_time_limit": ("source_id", "020001"),
                "min_purchase_amount": ("source_id", "020001"),
                "min_append_amount": ("source_id", "020001"),
                "min_append_amount_remark": ("source_id", "020001"),
                "fee_subscription": ("source_id", "020001"),
                "fee_redeem": ("source_id", "020001"),
            },
            1: {
                "locked_time_limit": ("source_id", "020002"),
                "min_purchase_amount": ("source_id", "020002"),
                "min_append_amount": ("source_id", "020008"),
                "min_append_amount_remark": ("source_id", "020002"),
                "fee_subscription": ("source_id", "020002"),
                "fee_redeem": ("source_id", "020002"),
            },
            2: {
                "locked_time_limit": ("source_id", "020005"),
                "min_purchase_amount": ("source_id", "020008"),
                "min_append_amount": ("source_id", "020003"),
                "min_append_amount_remark": ("source_id", "020008"),
                "fee_subscription": ("source_id", "020008"),
                "fee_redeem": ("source_id", "020008"),
            },
            3: {
                "min_purchase_amount": ("source_id", "020003"),
                "locked_time_limit": ("source_id", "020002"),
                "min_append_amount": ("source_id", "020004"),
                "min_append_amount_remark": ("source_id", "020003"),
                "fee_subscription": ("source_id", "020003"),
                "fee_redeem": ("source_id", "020003"),
            },
            4: {
                "min_purchase_amount": ("source_id", "020004"),
                "min_append_amount_remark": ("source_id", "020004"),
                "fee_subscription": ("source_id", "020004"),
                "fee_redeem": ("source_id", "020004"),
            }
        }

        streams = [cls.stream_020001(), cls.stream_020002(), cls.stream_020008(), cls.stream_020003(),
                   cls.stream_020005(), cls.stream_020004()]

        c = Confluence(*streams, on=["fund_id"], prio_l1=p)
        return c

    @classmethod
    def write(cls):
        df = cls.confluence().dataframe.drop(["source_id"], axis=1)
        io.to_sql("base.fund_description", ENGINE_RD, df, type="update")
        cls.fund_trust()


def test():
    from utils.etlkit.ext.tools import TableComparer
    import os
    cols = {
            'duration',
            'open_date',
            'fee_trust',
            'min_purchase_amount',
            'min_append_amount',
            'min_append_amount_remark',
            'fee_subscription',
            'fee_manage',
            'fee_trust',
            }
    t = TableComparer("base.fund_description", "base.fund_info", ENGINE_RD, cols_included=cols)
    t.result.to_csv(os.path.expanduser("~/Desktop/fund_description.csv"), encoding="gbk")


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()
