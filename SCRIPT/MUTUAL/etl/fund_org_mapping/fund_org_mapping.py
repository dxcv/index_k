import re
import pandas as pd
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    table = pd.read_sql("select org_full_name,org_id from base_public.org_info", ENGINE_RD)
    org_full_name = {key: value for key, value in zip(table["org_full_name"], table["org_id"])}

    table2 = pd.read_sql("select org_name,org_id from base_public.org_info", ENGINE_RD)
    org_name = {key: value for key, value in zip(table2["org_name"], table2["org_id"])}

    type_name = {1: "基金管理人",
                 2: "托管机构"}

    table3 = pd.read_sql("select org_id, org_type_code from base_public.org_info", ENGINE_RD)
    type_code = {key: value for key, value in zip(table3["org_id"], table3["org_type_code"])}

    @classmethod
    def stream_020001(cls):
        """
            清洗 fund_custodian;

        """
        dict_01 = {'中国银行(香港)有限公司': '02000001'}             #往这里可以添加需要的匹配关系

        dict_020001 = dict(cls.org_full_name, **dict_01)

        sql = "SELECT idh.matched_id, ff.fund_custodian, fi.fund_name FROM ( \
                SELECT matched_id,source_id FROM base_public.id_match \
                where id_type = 1 AND is_used = 1 AND data_source = '020001') as idh \
                JOIN \
                (SELECT * FROM ( \
                SELECT MAX(version) as mm, fund_id as id FROM crawl_public.d_fund_info GROUP BY fund_id) as idm \
                JOIN crawl_public.d_fund_info as df \
                ON idm.id = df.fund_id AND idm.mm = df.version \
                WHERE df.data_source = '020001') AS ff \
                ON ff.id = idh.matched_id \
                JOIN base_public.fund_info as fi \
                ON fi.fund_id = idh.matched_id \
                where idh.matched_id <> '777777'"
        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "org_id": (lambda x: dict_020001.get(x), "fund_custodian"),
        })

        vm2 = transform.ValueMap({
            "type_code": (lambda x: cls.type_code.get(x), "org_id"),
        })

        vm3 = transform.ValueMap({
            "type_name": (lambda x: cls.type_name.get(x), "type_code"),
        })

        dr = transform.Dropna(axis=0, how="any")

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            "org_id": "org_id",
            "fund_custodian": "org_name",
            "fund_name": "fund_name",
            "type_code": "type_code",
            "type_name": "type_name"
        })
        s = Stream(inp, transform=[vm, vm2, vm3, dr, sk])
        return s

    @classmethod
    def stream_020002(cls):
        """
            清洗 fund_custodian;

        """
        dict_02 = {'中金公司': '01000100',
                   '中信建投': '01000095',
                   '中国银行': '02000001',
                   '工商银行': '02000002',
                   '交通银行': '02000003',
                   '招商银行': '02000004',
                   '中信银行': '02000005',
                   '恒丰银行': '02000006',
                   '平安银行': '02000007',
                   '广发银行': '02000008',
                   '海通证券': '02000009',
                   '兴业银行': '02000010',
                   '华泰证券': '02000011',
                   '光大银行': '02000012',
                   '华夏银行': '02000013',
                   '浦发银行': '02000014',
                   '浙商银行': '02000015',
                   '招商证券': '02000016',
                   '广发证券': '02000017',
                   '国信证券': '02000018',
                   '中信证券': '02000019',
                   '北京银行': '02000020',
                   '上海银行': '02000021',
                   '南京银行': '02000022',
                   '中国民生银行': '02000023',
                   '杭州银行': '02000024',
                   '徽商银行': '02000025',
                   '宁波银行': '02000026',
                   '包商银行': '02000027',
                   '国泰君安': '02000028',
                   '建设银行': '02000029',
                   '渤海银行': '02000031',
                   '广州农商银行': '02000032',
                   '中国银河': '02000033',
                   '邮储银行': '02000034',
                   '农业银行': '02000035',
                   '兴业证券': '02000036',
                   }  # 往这里可以添加需要的匹配关系

        dict_020002 = dict(cls.org_name, **dict_02)

        sql = "SELECT idh.matched_id, ff.fund_custodian, fi.fund_name FROM ( \
                SELECT matched_id,source_id FROM base_public.id_match \
                where id_type = 1 AND is_used = 1 AND data_source = '020002') as idh \
                JOIN \
                (SELECT * FROM ( \
                SELECT MAX(version) as mm, fund_id as id FROM crawl_public.d_fund_info GROUP BY fund_id) as idm \
                JOIN crawl_public.d_fund_info as df \
                ON idm.id = df.fund_id AND idm.mm = df.version \
                WHERE df.data_source = '020002') AS ff \
                ON ff.id = idh.matched_id \
                JOIN base_public.fund_info as fi \
                ON fi.fund_id = idh.matched_id \
                where idh.matched_id <> '777777'"
        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "org_id": (lambda x: dict_020002.get(x), "fund_custodian"),
        })

        vm2 = transform.ValueMap({
            "type_code": (lambda x: cls.type_code.get(x), "org_id"),
        })

        vm3 = transform.ValueMap({
            "type_name": (lambda x: cls.type_name.get(x), "type_code"),
        })

        dr = transform.Dropna(axis=0, how="any")

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            "org_id": "org_id",
            "fund_custodian": "org_name",
            "fund_name": "fund_name",
            "type_code": "type_code",
            "type_name": "type_name"
        })
        s = Stream(inp, transform=[vm, vm2, vm3, dr, sk])
        return s

    @classmethod
    def stream_020003(cls):
        """
            清洗 fund_custodian;

        """
        sql = "SELECT idh.matched_id, ff.fund_custodian, fi.fund_name FROM ( \
                SELECT matched_id,source_id FROM base_public.id_match \
                where id_type = 1 AND is_used = 1 AND data_source = '020003') as idh \
                JOIN \
                (SELECT * FROM ( \
                SELECT MAX(version) as mm, fund_id as id FROM crawl_public.d_fund_info GROUP BY fund_id) as idm \
                JOIN crawl_public.d_fund_info as df \
                ON idm.id = df.fund_id AND idm.mm = df.version \
                WHERE df.data_source = '020003') AS ff \
                ON ff.id = idh.matched_id \
                JOIN base_public.fund_info as fi \
                ON fi.fund_id = idh.matched_id \
                where idh.matched_id <> '777777'"
        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "org_id": (lambda x: cls.org_full_name.get(x), "fund_custodian"),
        })

        vm2 = transform.ValueMap({
            "type_code": (lambda x: cls.type_code.get(x), "org_id"),
        })

        vm3 = transform.ValueMap({
            "type_name": (lambda x: cls.type_name.get(x), "type_code"),
        })

        dr = transform.Dropna(axis=0, how="any")

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            "org_id": "org_id",
            "fund_custodian": "org_name",
            "fund_name": "fund_name",
            "type_code": "type_code",
            "type_name": "type_name"
        })

        s = Stream(inp, transform=[vm, vm2, vm3, dr, sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.stream_020001(), cls.stream_020002(), cls.stream_020003()]
        c = Confluence(*streams, on=["org_id", "fund_id"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence().dataframe
        io.to_sql("base_public.fund_org_mapping", ENGINE_RD, df, type="update")


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.fund_org_mapping_test", "base_public.fund_org_mapping", ENGINE_RD,
                            cols_excluded={'org_id', 'org_name', 'entry_time', 'update_time', 'fund_id'})
    t.result.to_csv(os.path.expanduser("~/Desktop/fund_org_mapping_{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()



