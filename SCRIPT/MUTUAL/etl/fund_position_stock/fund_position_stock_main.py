import re
import numpy as np
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput
from utils.database.sqlfactory import SQL
from SCRIPT.MUTUAL.etl.fund_position_stock import quantity


ENGINE_RD = cfg.load_engine()["2Gb"]


def sub_wrong_to_none(x):
    s = re.sub("\s|-| |--|---|万股|%", "", x)
    if s == "":
        return None
    else:
        return s


def to_list(df):
    a = np.array(df)
    vv = a.tolist()
    return vv


class StreamsMain:
    from multiprocessing.dummy import Pool as ThreadPool
    pool = ThreadPool(8)

    @classmethod
    def clean_amount(cls, string):
        unit_trans = {"万": 1, "亿": 1e4}
        sre = re.search("(?P<amt>\d*(\.\d*)?)(?P<unit>万|亿).*", string)
        if sre:
            return float(sre.groupdict()["amt"]) * unit_trans.get(sre.groupdict()["unit"], 1)
        return None

    @classmethod
    def stream_020001(cls, fund_id):
        """
            清洗 d_fund_position（020001）;

        """

        sql = " \
                SELECT dp.fund_id, dp.fund_name, dp.data_source, dp.statistic_date, dp.subject_id, dp.subject_name,  \
                dp.quantity, dp.scale, dp.proportion, fa.total_asset FROM  \
                crawl_public.d_fund_position  as dp \
                JOIN base_public.fund_asset_scale AS fa \
                ON dp.fund_id = fa.fund_id and dp.statistic_date=fa.statistic_date \
                WHERE  dp.type= '股票' AND dp.data_source= '020001'"
        if fund_id is not None:
            fids = SQL.values4sql(fund_id)
            sql += "AND dp.fund_id IN {fids}".format(fids=fids)

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "quantity": lambda x: sub_wrong_to_none(x),
            "scale": lambda x: sub_wrong_to_none(x),
            "proportion": lambda x: sub_wrong_to_none(x),
            "total_asset": lambda x: float(x) if type(x) is str else x
        })

        vm1 = transform.ValueMap({
            "quantity": lambda x: cls.clean_amount(x),
            "scale": lambda x: cls.clean_amount(x)})

        vm2 = transform.ValueMap({
            "quantity": lambda x: float(x),
            "scale": lambda x: float(x),
            "proportion": lambda x: float(x)/100 if type(x) is str else x
        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "fund_name": "fund_name",
            "data_source": "data_source",
            "statistic_date": "statistic_date",
            "subject_id": "subject_id",
            "subject_name": "subject_name",
            # "quantity": "quantity",
            "scale": "scale",
            "proportion": "proportion_net",
            "total_asset": "asset_scale"

        })

        s = Stream(inp, transform=[vm, vm1, vm2, sk])
        return s

    @classmethod
    def stream_020002(cls, fund_id):
        """
            清洗 d_fund_position（020002）;

        """

        sql = " \
                SELECT dp.fund_id,dp.fund_name,dp.data_source,dp.statistic_date,dp.subject_id,dp.subject_name,  \
                dp.quantity,dp.scale,dp.proportion,fa.total_asset FROM  \
                crawl_public.d_fund_position  as dp \
                JOIN base_public.fund_asset_scale AS fa \
                ON dp.fund_id = fa.fund_id and dp.statistic_date=fa.statistic_date \
                WHERE  dp.type= '股票' AND dp.data_source= '020002'"
        if fund_id is not None:
            fids = SQL.values4sql(fund_id)
            sql += "AND dp.fund_id IN {fids}".format(fids=fids)

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "quantity": lambda x: sub_wrong_to_none(x),
            "scale": lambda x: sub_wrong_to_none(x),
            "proportion": lambda x: sub_wrong_to_none(x),
            "total_asset": lambda x: float(x) if type(x) is str else x
        })
        vm1 = transform.ValueMap({
            "quantity": lambda x: cls.clean_amount(x),
            "scale": lambda x: cls.clean_amount(x)})

        vm2 = transform.ValueMap({
            "quantity": lambda x: float(x),
            "scale": lambda x: float(x),
            "proportion": lambda x: float(x)/100 if type(x) is str else x
        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "fund_name": "fund_name",
            "data_source": "data_source",
            "statistic_date": "statistic_date",
            "subject_id": "subject_id",
            "subject_name": "subject_name",
            # "quantity": "quantity",
            "scale": "scale",
            "proportion": "proportion_net",
            "total_asset": "asset_scale"

        })

        s = Stream(inp, transform=[vm, vm1, vm2, sk])
        return s

    @classmethod
    def stream_020003(cls, fund_id):
        """
            清洗 d_fund_position（020003）;

        """

        sql = " \
                SELECT dp.fund_id,dp.fund_name,dp.data_source,dp.statistic_date,dp.subject_id,dp.subject_name,  \
                dp.quantity,dp.scale,dp.proportion,fa.total_asset FROM  \
                crawl_public.d_fund_position  as dp \
                JOIN base_public.fund_asset_scale AS fa \
                ON dp.fund_id = fa.fund_id and dp.statistic_date=fa.statistic_date \
                WHERE  dp.type= '股票' AND dp.data_source= '020003'"
        if fund_id is not None:
            fids = SQL.values4sql(fund_id)
            sql += "AND dp.fund_id IN {fids}".format(fids=fids)

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "quantity": lambda x: sub_wrong_to_none(x),
            "scale": lambda x: sub_wrong_to_none(x),
            "proportion": lambda x: sub_wrong_to_none(x),
            "total_asset": lambda x: float(x) if type(x) is str else x
        })

        vm1 = transform.ValueMap({
            "quantity": lambda x: cls.clean_amount(x),
            "scale": lambda x: cls.clean_amount(x)})

        vm2 = transform.ValueMap({
            "quantity": lambda x: float(x),
            "scale": lambda x: float(x),
            "proportion": lambda x: float(x)/100 if type(x) is str else x
        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "fund_name": "fund_name",
            "data_source": "data_source",
            "statistic_date": "statistic_date",
            "subject_id": "subject_id",
            "subject_name": "subject_name",
            # "quantity": "quantity",
            "scale": "scale",
            "proportion": "proportion_net",
            "total_asset": "asset_scale"

        })

        s = Stream(inp, transform=[vm, vm1, vm2, sk])
        return s

    @classmethod
    def fund_ids_020002(cls):
        sql = "SELECT DISTINCT matched_id FROM base_public.id_match   \
                WHERE id_type = 1 AND is_used = 1  \
                AND matched_id  IN  \
                (SELECT DISTINCT fund_id FROM crawl_public.d_fund_position WHERE data_source = '020002')"

        inp = MysqlInput(ENGINE_RD, sql)

        a = inp.dataframe
        b = a["matched_id"]
        fund_ids = to_list(b)
        return fund_ids

    @classmethod
    def fund_ids_next_020001(cls):
        sql = "SELECT DISTINCT matched_id FROM base_public.id_match   \
                WHERE id_type = 1 AND is_used = 1  \
                AND matched_id NOT IN  \
                (SELECT DISTINCT fund_id FROM crawl_public.d_fund_position WHERE data_source = '020002')"

        inp = MysqlInput(ENGINE_RD, sql)

        a = inp.dataframe
        b = a["matched_id"]
        fund_ids = to_list(b)
        return fund_ids

    @classmethod
    def fund_ids_next_020003(cls):
        sql = "SELECT DISTINCT matched_id FROM base_public.id_match   \
                WHERE id_type = 1 AND is_used = 1  \
                AND matched_id NOT IN  \
                (SELECT DISTINCT fund_id FROM crawl_public.d_fund_position WHERE data_source IN ('020001','020002'))"

        inp = MysqlInput(ENGINE_RD, sql)

        a = inp.dataframe
        b = a["matched_id"]
        fund_ids = to_list(b)
        return fund_ids

    @classmethod
    def save(cls, df):
        io.to_sql("base_public.fund_position_stock", ENGINE_RD, df.flow()[0])

    @classmethod
    def write(cls):
        fund_ids = cls.fund_ids_020002()
        chunks = [fund_ids[i: i + 100] for i in range(0, len(fund_ids), 100)]
        [cls.pool.apply_async(cls.stream_020002, args=(id_chunk,), callback=cls.save) for id_chunk in chunks]
        print('020002导入')

        fund_ids_01 = cls.fund_ids_next_020001()
        chunks020001 = [fund_ids_01[i: i + 100] for i in range(0, len(fund_ids_01), 100)]
        [cls.pool.apply_async(cls.stream_020001, args=(id_chunk,), callback=cls.save) for id_chunk in chunks020001]
        print('020001导入')

        fund_ids_03 = cls.fund_ids_next_020003()
        chunks020003 = [fund_ids_03[i: i + 100] for i in range(0, len(fund_ids_03), 100)]
        [cls.pool.apply_async(cls.stream_020003, args=(id_chunk,), callback=cls.save) for id_chunk in chunks020003]
        cls.pool.close()
        cls.pool.join()
        print('020003导入')


def main():
    StreamsMain.write()
    quantity.main()


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.fund_position_stock_test", "base_public.fund_position_stock", ENGINE_RD,
                            cols_excluded={'fund_id',	'data_source', 'entry_time', 'update_time', 'fund_name'})
    t.result.to_csv(os.path.expanduser("~/Desktop/fund_position_stock_{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


if __name__ == "__main__":
    main()

