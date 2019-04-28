from utils.database import io, config as cfg, sqlfactory as sf
from utils.etlkit.core import base, transform
from utils.etlkit.reader.mysqlreader import MysqlInput
import datetime as dt
import re
from dateutil.relativedelta import relativedelta


class MainStream:
    engine = cfg.load_engine()["2Gbp"]
    upt_from = dt.datetime.now() - relativedelta(hours=3)
    upt_until = dt.datetime.now()
    from multiprocessing.dummy import Pool as ThreadPool
    pool = ThreadPool(8)

    @classmethod
    def _clean_amount(cls, string):
        unit_trans = {"万": 1, "亿": 1e4}
        sre = re.search("(?P<amt>\d*(\.\d*)?)(?P<unit>万|亿).*", string)
        if sre:
            return float(sre.groupdict()["amt"]) * unit_trans.get(sre.groupdict()["unit"], 1)
        return None

    @classmethod
    def _fetch_fund_ids(cls, all=False):
        if all:
            sql = "SELECT DISTINCT fund_id FROM base_public.fund_info"
        else:
            sql = "SELECT DISTINCT matched_id FROM crawl_public.d_fund_position dfp " \
                  "JOIN base_public.id_match im ON dfp.fund_id = im.source_id AND dfp.data_source = im.data_source " \
                  "WHERE dfp.update_time >= '{upt_from}' AND dfp.update_time <= '{upt_until}'".format(
                upt_from=str(cls.upt_from), upt_until=str(cls.upt_until))

        fids = sorted([x[0] for x in cls.engine.execute(sql).fetchall()])
        return fids

    @classmethod
    def stream_020002(cls, fund_ids):
        sql = "SELECT im.matched_id, fi.fund_name, im.data_source, dfp.statistic_date, dfp.subject_id, dfp.subject_name, dfp.scale, " \
              "dfp.proportion, fas.total_asset " \
              "FROM base_public.id_match im " \
              "JOIN crawl_public.d_fund_position dfp ON im.source_id = dfp.fund_id AND im.data_source = dfp.data_source " \
              "JOIN base_public.fund_info fi ON im.matched_id = fi.fund_id " \
              "LEFT JOIN base_public.fund_asset_scale fas " \
              "ON dfp.fund_id = fas.fund_id AND dfp.statistic_date = fas.statistic_date " \
              "WHERE type = '债券' AND im.matched_id IN {fids} AND im.id_type = 1 AND im.is_used = 1 " \
              "AND im.data_source = '020002'".format(fids=sf.SQL.values4sql(fund_ids))

        inp = MysqlInput(cls.engine, sql)

        vm = transform.ValueMap({
            "proportion": lambda x: float(x.replace("%", "")) / 100,
            "scale": lambda x: cls._clean_amount(x),
        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            "fund_name": None,
            "data_source": None,
            "statistic_date": None,
            "subject_id": None,
            "subject_name": None,
            "scale": None,
            "proportion": "proportion_net",
            "total_asset": "asset_scale",
        })

        return base.Stream(inp, transform=[vm, sk])

    @classmethod
    def stream_020001(cls, fund_ids):
        sql = "SELECT im.matched_id, fi.fund_name, im.data_source, dfp.statistic_date, dfp.subject_id, dfp.subject_name, " \
              "dfp.scale, dfp.proportion, fas.total_asset " \
              "FROM base_public.id_match im " \
              "JOIN crawl_public.d_fund_position dfp " \
              "ON im.source_id = dfp.fund_id AND im.data_source = dfp.data_source " \
              "JOIN base_public.fund_info fi ON im.matched_id = fi.fund_id " \
              "left JOIN base_public.fund_asset_scale fas ON dfp.fund_id = fas.fund_id AND dfp.statistic_date = fas.statistic_date " \
              "WHERE type = '债券' AND im.matched_id IN {fids} AND im.id_type = 1 AND im.is_used = 1 " \
              "AND dfp.fund_id NOT IN (" \
              "SELECT DISTINCT fund_id FROM crawl_public.d_fund_position WHERE data_source = '020002')" \
              "AND im.data_source = '020001'".format(fids=sf.SQL.values4sql(fund_ids))

        inp = MysqlInput(cls.engine, sql)

        vm = transform.ValueMap({
            "proportion": lambda x: float(x.replace("%", "")) / 100,
            "scale": lambda x: cls._clean_amount(x),
        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            "fund_name": None,
            "data_source": None,
            "statistic_date": None,
            "subject_id": None,
            "subject_name": None,
            "scale": None,
            "proportion": "proportion_net",
            "total_asset": "asset_scale",
        })

        return base.Stream(inp, transform=[vm, sk])

    @classmethod
    def stream_020003(cls, fund_ids):
        sql = "SELECT im.matched_id, fi.fund_name, im.data_source, dfp.subject_id, dfp.subject_name, dfp.statistic_date, " \
              "dfp.scale, dfp.proportion, fas.total_asset " \
              "FROM base_public.id_match im " \
              "JOIN crawl_public.d_fund_position dfp " \
              "ON im.source_id = dfp.fund_id AND im.data_source = dfp.data_source " \
              "JOIN base_public.fund_info fi ON im.matched_id = fi.fund_id " \
              "LEFT JOIN base_public.fund_asset_scale fas ON dfp.fund_id = fas.fund_id AND dfp.statistic_date = fas.statistic_date " \
              "WHERE type = '债券' AND im.matched_id IN {fids} AND im.id_type = 1 AND im.is_used = 1 " \
              "AND dfp.fund_id NOT IN (" \
              "SELECT DISTINCT fund_id FROM crawl_public.d_fund_position WHERE data_source IN ('020002', '020001'))" \
              "AND im.data_source = '020003'".format(fids=sf.SQL.values4sql(fund_ids))


        inp = MysqlInput(cls.engine, sql)

        vm = transform.ValueMap({
            "proportion": lambda x: float(x.replace("%", "")) / 100,
            "scale": lambda x: cls._clean_amount(x),
        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            "fund_name": None,
            "data_source": None,
            "statistic_date": None,
            "subject_id": None,
            "subject_name": None,
            "scale": None,
            "proportion": "proportion_net",
            "total_asset": "asset_scale",
        })

        return base.Stream(inp, transform=[vm, sk])

    @classmethod
    def clean(cls):
        fids = cls._fetch_fund_ids(True)
        cls.save(cls.stream_020001(fids))
        cls.save(cls.stream_020003(fids))
        chunks = [fids[i: i + 100] for i in range(0, len(fids), 100)]
        [cls.pool.apply_async(cls.stream_020002, args=(id_chunk,), callback=cls.save) for id_chunk in chunks]
        cls.pool.close()
        cls.pool.join()

    @classmethod
    def save(cls, stream):
        io.to_sql("base_test.fund_position_bond_test_20180515", cls.engine, stream.flow()[0])


def main():
    MainStream.clean()


def test():
    from utils.etlkit.ext import tools
    ENGINER = cfg.load_engine()["2Gbp"]

    tc = tools.TableComparer(
        "base_test.fund_position_bond_test", "base_public.fund_position_bond", ENGINER,
        cols_included={"quantity", "scale", "asset_scale", "proportion_net"}
    )
    res = tc.result
    res.to_csv("fund_position_bond.csv", )


if __name__ == "__main__":
    main()
