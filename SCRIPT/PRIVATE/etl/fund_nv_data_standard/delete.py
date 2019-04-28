from utils.database import config as cfg, io, sqlfactory as sf
from utils.database.models.base_private import FundNvDataSource, FundNvDataStandard
from utils.database.models.config_private import SyncSource
from sqlalchemy.orm import sessionmaker
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
from collections import Iterable
import time

engine = cfg.load_engine()["2Gb"]

dbsession = sessionmaker()
NOW = dt.datetime.now()


# JR079110，JR003245 JR091937(2017.9.13), JR013496(2017.9.8)


def values4sql(values, usage="tuple", **kwargs):
    if isinstance(values, Iterable) and not isinstance(values, dict):
        if isinstance(values, (str, int, float)):
            values = [values]
        if len(values) > 1:
            result = str(tuple(values))
        elif len(values) == 1:
            result = "('{0}')".format(tuple(values)[0])

        if usage == "column":
            result = result[1:-1].replace("'", "`")

        if kwargs.get("operator"):
            result = kwargs.get("operator") + result

    elif isinstance(values, dict):
        value_min = values.get("min")
        value_max = values.get("max")
        if value_min is not None and value_max is not None:
            if value_min != value_max:
                result = "BETWEEN {rng_min} AND {rng_max}".format(rng_min=value_min, rng_max=value_max)
            else:
                result = "= {rng_eql}".format(rng_eql=value_min)
        elif value_min is not None and value_max is None:
            result = "> {rng_min}".format(rng_min=value_min)

        elif value_min is None and value_max is not None:
            result = "<= {rng_max}".format(rng_max=value_max)

    return result


def fetch_all_fids():
    sql_fids = "SELECT DISTINCT fund_id FROM fund_nv_data_standard_copy2"
    return [x[0] for x in engine.execute(sql_fids).fetchall()]


def fetch_wrongdate(fid):
    sql1 = "SELECT statistic_date FROM fund_nv_data_standard_copy2 WHERE fund_id = '{fid}'".format(fid=fid)
    sql2 = "SELECT DISTINCT statistic_date FROM fund_nv_data_source_copy2 WHERE fund_id = '{fid}' AND is_used = 1".format(
        fid=fid)

    df_std = pd.read_sql(sql1, engine)
    df_src = pd.read_sql(sql2, engine)

    date_std = set(df_std["statistic_date"])
    date_src = set(df_src["statistic_date"])

    wrong_date = date_std - date_src
    if len(wrong_date) > 0:
        try:
            min_date = min(date_std)
            return wrong_date, min_date
        except:
            return None, None
    else:
        return None, None


def dt2str(a):
    return dt.datetime.fromtimestamp(time.mktime(a.timetuple())).strftime("%Y%m%d")


def delete_wrong(fid, wrong_dates):
    wrong_dates = [dt2str(x) for x in wrong_dates]
    wrong_dates = values4sql(wrong_dates)
    tables = (
        "base.fund_nv_data_standard_copy2", "base.fund_nv_data_standard",
        "base.fund_weekly_return", "base.fund_weekly_risk", "base.fund_weekly_risk2",
        "base.fund_subsidiary_weekly_index", "base.fund_subsidiary_weekly_index2", "base.fund_subsidiary_weekly_index3"
    )
    for table in tables:
        sql = "DELETE FROM {tb} WHERE fund_id = '{fid}' AND statistic_date IN {wd}".format(
            tb=table, fid=fid, wd=wrong_dates
        )
        engine.execute(sql)


def mod_table(fid, min_date):
    min_date = dt2str(min_date)
    now = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    for table in ("base.fund_nv_data_standard_copy2", "base.fund_nv_data_standard"):
        sql = "UPDATE {tb} SET update_time = {n} WHERE fund_id = '{fid}' AND statistic_date = '{md}'".format(
            n=now, fid=fid, md=min_date, tb=table
        )
        engine.execute(sql)


def check_fund(fund_id):
    wd, md = fetch_wrongdate(fund_id)
    if wd is not None:
        print("checked: {fid}".format(fid=fund_id), dt.datetime.now())
        delete_wrong(fund_id, wd)
        mod_table(fund_id, md)


class NvDeleteHelper:
    TABLES_TO_DEL = [
        "base.fund_nv_data_standard_copy2", "base.fund_nv_data_standard",
        "base.fund_weekly_return", "base.fund_weekly_risk", "base.fund_weekly_risk2",
        "base.fund_subsidiary_weekly_index", "base.fund_subsidiary_weekly_index2", "base.fund_subsidiary_weekly_index3"
    ]

    # TABLES_TO_DEL = [
    #     "fund_nv_data_standard",
    # ]

    def __init__(self):
        self.sess = dbsession(bind=engine)
        self.res = pd.DataFrame()

    def fetch_nv_to_check(self):
        res = []
        sql1 = "SELECT DISTINCT pk as fund_id FROM config_private.sync_source WHERE is_used = 0"
        sql2 = "SELECT DISTINCT fund_id FROM base.{tb} WHERE is_used = 0".format(tb=FundNvDataSource.__tablename__)
        for sql in [sql1, sql2]:
            res.extend([x[0] for x in self.sess.bind.execute(sql).fetchall()])
        return sorted(set(res))

    def _fetch_all_src_of_fund(self, fund_id):
        stmt = self.sess.query(FundNvDataSource).filter(
            FundNvDataSource.fund_id == fund_id, FundNvDataSource.source_id.notin_({"03", "04"})
        ).with_entities(
            FundNvDataSource.statistic_date, FundNvDataSource.source_id, FundNvDataSource.added_nav,
            FundNvDataSource.is_used
        )
        return pd.read_sql(stmt.statement, self.sess.bind)

    def _fetch_sync_source_used(self, fund_id):
        stmt = self.sess.query(SyncSource).filter(
            SyncSource.target_table == "fund_nv_data_standard", SyncSource.pk == fund_id, SyncSource.is_used == 1
        ).with_entities(
            SyncSource.source_id
        )
        return set(x[0] for x in stmt.all())

    @classmethod
    def judge_date_to_delete(cls, df_src: pd.DataFrame, src_used: set):
        is_used_judge_by_syncsource = list(
            map(lambda is_used, src: int(is_used and (src in src_used)), df_src["is_used"], df_src["source_id"])
        )
        is_used_judge_by_addednv = df_src["added_nav"].notnull()

        df_src["is_used"] = [jg1 * jg2 for jg1, jg2 in zip(is_used_judge_by_syncsource, is_used_judge_by_addednv)]

        # 筛选出 出了不使用的数据之外, 没有其他可用数据的日期
        a = ~df_src.groupby("statistic_date")["is_used"].any()
        date = a[a].index

        return date

    @classmethod
    def generate_test_sql(cls, fund_id, dates: list):
        res = []
        if len(dates) == 0:
            return res
        dates = [str(d) for d in dates]
        cond = sf.SQL.values4sql(dates)
        sql_cond = "WHERE fund_id = '{fid}' AND statistic_date IN {cond}".format(
            fid=fund_id, cond=cond, tb="fund_nv_data_standard"
        )
        for tb in cls.TABLES_TO_DEL:
            sql = "SELECT fund_id, statistic_date FROM {tb} ".format(tb=tb) + sql_cond
            res.append(sql)
        return res

    @classmethod
    def generate_DELETE_sql(cls, fund_id, dates: list):
        res = []
        if len(dates) == 0:
            return res
        print(fund_id, len(dates), dt.datetime.now())
        dates = [str(d) for d in dates]
        cond = sf.SQL.values4sql(dates)
        sql_cond = "WHERE fund_id = '{fid}' AND statistic_date IN {cond}".format(
            fid=fund_id, cond=cond
        )
        for tb in cls.TABLES_TO_DEL:
            sql = "DELETE FROM {tb} ".format(tb=tb) + sql_cond
            res.append(sql)
        return res

    def test_delete(self, fund_id):
        df_src = self._fetch_all_src_of_fund(fund_id)
        src_used = self._fetch_sync_source_used(fund_id)
        dates_to_del = self.judge_date_to_delete(df_src, src_used)

        sql = self.generate_test_sql(fund_id, dates_to_del)
        if len(sql) == 0:
            return
        df1 = pd.read_sql(sql[0], self.sess.bind)
        self.res = self.res.append(df1)

    def DELETE(self, fund_id):
        from time import sleep
        df_src = self._fetch_all_src_of_fund(fund_id)
        src_used = self._fetch_sync_source_used(fund_id)
        dates_to_del = self.judge_date_to_delete(df_src, src_used)

        sql = self.generate_DELETE_sql(fund_id, dates_to_del)
        if len(sql) == 0:
            return
        for s in sql:
            self.sess.bind.execute(s)
            sleep(0.5)
            # DEBUG
            # ENGINE_TEST.execute(s)

    def loop(self, fund_ids=None):
        if fund_ids is None:
            fund_ids = self.fetch_nv_to_check()

        for fid in fund_ids:
            self.test_delete(fid)

    def LOOP_DELETE(self, fund_ids=None):
        if fund_ids is None:
            fund_ids = self.fetch_nv_to_check()

        for fid in fund_ids:
            print(dt.datetime.now(), fid)
            self.DELETE(fid)
            check_fund(fid)


def main():
    FN = (NOW - relativedelta(minutes=11)).strftime("%Y%m%d%H%M%S")
    NW = NOW.strftime("%Y%m%d%H%M%S")

    res = []
    sql1 = "SELECT DISTINCT pk as fund_id FROM config_private.sync_source WHERE is_used = 0 AND update_time BETWEEN '{fn}' AND '{nn}'".format(
        fn=FN, nn=NW
    )
    sql2 = "SELECT DISTINCT fund_id FROM base.{tb} WHERE is_used = 0 AND update_time BETWEEN '{fn}' AND '{nn}'".format(
        tb=FundNvDataSource.__tablename__, fn=FN, nn=NW
    )

    for sql in [sql1, sql2]:
        res.extend([x[0] for x in engine.execute(sql).fetchall()])
    res = sorted(set(res))

    d = NvDeleteHelper()
    d.LOOP_DELETE(res)


def delete_one(fid):
    d = NvDeleteHelper()
    fids = ["JR114073"]

    d.LOOP_DELETE(fids)


if __name__ == "__main__":
    main()
