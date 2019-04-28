import calendar as cld
import datetime as dt
import pandas as pd
from utils.database import config as cfg, sqlfactory as sf, io
from utils.decofactory.common import inscache
from utils.algorithm import timeutils as tu
from dateutil.relativedelta import relativedelta
from multiprocessing import Pool
from functools import partial
import numpy as np

engine_rd = cfg.load_engine()["2Gb"]


CALDATE = dt.date.today()


class OrgIndex:
    class Index:
        idx_names_strategy_map = {
            "OI01": ("投顾综合指数", None),
            "OI03": ("投顾FOF指数", 6010702),
            "OI04": ("投顾股票多头指数", 6010101),
            "OI05": ("投顾股票多空指数", 6010102),
            "OI06": ("投顾市场中性指数", 6010103),
            "OI07": ("投顾债券基金指数", 60105),
            "OI08": ("投顾管理期货指数", 60102),
            "OI09": ("投顾宏观策略指数", 60106),
            "OI10": ("投顾事件驱动指数", 60104),
            "OI11": ("投顾相对价值指数", 60103),
            "OI12": ("投顾组合策略指数", 60107),
            "OI13": ("投顾多策略指数", 60108),
        }

        def __init__(self, idx):
            self.idx = idx
            self.name = OrgIndex.Index.idx_names_strategy_map[self.idx][0]
            self.code = OrgIndex.Index.idx_names_strategy_map[self.idx][1]

        def __repr__(self):
            return "{idx_name}({idx})".format(idx_name=self.name, idx=self.idx)

    def __init__(self, freq, engine_rd, engine_wt):
        """"""
        self.index = OrgIndex.Index
        self.ois = ["OI01", "OI03", "OI04", "OI05", "OI06", "OI07", "OI08", "OI09", "OI10", "OI11", "OI12", "OI13"]
        self.basedate_table = {
            "w": "org_weekly_index_static",
            "m": "org_monthly_index_static"
        }
        self.freq = freq
        self.engine_rd = engine_rd
        self.engine_wt = engine_wt
        self.data = {}

    @inscache("cached")
    def types(self):
        """
        返回每个投顾策略下对应的基金id;

        Returns:
            dict<org_index: {fund_id1, fund_id2, ...,}>

        """

        sql_types = "SELECT fund_id as fund_id, type_code as type_code, stype_code as stype_code FROM fund_type_mapping WHERE typestandard_code = 601 AND flag = 1"
        df_types = pd.read_sql(sql_types, engine_rd)
        df_types["stype_code"] = df_types["stype_code"].apply(lambda x: int(x) if ~np.isnan(x) else None)

        d_type = {}
        for oi in self.ois:
            if oi != "OI01":
                d_type[oi] = set(
                    df_types.loc[(df_types["type_code"] == self.Index(oi).code) |
                                 (df_types["stype_code"] == self.Index(oi).code)]["fund_id"]
                )

        return d_type

    ####
    @inscache("cached", True)
    def _fetch_fom_old(self, compare_date=dt.date.today()):
        """"""

        # 查询投顾, 旗下产品, 产品最早净值日期并且未设定基期(不存在于投顾指数表中的投顾?)
        sql_org3fund = "SELECT org_id, fom.fund_id, msd FROM fund_org_mapping fom \
                       JOIN (SELECT fund_id, MIN(statistic_date) msd FROM fund_nv_data_standard WHERE swanav IS NOT NULL GROUP BY fund_id) T_msd \
                       ON fom.fund_id = T_msd.fund_id \
                       WHERE org_type_code = 1"

        # 计算各产品成立日期
        df = pd.read_sql(sql_org3fund, self.engine_rd).sort_values(by=["org_id", "msd"], ascending=[True, True])
        df["founded_months"] = df["msd"].apply(lambda x: relativedelta(compare_date, x))
        df["founded_months"] = df["founded_months"].apply(lambda x: x.years * 12 + x.months)
        return df

    def sample_check_old(self, oi, compare_date=dt.date.today()):
        """
            返回所选策略下, 符合采样要求(有三只及以上的成立满一个月基金的投顾)的样本;

        Args:
            oi:
            compare_date:

        Returns:

        """

        df_tmp = self._fetch_fom_old(compare_date).copy(False)

        # 过滤策略
        if oi != "OI01":
            df_tmp = df_tmp.loc[df_tmp["fund_id"].apply(lambda x: x in self.types()[oi])]

        # 产品成立满一个月
        df_tmp = df_tmp.loc[df_tmp["founded_months"] >= 1]

        # 选出有三个以上满一个月产品的投顾
        org_fnum = df_tmp.groupby("org_id")["founded_months"].count()
        org_id_used = set(org_fnum[org_fnum >= 3].index)
        df_tmp = df_tmp.loc[df_tmp["org_id"].apply(lambda x: x in org_id_used)]

        df_tmp = df_tmp.sort_values(["org_id", "founded_months"], ascending=[True, False])
        df_tmp.index = range(len(df_tmp))
        return df_tmp
    ####

    @inscache("cached", True)
    def _fetch_fom(self):
        sql_fom = "SELECT org_id, fom.fund_id, msd FROM fund_org_mapping fom \
                       JOIN (SELECT fund_id, MIN(statistic_date) msd FROM fund_nv_data_standard WHERE swanav IS NOT NULL GROUP BY fund_id) T_msd \
                       ON fom.fund_id = T_msd.fund_id \
                       WHERE org_type_code = 1"

        df = pd.read_sql(sql_fom, self.engine_rd).sort_values(by=["org_id", "msd"], ascending=[True, True])
        return df

    @inscache("cached", True)
    def sample_check(self, oi, check_all=False):
        if self.freq == "w":
            f = partial(self.nearest_weekday, weekday=4)
        elif self.freq == "m":
            f = self.nearest_monthlday

        df_tmp = self._fetch_fom().copy()
        if oi != "OI01":
            df_tmp = df_tmp.loc[df_tmp["fund_id"].apply(lambda x: x in self.types()[oi])]

        if check_all is False:
            oids_existed = set(self._initialized_basedate(oi)["org_id"])
            df_tmp = df_tmp.loc[df_tmp["org_id"].apply(lambda x: x not in oids_existed)]

        df_tmp["fid_date"] = list(map(lambda x, y: (x, y), df_tmp.fund_id, df_tmp.msd))
        grouped = df_tmp.groupby("org_id")
        all_orgs = grouped["fid_date"].apply(lambda x: x.tolist())

        # time-costing
        # 寻找投顾旗下第一个有产品能计算收益率的日期(该日期已对其至周五/月末), 作为投顾指数基期
        result = {}
        for oid in all_orgs.index:  # pd.Series<index=oid, [(fid1, date1), (fid2, date2), ..., ]> asc
            # oid = "P1014990"
            basedate_min = None
            funds_with_date = all_orgs[oid]  # [(fid1, dt1), (fid2, dt2), ...,]
            print(oid, len(funds_with_date))
            for i in range(len(funds_with_date)):
                fid, sd = funds_with_date[i]
                sql = "SELECT statistic_date FROM fund_nv_data_standard WHERE fund_id = '%s'" % fid
                df_t = pd.read_sql(sql, engine_rd)
                if len(df_t) > 1:
                    idx_date = df_t["statistic_date"].tolist()[:-1]
                    df_t["statistic_date"] = df_t["statistic_date"].apply(lambda x: f(x))
                    # series_deltat = (df_t - df_t.shift(1))[1:]["statistic_date"]
                    series_deltat = list(map(lambda x, y: relativedelta(x, y), df_t["statistic_date"][1:], df_t["statistic_date"][:-1]))

                    basedate_tmp = None
                    if self.freq == "w":
                        for i in range(len(series_deltat)):
                            if series_deltat[i].days == 7 and series_deltat[i].months == 0 and series_deltat[i].years == 0:
                                basedate_tmp = idx_date[i]
                                break
                    elif self.freq == "m":
                        for i in range(len(series_deltat)):
                            if series_deltat[i].months == 1 and series_deltat[i].years == 0:
                                basedate_tmp = idx_date[i]
                                break

                    if basedate_tmp is not None:
                        basedate_min = min(basedate_min or basedate_tmp, basedate_tmp)

                    if i < len(funds_with_date) - 1:
                        # 如果由第i个产品确立的基期basedate_min小于等于后一个产品的最早净值日期, 提前break
                        if basedate_min is not None and basedate_min <= funds_with_date[i + 1][1]:
                            break
                        else:
                            continue
            result[oid] = (fid, basedate_min)
        return pd.DataFrame.from_dict(result, orient="index").reset_index().rename(columns={"index": "org_id", 0: "fund_id", 1: "msd"}).dropna()

    @inscache("cached")
    def _fetch_initialized_basedate(self):
        sql_basedate = "SELECT org_id, index_id, MIN(statistic_date) as msd FROM {tb} GROUP BY org_id, index_id".format(tb=self.basedate_table[self.freq])
        initlized_basedate = pd.read_sql(sql_basedate, self.engine_rd)
        return initlized_basedate

    def _initialized_basedate(self, oi):
        initialized_basedate = self._fetch_initialized_basedate()
        initialized_basedate = initialized_basedate.loc[initialized_basedate["index_id"] == oi][["org_id", "msd"]]
        return initialized_basedate

    def _real_basedate_old(self, oi, compare_date=dt.date.today()):
        # 计算基期, 基于投顾的第三只符合条件的基金
        real_basedate = self.sample_check_old(oi, compare_date).groupby(["org_id"]).apply(lambda x: x.iloc[2])  # 取每个org_id第三小的日期作为基期
        real_basedate = real_basedate[["org_id", "msd"]]
        real_basedate["msd"] = real_basedate["msd"].apply(
            lambda x: x + relativedelta(weeks=1 * int(self.freq == "w"), months=1 * int(self.freq == "m"))
        )
        # 对齐至周五/月末
        if self.freq == "w":
            real_basedate["msd"] = real_basedate["msd"].apply(lambda x: self.nearest_weekday(x, 4))
        if self.freq == "m":
            real_basedate["msd"] = real_basedate["msd"].apply(lambda x: self.nearest_monthlday(x))
        return real_basedate

    def _real_basedate(self, oi):
        if self.freq == "w":
            f = partial(self.nearest_weekday, weekday=4)
        elif self.freq == "m":
            f = self.nearest_monthlday

        real_basedate = self.sample_check(oi)

        # 对齐至周五/月末
        real_basedate["msd"] = real_basedate["msd"].apply(lambda x: f(x))
        return real_basedate

    @inscache("cached", True)
    def init_basedate(self, oi, precal=True):
        initialized_basedate = self._initialized_basedate(oi)
        if not precal:
            real_basedate = self._real_basedate(oi)
        else:
            real_basedate = pd.DataFrame()
        all_basedate = initialized_basedate.append(real_basedate)  # 数据库中已初始化好的基期优先, 其次是实际计算出的基期;
        all_basedate.index = range(len(all_basedate))
        all_basedate = all_basedate.drop_duplicates(subset=["org_id"], keep="first")
        return all_basedate

    @classmethod
    def _stacknv(cls, nvframe):
        """

        Args:
            nvframe<fund_id, date, nv>:

        Returns:
            pd.DataFrame(
                columns: fid1, fid2, ...,
                index: date1, date2, ...,
            )
        """
        return nvframe.groupby(["date", "fund_id"]).last()["nv"].unstack()

    @classmethod
    def _return_series(cls, nvstack, freq):
        nvstack_resp = tu.resample(nvstack, freq, use_bday=False, weekday=4, use_last=False)  # 不取last(), 计算时会使用平均值
        nvstack_resp = nvstack_resp.last()
        return nvstack_resp / nvstack_resp.shift(1) - 1

    @classmethod
    def _comprehensive_return_series(cls, rsstack):
        return rsstack.mean(axis=1).fillna(0)

    @classmethod
    def _factor_series(cls, rseries):
        return (1 + rseries).cumprod()

    def _cal_index(self, oi, fids, start_date, end_date, itervalue=None):
        idx = self.Index(oi)

        nv = self._get_nv(fids)
        rs_comprehensive = self._rs_comprehensive(nv, self.freq, start_date, end_date)
        if rs_comprehensive is None:
            return None, None

        res_dynamic = self._iterfrom(1000, rs_comprehensive, start_date)

        if itervalue is not None:
            res_static = self._iterfrom(itervalue["value"], rs_comprehensive, itervalue["date"])
        else:
            res_static = res_dynamic

        if res_dynamic is not None:
            res_dynamic["index_id"] = idx.idx
            res_dynamic["index_name"] = idx.name
            res_dynamic = res_dynamic.reset_index()
        if res_static is not None:
            res_static["index_id"] = idx.idx
            res_static["index_name"] = idx.name
            res_static = res_static.reset_index()

        return res_dynamic, res_static

    def _fetch_lastperiod(self, oid, oi):
        sql = "\
        SELECT oi.org_id, oi.statistic_date, oi.index_value \
        FROM {tb} oi \
        JOIN (SELECT org_id, MAX(statistic_date) as `date` \
        FROM {tb} WHERE org_id IN {oids} AND index_id = '{oi}' ) tb_latest \
        ON oi.org_id = tb_latest.org_id AND oi.statistic_date = tb_latest.date".format(
            tb=self.basedate_table[self.freq],
            oids=sf.SQL.values4sql(oid),
            oi=oi
        )
        return pd.read_sql(sql, self.engine_rd)

    def _cal_index_by_org(self, org_ids, oi, end_date):
        init_index = self.init_basedate(oi)
        initdate = dict(init_index.as_matrix())

        # sp = self.sample_check(oi)  #!
        sp = self._fetch_fom().copy()
        # 过滤策略
        if oi != "OI01":
            sp = sp.loc[sp["fund_id"].apply(lambda x: x in self.types()[oi])]
        sp = sp.loc[sp["org_id"].apply(lambda x: x in set(init_index["org_id"]))]

        oids_calculable = set(sp["org_id"])
        last_period = self._fetch_lastperiod(oids_calculable, oi)
        itervalue = dict(
            zip(last_period.org_id, list(zip(last_period.statistic_date, last_period.index_value)))
        )

        for oid in org_ids:
            if oid not in oids_calculable:
                continue
            fids_of_org = set(sp.loc[sp["org_id"] == oid]["fund_id"])
            try:
                res_dynamic, res_static = self._cal_index(oi, fids_of_org, initdate[oid], end_date, itervalue=itervalue.get(oid))
            except TypeError:
                res_dynamic, res_static = None, None

            if res_dynamic is not None:
                res_dynamic["org_id"] = oid
            if res_static is not None:
                res_static["org_id"] = oid
            yield res_dynamic, res_static

    def loop_by_orgs(self, org_ids, end_date):
        for oi in self.ois:
            yield self._cal_index_by_org(org_ids, oi, end_date)

    def iterindex(self, base_val, nvframe, start=None, end=None):
        rs_comprehensive = self._rs_comprehensive(nvframe, start, end)
        return self._iterfrom(base_val, rs_comprehensive, end)

    # HELPER FUNCTION
    @classmethod
    def nearest_weekday(cls, date, weekday):
        deltadays = (weekday + 7 - date.weekday()) % 7
        return date + relativedelta(days=deltadays)

    @classmethod
    def nearest_monthlday(cls, date):
        return dt.date(date.year, date.month, cld.monthrange(date.year, date.month)[1])

    @classmethod
    def _slice_by_date(cls, series, start=None, end=None):
        """
        Args:
            series: pd.Series<date>
            start:
            end:

        Returns:

        """

        cond_s = (series.index.date >= start) if start else None
        cond_e = (series.index.date <= end) if end else None
        if cond_s is None and cond_e is None:
            return series
        elif cond_s is not None and cond_e is None:
            return series.loc[cond_s]
        elif cond_s is None and cond_e is not None:
            return series.loc[cond_e]
        elif cond_s is not None and cond_e is not None:
            return series.loc[cond_s & cond_e]

    @classmethod
    def _rs_comprehensive(cls, nvframe, freq, start=None, end=None):
        """

        Args:
            base_val:
            nvframe: pd.DataFrame<fund_id, date, nv>:
            start:
                迭代计算区间首, 包括基期
            end:

        Returns:

        """
        rs = cls._return_series(cls._stacknv(nvframe), freq)
        rs = cls._slice_by_date(rs, start, end)

        if start not in rs.index.date:
            return None
        elif len(rs) == 0:
            return None
            # return pd.Series([0], index=pd.DatetimeIndex([start])), pd.Series([0], index=pd.DatetimeIndex([dt.date(2017, 1, 1)]))

        rs_comprehensive = cls._comprehensive_return_series(rs)

        sample_num = rs.notnull().sum(axis=1)  # 有收益的基金, 作为综合收益率的成分基金, 并计入样本数量
        return pd.DataFrame([rs_comprehensive, sample_num], index=["return", "sample_num"]).T

    @classmethod
    def _iterfrom(cls, baseval, return_ratio, iterfrom):
        """

        Args:
            baseval:
            return_ratio: pd.DataFrame<return, ...>
            iterfrom: date

        Returns:

        """
        if iterfrom not in return_ratio.index:
            return None

        tmpseries = cls._slice_by_date(return_ratio, iterfrom)
        if len(tmpseries) == 0:
            return None

        tmpseries.loc[iterfrom, "return"] = 0
        tmpseries["index_value"] = cls._factor_series(tmpseries["return"]) * baseval
        return tmpseries

    def _delnv(self, fids_del):
        if hasattr(self, "CACHEDNV"):
            fids_del = set(fids_del)
            self.CACHEDNV = self.CACHEDNV.drop(
                self.CACHEDNV.loc[self.CACHEDNV["fund_id"].apply(lambda x: x in fids_del)].index
            )

    def _get_nv(self, fund_ids):
        fids_need = set(fund_ids)
        sql_fundnv = "SELECT fund_id as fund_id, statistic_date as date, swanav as nv FROM fund_nv_data_standard WHERE fund_id IN {fids} AND swanav IS NOT NULL"
        if hasattr(self, "CACHEDNV"):
            fnv_cached = set(self.CACHEDNV["fund_id"])
            fids_notcached = fids_need - fnv_cached
            if len(fids_notcached) != 0:  # partially not cached
                print("using cachednv partially", len(fids_need), len(fids_notcached))

                sqlstr_fundid = sf.SQL.values4sql(fids_notcached)
                sql_fundnv = sql_fundnv.format(fids=sqlstr_fundid)
                df_nv = pd.read_sql(sql_fundnv, self.engine_rd)
                self.CACHEDNV = self.CACHEDNV.append(df_nv)
            else:
                print("all using cached nv")
                pass
        else:
            print("fetching cachednv")

            sqlstr_fundid = sf.SQL.values4sql(fids_need)
            sql_fundnv = sql_fundnv.format(fids=sqlstr_fundid)
            df_nv = pd.read_sql(sql_fundnv, self.engine_rd)
            self.CACHEDNV = df_nv

        return self.CACHEDNV.loc[self.CACHEDNV["fund_id"].apply(lambda x: x in fids_need)]


def start_cal(oids, freq):
    table = {
        "w": {
            "dynamic": "org_weekly_index", "static": "org_weekly_index_static"
        },
        "m": {
            "dynamic": "org_monthly_index", "static": "org_monthly_index_static"
        }
    }
    oi = OrgIndex(freq, engine_rd, engine_rd)
    tasks = list(oi.loop_by_orgs(oids, CALDATE))
    for task in tasks:
        for results in task:
            d, s = results
            if d is not None:
                d.reset_index(inplace=True)
                d = d.reset_index()[["sample_num", "index_value", "index_id", "index_name", "org_id", "date"]].rename(columns={"date": "statistic_date"})
                io.to_sql(table[freq]["dynamic"], engine_rd, d)

                s.reset_index(inplace=True)
                s = s.reset_index()[["sample_num", "index_value", "index_id", "index_name", "org_id", "date"]].rename(columns={"date": "statistic_date"})
                io.to_sql(table[freq]["static"], engine_rd, s)


def main(freq):
    from functools import partial

    p = Pool(4)

    sql_orgs = "SELECT DISTINCT org_id FROM fund_org_mapping WHERE org_type_code = 1"
    oids = pd.read_sql(sql_orgs, engine_rd)["org_id"].sort_values().tolist()

    # TMP EXEC
    # oids = ['P1001447', 'P1008404', 'P1003197', 'P1000902', 'P1004813', 'P1004746', 'P1001198',
    #         'P1001203', 'P1004150', 'P1014451', 'P1008451', 'P1008160', 'P1000277', 'P1028421']

    step = 1000
    tasks = [oids[i: i + step] for i in range(0, len(oids), step)]

    start_freqcal = partial(start_cal, freq=freq)
    p.map(start_freqcal, tasks)
    # p.close()
    # p.join()


def init(freq):
    p = Pool(4)
    index_ids = ["OI01", "OI03", "OI04", "OI05", "OI06", "OI07", "OI08", "OI09", "OI10", "OI11", "OI12", "OI13"]
    if freq == "w":
        p.map(init_w, index_ids)
    elif freq == "m":
        p.map(init_m, index_ids)


def init_w(index_id):
    oi = OrgIndex("w", engine_rd, engine_rd)

    # initialize basedate
    # oi._initialized_basedate("OI01")
    # oi._real_basedate("OI01")
    df = oi.init_basedate(index_id, False).copy()
    df = df.rename(columns={"msd": "statistic_date"})[["org_id", "statistic_date"]]
    df["index_id"] = oi.Index(index_id).idx
    df["index_name"] = oi.Index(index_id).name
    df["index_value"] = 1000

    io.to_sql("org_weekly_index_static", engine_rd, df)


def init_m(index_id):
    oi = OrgIndex("m", engine_rd, engine_rd)

    # initialize basedate
    # oi._initialized_basedate("OI01")
    # oi._real_basedate("OI01")
    df = oi.init_basedate(index_id, False).copy()
    df = df.rename(columns={"msd": "statistic_date"})[["org_id", "statistic_date"]]
    df["index_id"] = oi.Index(index_id).idx
    df["index_name"] = oi.Index(index_id).name
    df["index_value"] = 1000

    io.to_sql("org_monthly_index_static", engine_rd, df)


if __name__ == "__main__":
    # init("w")
    main("w")
    main("m")
