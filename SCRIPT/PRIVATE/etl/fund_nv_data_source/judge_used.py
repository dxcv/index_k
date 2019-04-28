from utils.database import config as cfg, io
from utils.database.models.base_private import FundNvDataSource
from sqlalchemy.orm import sessionmaker
import pandas as pd
import time
import datetime as dt
import numpy as np
from multiprocessing import Pool

engine = cfg.load_engine()["2Gb"]

dbsession = sessionmaker()


class NvJudger:
    def __init__(self, fund_id):
        self.dbss = sessionmaker()
        self.engine = engine
        self.fund_id = fund_id
        self.srcnv = None
        self.bm = None

    def fetch_nvsrc_data(self):
        if self.srcnv is None:
            ss = self.dbss(bind=engine)
            stmt = ss.query(FundNvDataSource).filter(
                FundNvDataSource.fund_id == self.fund_id,
                FundNvDataSource.source_id.notin_({"03", "04", "05", "000001"})
            ).with_entities(
                FundNvDataSource.statistic_date, FundNvDataSource.source_id, FundNvDataSource.nav,
                FundNvDataSource.added_nav
            )
            df = pd.read_sql(stmt.statement, engine)

            gp = df.groupby(["statistic_date", "source_id"]).last().unstack()

            self.srcnv = gp

    def fetch_benchmark(self):
        if self.bm is None:
            ss = self.dbss(bind=engine)
            stmt = ss.query(FundNvDataSource).filter(
                FundNvDataSource.fund_id == self.fund_id,
                FundNvDataSource.source_id.notin_({"03", "04", "05", "000001"})
            ).with_entities(
                FundNvDataSource.statistic_date, FundNvDataSource.source_id, FundNvDataSource.is_used
            )
            df = pd.read_sql(stmt.statement, engine)

            gp = df.groupby(["statistic_date", "source_id"]).last().unstack()

            self.bm = gp

    def judge(self, func=None, DEBUG=False):
        self.fetch_nvsrc_data()
        self.fetch_benchmark()

        if func is not None:
            a = func()
        else:
            a = self.judgement
        b = self.bm.stack()["is_used"][a.index]
        self.pr_ratio(a, b, DEBUG)

    @classmethod
    def scan_delta(cls, series_nv, series_t, start=1, result=None):
        if result is None:
            result = []
        for i in range(start, len(series_nv) - 1):
            delta_t0 = (series_t[i] - series_t[i - 1]) / 86400
            r_t0 = (series_nv[i] / series_nv[i - 1] - 1) / delta_t0
            if r_t0 > 0.12:
                delta_t1 = (series_t[i + 1] - series_t[i]) / 86400
                r_t1 = (series_nv[i + 1] / series_nv[i] - 1) / delta_t1
                if r_t1 < (- 0.05):  # 折线形判断1
                    # print(series_nv[i], dt.datetime.fromtimestamp(series_t[i]))
                    result.append(series_t[i])
                    del series_nv[i]
                    del series_t[i]
                    return cls.scan_delta(series_nv, series_t, i, result)
            elif r_t0 < (- 0.05):
                delta_t1 = (series_t[i + 1] - series_t[i]) / 86400
                r_t1 = (series_nv[i + 1] / series_nv[i] - 1) / delta_t1
                if r_t1 > 0.12:  # 折线形判断2
                    # print(series_nv[i], dt.datetime.fromtimestamp(series_t[i]))
                    result.append(series_t[i])
                    del series_nv[i]
                    del series_t[i]
                    return cls.scan_delta(series_nv, series_t, i, result)

        return result

    def judge_by_mode(self, by="nav"):
        q = self.srcnv[by]

        if len(q) == 0:
            return None

        def custom_mode(arr):
            res = set(pd.Series.mode(arr))
            num_validval = sum(arr.notnull())
            if num_validval < 2:
                return res
            else:
                # res = set(pd.Series.mode(arr))
                if num_validval > 2:
                    return res
                else:
                    if len(res) == 2:
                        return set()
                    else:
                        return res

        def custom_score(series, d_mode):
            res = []
            for idx_date, nv_val in zip(series.index, series.values):
                if len(d_mode[idx_date]) > 0:
                    score = int(nv_val in d_mode[idx_date])
                else:
                    if np.isnan(nv_val):
                        score = 0
                    else:
                        score = 1
                res.append(score)
            return res

        s = q.apply(custom_mode, axis=1)
        d = dict(s)

        # r = q.apply(lambda x: [y in d[idx] if (len(d[idx]) > 0) else tolerant for idx, y in zip(x.index, x.values)])
        r = q.apply(lambda x: custom_score(x, d))

        r_reverse = r[::-1]
        r0 = r.rolling(1).mean() * 0.7
        r1 = r.rolling(2).mean() * 0.15
        r2 = r.rolling(5).mean() * 0.08
        r3 = r.rolling(10).mean() * 0.045
        r4 = r.rolling(20).mean() * 0.025
        r0_ = r_reverse.rolling(1).mean()[::-1] * 0.7
        r1_ = r_reverse.rolling(2).mean()[::-1] * 0.15
        r2_ = r_reverse.rolling(5).mean()[::-1] * 0.08
        r3_ = r_reverse.rolling(10).mean()[::-1] * 0.045
        r4_ = r_reverse.rolling(20).mean()[::-1] * 0.025

        r0 = r0.fillna(r0_)
        r0_ = r0_.fillna(r0)

        r1 = r1.fillna(r1_)
        r1_ = r1_.fillna(r1)

        r2 = r2.fillna(r2_)
        r2_ = r2_.fillna(r2)

        r3 = r3.fillna(r3_)
        r3_ = r3_.fillna(r3)

        r4 = r4.fillna(r4_)
        r4_ = r4_.fillna(r4)

        rs = r0.add(r1, fill_value=0.15).add(r2, fill_value=0.08).add(r3, fill_value=0.045).add(r4, fill_value=0.025).\
                 add(r0_).add(r1_, fill_value=0.15).add(r2_, fill_value=0.08).add(r3_, fill_value=0.045).add(r4_, fill_value=0.025) / 2
        u = rs >= 0.8

        a = u.stack()[q.stack().index].unstack()
        a = a.applymap(lambda x: int(x) if x is not None else None)
        a = a.stack()
        return a

    def judge_by_volatility(self, pre=None):
        """

        Returns:
            dict<source_id: dates>
            source_id: str
            dates: list<datetime.date>
        """
        if len(self.srcnv) == 0:
            return None
        nv = self.srcnv["nav"]

        if pre is not None:
            nv = nv.stack()[pre[pre == 1].index].unstack()

        result = {}

        for sid in nv.columns:
            q = nv[sid].copy()
            q = q.dropna()
            ns = q.tolist()
            ts = q.index.to_series().apply(lambda x: time.mktime(x.timetuple())).tolist()

            tmp = self.scan_delta(ns, ts)
            if len(tmp) > 0:
                wr = [dt.datetime.fromtimestamp(x).date() for x in tmp]
                result[sid] = wr

        return result

    @property
    def judgement(self):
        try:
            self.fetch_nvsrc_data()
            if self.srcnv is None or len(self.srcnv) == 0:
                return None
            nv = self.srcnv["nav"].stack()

            j1 = self.judge_by_mode("nav")
            j2 = self.judge_by_volatility(pre=j1)

            if j2 is not None:
                for sid, dates in j2.items():
                    for date in dates:
                        is_also_wrong = (nv[date] == nv[(date, sid)])
                        wrong_srcs = is_also_wrong[is_also_wrong]
                        for wrong_src in wrong_srcs.index:
                            j1[(date, wrong_src)] = 0

            # nv = self.srcnv["nav"]
            # idx_judge_wrong = nv[j1[j1 == 0].index]
            return j1
        except:
            print(self.srcnv)
            return None

    def pr_ratio(self, a, b, DEBUG=False):
        assert (len(a) == len(b))
        sp_num = len(a)
        right_pred = a[a == b]
        wrong_pred = a[a != b]

        tp = int(sum(right_pred == 1))  # 预测正确, 且可以为可以使用
        tn = int(sum(right_pred == 0))  # 预测正确, 且标记为不可使用
        fp = int(sum(wrong_pred == 1))  # 预测错误, 且标记为可以使用(即不应该被使用, 却被判为使用) *****
        fn = int(sum(wrong_pred == 0))  # 预测错误, 且标记为不可使用(即应该被使用, 却误判为不使用) ***

        rt = tp + tn
        wr = fp + fn

        rt_ratio = round(rt / sp_num, 4) * 100
        wr_ratio = round(wr / sp_num, 4) * 100

        tp_ratio = round(tp / sp_num, 4) * 100
        tn_ratio = round(tn / sp_num, 4) * 100
        fp_ratio = round(fp / sp_num, 4) * 100
        fn_ratio = round(fn / sp_num, 4) * 100

        result = "num: {num}, right: {rt}({rt_ratio}%), wrong: {wr}({wr_ratio}%)\n" \
                 "tp: {tp}({tp_ratio}%), tn: {tn}({tn_ratio}%),\n" \
                 "***fp: {fp}({fp_ratio}%), **fn: {fn}({fn_ratio}%)".format(
            tp=tp, tn=tn, fp=fp, fn=fn, num=sp_num,
            tp_ratio=tp_ratio, tn_ratio=tn_ratio, fp_ratio=fp_ratio, fn_ratio=fn_ratio,
            rt=rt, wr=wr, rt_ratio=rt_ratio, wr_ratio=wr_ratio
        )

        print(self.fund_id)
        print(result, "\n")
        if DEBUG:
            print(wrong_pred[wrong_pred == 1], "\n")
            print(wrong_pred[wrong_pred == 0], "\n")
            print("\n\n")


def update_is_used(fid):
    print(fid)
    cols = ["statistic_date", "source_id", "is_used", ]
    n = NvJudger(fid)
    tmp = n.judgement
    if tmp is not None:
        tmp = tmp.reset_index()
        tmp.columns = cols
        tmp["fund_id"] = fid
        io.to_sql("base.fund_nv_data_source_copy2", engine, tmp)
    else:
        print("WRONG ID: ", fid)


def test():
    import time
    import datetime as dt

    # JR000001 20150211, 20150325, 20150331
    # JR148916
    # JR122709
    ## ID TO TEST
    # JR106815

    n = NvJudger("JR082492")
    n.judge(DEBUG=True)
    a = n.judge_by_mode()
    b = n.judge_by_volatility()
    c = n.judgement

    update_is_used("JR003844")

    for fid in ["JR083191", "JR000001", "JR122709", "JR148916", "JR114133", ]:
        n = NvJudger(fid)
        n.judge(DEBUG=False)
        n.judge(DEBUG=True)


def main():
    p = Pool(4)

    sql = "SELECT DISTINCT fund_id FROM fund_nv_data_source_copy2 " \
          "WHERE fund_id NOT IN (SELECT DISTINCT fund_id FROM fund_nv_data_source_copy2 WHERE nav != added_nav)"
    fids = sorted(pd.read_sql(sql, engine)["fund_id"])
    ids_to_update = [fids[i] for i in range(10000) if fids[i] != "JR003824"]

    p.map(update_is_used, ids_to_update)
    p.close()
    p.join()


if __name__ == "__main__":
    pass
