from utils.database import config as cfg, io
import numpy as np
import pandas as pd

engine = cfg.load_engine()["2Gbp"]


class AdjustedNvCalculator:
    COL_DATE, COL_NV, COL_VAL = "date", "nav", "value"
    COL_D = COL_VAL + "_d"
    COL_S = COL_VAL + "_s"

    def __init__(self, nv, dividend=None, split=None):
        """

        Args:
            nv: pandas.DataFrame<date, nav>
            dividend: pandas.DataFrame<date, value>
            split: pandas.DataFrame<date, value>
        """
        if dividend is None:
            self.dividend = pd.DataFrame(columns=[self.COL_DATE, self.COL_VAL])
        if split is None:
            self.split = pd.DataFrame(columns=[self.COL_DATE, self.COL_DATE])

        self.nv = nv
        self.dividend = dividend
        self.split = split

    def _checkdate(self):
        if not set(self.dividend[self.COL_DATE]).issubset(set(self.nv[self.COL_DATE])):
            raise ValueError("D")
        if not set(self.split[self.COL_DATE]).issubset(set(self.nv[self.COL_DATE])):
            raise ValueError("S")

    def cal_by_nv(self):
        result = self.nv.merge(
            self.dividend, on=self.COL_DATE, how="outer"
        ).merge(
            self.split, on=self.COL_DATE, how="outer", suffixes=("_d", "_s")
        ).sort_values("date")

        # 分红默认为0, 拆分默认为1
        result[self.COL_D].fillna(0, inplace=True)
        result[self.COL_S].fillna(1, inplace=True)

        # 有净值的行索引(与分, 拆frame作outer_join时, 当净值日期与分拆日期不完全一致时, 会产生额外行
        NAV_INDEX = result[result[self.COL_NV].notnull()].index
        result[["date", "nav", "added_nav", "value_d", "value_s"]].fillna(method="bfill", inplace=True)
        # result["factor"] = (result[self.COL_S] * (1 + result[self.COL_D] / result[self.COL_NV])).cumprod()  # 旧公式
        factor_series = result[self.COL_S] * (result[self.COL_NV].shift(1) / (result[self.COL_NV].shift(1) - result[self.COL_D]))
        factor_series[0] = 1
        result["factor"] = factor_series.cumprod()  #
        result["adjusted_nav"] = result["factor"] * result[self.COL_NV]
        return result.loc[NAV_INDEX]


class DataSetup:
    def __init__(self):
        self.__errors = {}

    def init_data(self, fund_id, engine):
        """
        根据输入基金id, 初始化计算所需的3个frame:
        Args:
            fund_id:
            engine:

        Returns:

        """
        sql_nv = "SELECT fund_id, statistic_date as date, nav, added_nav " \
                 "FROM fund_nv WHERE fund_id = '{fid}'".format(fid=fund_id)

        sql_s = "SELECT IFNULL(split_date, statistic_date) as date, value " \
                "FROM fund_dividend_split WHERE fund_id = '{fid}' AND type='拆分'".format(fid=fund_id)
        sql_d = "SELECT IFNULL(ex_dividend_date, statistic_date) as date, value " \
                "FROM fund_dividend_split WHERE fund_id = '{fid}' AND type='分红'".format(fid=fund_id)

        df_nv = pd.read_sql(sql_nv, engine)
        df_d = pd.read_sql(sql_d, engine)
        df_s = pd.read_sql(sql_s, engine)
        return df_nv, df_d, df_s

    def init_funds(self, engine, num=None):
        import datetime as dt
        from dateutil.relativedelta import relativedelta
        now = (dt.datetime.now() - relativedelta(hours=1, minutes=5)).strftime("%Y%m%d%H%M%S")
        fids_nvupdated = set(pd.read_sql("SELECT DISTINCT fund_id FROM fund_nv WHERE update_time >= '{tm}'".format(tm=now), engine)["fund_id"])
        fids_dsupdated = set(pd.read_sql("SELECT DISTINCT fund_id FROM fund_dividend_split WHERE update_time >= '{tm}'".format(tm=now), engine)["fund_id"])
        fids_hasnonv = set(pd.read_sql("SELECT DISTINCT fund_id FROM fund_nv WHERE swanav IS NULL".format(tm=now), engine)["fund_id"])
        fids = sorted(fids_nvupdated.union(fids_dsupdated).union(fids_hasnonv))

        if num is not None:
            fids = fids[:num]
        return fids

    @property
    def errors(self):
        return pd.DataFrame.from_dict(self.__errors, orient="index").T


def main():
    fids = DataSetup.init_funds(DataSetup, engine)

    for fid in fids:
        print(fid)
        df_nv, df_dividend, df_split = DataSetup.init_data(DataSetup, fid, engine)
        calculator = AdjustedNvCalculator(df_nv, df_dividend, df_split)
        res = calculator.cal_by_nv()[["fund_id", "date", "adjusted_nav"]]
        res.columns = ["fund_id", "statistic_date", "swanav"]
        io.to_sql("fund_nv", engine, res)


def test():
    fids = ["020026"]

    for fid in fids:
        # print(fid)
        df_nv, df_dividend, df_split = DataSetup.init_data(DataSetup, fid, engine)
        calculator = AdjustedNvCalculator(df_nv, df_dividend, df_split)
        res = calculator.cal_by_nv()
        res.to_csv("c:/Users/Yu/Desktop/res_{fid}.csv".format(fid=fid), index=False)


def check_result():
    from collections import OrderedDict

    def init_smytnvdata():
        df_smyt = pd.read_sql(
            "SELECT fn.fund_id, fund_name, fn.statistic_date as date, nav, added_nav, swanav as adjusted_nav FROM fund_nv fn " \
            "JOIN (SELECT fund_id, MAX(statistic_date) as md FROM fund_nv GROUP BY fund_id) fn_latest " \
            "ON fn.fund_id = fn_latest.fund_id AND fn.statistic_date = fn_latest.md",
            engine
        )
        return df_smyt

    def init_windnvdata(fids):
        try:
            import datetime as dt
            df = pd.read_csv("c:/Users/Yu/Desktop/windnv.csv")
            df["date"] = df["date"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").date())
            df["fund_id"] = df["fund_id"].apply(lambda x: (6-len(str(x))) * "0" + str(x))
            df.columns = ["fund_id", "date", "nav", "added_nav", "adjusted_nav"]
            return df

        except:
            from WindPy import w
            from functools import partial
            w.start()
            FIELDS = OrderedDict([
                ("NAV_date", "date"),
                ("nav", "nav"),
                ("NAV_acc", "added_nav"),
                ("NAV_adj", "swanav"),
            ])
            windnvapi = partial(w.wsd, fields=",".join(FIELDS), options="")
            df_res = pd.DataFrame()

            for fid, sd in fids.items():
                begin = end = sd.strftime("%Y-%m-%d")
                print(fid, begin)
                wres = windnvapi("{fid}.OF".format(fid=fid), beginTime=begin, endTime=end)
                if wres.Data[0][0] is None:
                    continue
                df_tmp = pd.DataFrame(wres.Data).T
                df_tmp.columns = wres.Fields
                df_tmp["fund_id"] = fid
                df_res = df_res.append(df_tmp)
            df_res.columns = ["date", "nav", "added_nav", "adjusted_nav", "fund_id"]
            df_res = df_res[["fund_id", "date", "nav", "added_nav", "adjusted_nav"]]
            df_res.to_csv("c:/Users/Yu/Desktop/windnv.csv", index=False)
            return df_res

    def diff(lframe, rframe):
        merged = lframe.merge(rframe, on=["fund_id", "date"], suffixes=("_smyt", "_wind"))
        merged.index = merged["fund_id"]

        cond_nv_eq = merged["nav_smyt"].round(3) == merged["nav_wind"].round(3)
        cond_addednv_eq = merged["added_nav_smyt"].round(3) == merged["added_nav_wind"].round(3)
        cond_adjustednv_ne = merged["adjusted_nav_smyt"].round(3) != merged["adjusted_nav_wind"].round(3)

        # nav == added_nav, but adjusted_nav not equivalent
        df_p1 = merged[(cond_adjustednv_ne & cond_nv_eq)]

        # nav != added_nav, and adjusted_nav not equal
        df_p2 = merged[(cond_adjustednv_ne & (~cond_nv_eq))]

        merged.loc[df_p1.index, "error"] = "adjusted_nav"
        merged.loc[df_p2.index, "error"] = "nav,added_nav,adjusted_nav"
        return merged.sort_values(by="error", ascending=False)

    df_smyt = init_smytnvdata()

    fids_with_date = OrderedDict(df_smyt[["fund_id", "date"]][:500].as_matrix())
    df_wind = init_windnvdata(fids_with_date)

    q = diff(df_smyt, df_wind)
    q.to_csv("c:/Users/Yu/Desktop/stats.csv", index=False)

if __name__ == "__main__":
    main()
