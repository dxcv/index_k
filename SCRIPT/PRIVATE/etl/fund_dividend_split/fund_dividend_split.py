from multiprocessing.dummy import Pool as ThreadPool
import pandas as pd
from utils.database import config as cfg, io, sqlfactory

ENGINE_RD = cfg.load_engine()["2Gb"]
TABLE = "base_test.fund_dividend_split_test"


class Calculator:
    COLS = ["fund_id", "statistic_date", "nav", "added_nav", "dividend", "dividend_acc", "split", "split_acc", "is_used",]

    @classmethod
    def fetch_all_funds(cls):
        sql = "SELECT DISTINCT fund_id FROM base.fund_nv_data_standard_copy2 WHERE nav != added_nav"
        sql_2 = "SELECT DISTINCT fund_id FROM {tb}".format(tb=TABLE)

        fids_to_update = set([x[0] for x in ENGINE_RD.execute(sql).fetchall()])
        fids_to_logicdel = set([x[0] for x in ENGINE_RD.execute(sql_2).fetchall()]) - fids_to_update
        return sorted(fids_to_update), sorted(fids_to_logicdel)

    @classmethod
    def _calculate_ds(cls, fund_id):
        sql = "SELECT fund_id, statistic_date, nav, added_nav " \
              "FROM fund_nv_data_standard_copy2 WHERE fund_id = '{fid}' " \
              "AND nav IS NOT NULL AND added_nav IS NOT NULL AND statistic_date IS NOT NULL".format(fid=fund_id)
        df_nv = pd.read_sql(sql, ENGINE_RD)

        if len(df_nv) <= 1:
            return pd.DataFrame()

        df_nv["dividend_acc"] = df_nv["added_nav"] - df_nv["nav"]
        df_nv["dividend"] = df_nv["dividend_acc"] - df_nv["dividend_acc"].shift(1)
        df_nv["split_acc"] = df_nv["added_nav"] / df_nv["nav"]
        df_nv["split"] = df_nv["split_acc"] / df_nv["split_acc"].shift(1)
        df_nv["is_used"] = 1
        df_nv = df_nv[1:]  # 去除首条记录
        df_nv.index = range(len(df_nv))

        try:
            ds_from = min(df_nv.loc[(df_nv.dividend != 0) | (df_nv.split != 1)].index)
            df_nv = df_nv[ds_from:]
        except:
            df_nv = pd.DataFrame()

        return df_nv

    @classmethod
    def calculate_ds(cls, fund_ids):
        res = pd.DataFrame()
        for fund_id in fund_ids:
            res = res.append(cls._calculate_ds(fund_id))

        # 减少读写
        if len(fund_ids) > 0:
            fund_ids = sqlfactory.SQL.values4sql(fund_ids)
            ENGINE_RD.execute("UPDATE {tb} SET is_used = 0 WHERE fund_id IN {fids}".format(tb=TABLE, fids=fund_ids))
        if len(res) > 0: io.to_sql(TABLE, ENGINE_RD, res[cls.COLS])

    @classmethod
    def calculate_all(cls):
        p = ThreadPool(6)
        fids, fids_logicdel = cls.fetch_all_funds()
        if len(fids_logicdel) > 0:
            ENGINE_RD.execute("UPDATE {tb} SET is_used = 0 WHERE fund_id IN {fids}".format(tb=TABLE, fids=sqlfactory.SQL.values4sql(fids_logicdel)))
        STEP = 50
        fids = [fids[i: i + STEP] for i in range(0, len(fids), STEP)]
        p.map(cls.calculate_ds, fids)


def main():
    Calculator.calculate_all()


if __name__ == "__main__":
    main()
