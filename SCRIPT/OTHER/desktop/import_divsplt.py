import datetime as dt
import pandas as pd
from utils.database import config as cfg, io

eg = cfg.load_engine()["2Gb"]


class Parser:
    def __init__(self, file):
        self.file = file

    @property
    def new(self):
        df = pd.read_excel(self.file)
        df["statistic_date"] = df["statistic_date"].apply(lambda x: dt.date(x.year, x.month, x.day))
        return df.dropna(subset=["fund_id", "statistic_date", "type"])

    @property
    def old(self):
        df = self.new
        df["source"] = "私募云通"
        df["source_code"] = "0"
        df_div = df.loc[df["type"] == "分红"]
        df_splt = df.loc[df["type"] == "拆分"]

        df_div = df_div.rename(columns={"value": "after_tax_bonus"}).drop(axis=1, columns=["type"])
        df_div["fund_allocation_category"] = 1
        df_splt = df_splt.rename(columns={"value": "split_ratio"}).drop(axis=1, columns=["type"])
        df_splt["fund_allocation_category"] = 1
        return df_div.dropna(subset=["fund_id", "statistic_date", "fund_allocation_category"]), df_splt.dropna(subset=["fund_id", "statistic_date", "fund_allocation_category"])


def main():
    f = "C:\\Users\\Yu\\Documents\\WeChat Files\\wxid_rc6z8uvrexqz21\\Files\\分红导入0530.xlsx"
    p = Parser(f)
    io.to_sql("base.fund_allocation_data", eg, p.old[0])
    io.to_sql("base.fund_allocation_data", eg, p.old[1])
    io.to_sql("base.fund_dividend_split", eg, p.new)


if __name__ == "__main__":
    main()
