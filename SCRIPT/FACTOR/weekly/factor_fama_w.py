import datetime as dt
import pandas as pd
from utils.algorithm.base.exceptions import DataError
from utils.algorithm.fama import resultproxy
from utils.database import config as cfg, io

engine = cfg.load_engine()["2Gf"]


def cal(start=None, end=None):
    start = start or dt.date(2015, 4, 1)
    end = end or dt.date.today()
    dates = [x.date() for x in pd.date_range(start, end, freq="W-FRI")]
    for ed in dates:
        try:
            prox = resultproxy.Fama3(ed, "w")
            io.to_sql("factor_style_w", engine, prox.result)
            print(prox.result)
        except DataError:
            continue


def main():
    cal()


if __name__ == "__main__":
    main()
