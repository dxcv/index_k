import datetime as dt
from calendar import monthrange
from utils.algorithm.stock.typing import StyleType
from utils.database import config as cfg, io


def get_param():
    from dateutil.relativedelta import relativedelta
    today = dt.date.today()
    year, month = today.year, today.month

    delta = month % 3
    if delta == 0 and today.day != monthrange(year, month)[1]:
        delta += 3
    date = today - relativedelta(months=delta)
    calculable_date = dt.date(date.year, date.month, monthrange(date.year, date.month)[1])

    year, month = calculable_date.year, calculable_date.month
    season = (month - 1) // 3 + 1
    return year, season


def main():
    engine = cfg.load_engine()["2Gb"]
    io.to_sql("base_finance.stock_type_style", engine, StyleType(*get_param()).classified)
    # for year, season in [*[(y, s) for y in range(2015, 2018) for s in range(1, 5)], (2018, 1)]:
    #     io.to_sql("base_finance.stock_type_style", engine, StyleType(year, season).classified)


if __name__ == "__main__":
    main()
