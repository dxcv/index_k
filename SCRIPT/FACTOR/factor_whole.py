import datetime as dt
from SCRIPT.FACTOR.daily import (
    factor_fama_d, factor_carhart_d, factor_famafrench_d)
from SCRIPT.FACTOR.weekly import (
    factor_famafrench_w, factor_carhart_w, factor_fama_w, factor_sharpe_w)

END = dt.date.today()
START = END - dt.timedelta(31)


# 因子计算, 启动前要确保base_finance.stock_price中的close字段, 和stock_valuation中的circulated_price字段已从万德采集完毕;

def main():
    for mod in (
            factor_fama_d, factor_carhart_d, factor_famafrench_d, factor_fama_w, factor_carhart_w, factor_famafrench_w):
        mod.cal(START, END)


if __name__ == "__main__":
    main()
