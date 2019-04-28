import calendar as cld, datetime as dt
import numpy as np, pandas as pd
from utils.database import sqlfactory as sf, config as cfg, io
from utils.script import scriptutils as su
from dateutil.relativedelta import relativedelta
from utils.algorithm import fund_indicator as fi, timeutils as tu

table = sf.Table("w", "bm")
columns = ["index_id", "index_name", "statistic_date"]
columns.extend(table.columns())
yesterday = dt.date.today() - dt.timedelta(1)

engines = cfg.load_engine()
engine_rd = engines["2Gb"]


def calculate():
    conn = engine_rd.connect()

    year, month = yesterday.year, yesterday.month
    month_range = cld.monthrange(year, month)[1]
    time_to_fill = sf.Time(dt.datetime(year, month, month_range))
    year, month = time_to_fill.year, time_to_fill.month

    bms_used = ["hs300", "csi500", "sse50", "ssia", "cbi", "y1_treasury_rate", "nfi"]
    sql_bm = sf.SQL.market_index(date=time_to_fill.today, benchmarks=bms_used, whole=True)  # Get benchmark prices
    bm = pd.read_sql(sql_bm, conn)
    # bm.loc[bm["statistic_date"] == dt.date(1995, 8, 16), "y1_treasury_rate"] = 2.35

    bm["y1_treasury_rate"] = bm["y1_treasury_rate"].fillna(method="backfill")
    bm["y1_treasury_rate"] = bm["y1_treasury_rate"].apply(su.annually2weekly)
    bm["statistic_date"] = bm["statistic_date"].apply(su.date2tstp)

    prices_bm = [bm.dropna(subset=[bm_name])[bm_name].tolist() for bm_name in bms_used]
    ts_bm = [bm.dropna(subset=[bm_name])["statistic_date"].tolist() for bm_name in bms_used]

    prices = prices_bm.copy()
    ts = ts_bm.copy()

    t_mins_pe_all = sf.PEIndex().firstmonday  # 寻找指数中可被计算的
    pesid_used = []
    for k in t_mins_pe_all:
        if t_mins_pe_all[k].year < year:
            pesid_used.append(k)
        elif t_mins_pe_all[k].year == year:
            if t_mins_pe_all[k].month <= month:
                pesid_used.append(k)
            else:
                continue
        else:
            continue

    prices_pe = []
    ts_pe = []
    pes_used = []
    for idx in pesid_used:
        PE = sf.PEIndex(idx)
        pes_used.append(PE.id)
        sql_pe = sf.SQL.pe_index(time_to_fill.today, index_id=PE.id, freq="w")
        pe = pd.read_sql(sql_pe, conn)
        pe["statistic_date"] = pe["statistic_date"].apply(su.date2tstp)
        prices_pe.append(pe["index_value"].tolist())
        ts_pe.append(pe["statistic_date"].tolist())
    conn.close()

    prices.extend(prices_pe)
    ts.extend(ts_pe)

    t_mins_tstp = [min(x) for x in ts]
    t_mins = tu.tr(t_mins_tstp)

    intervals = table.intervals
    intervals1 = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    intervals3 = [0, 1, 2, 3, 4, 5, 6, 10, 11]

    index_used = bms_used.copy()
    index_used.extend(pes_used)

    index_name = {"FI01": "私募全市场指数", "FI02": "阳光私募指数", "FI03": "私募FOF指数",
                  "FI04": "股票多头策略私募指数", "FI05": "股票多空策略私募指数", "FI06": "市场中性策略私募指数",
                  "FI07": "债券基金私募指数", "FI08": "管理期货策略私募指数", "FI09": "宏观策略私募指数",
                  "FI10": "事件驱动策略私募指数", "FI11": "相对价值策略私募指数", "FI12": "多策略私募指数",
                  "FI13": "组合投资策略私募指数", "hs300": "沪深300指数", "csi500": "中证500指数",
                  "sse50": "上证50指数", "ssia": "上证A股指数", "cbi": "中债指数", "nfi": "南华商品指数",
                  "y1_treasury_rate": "y1_treasury_rate"}

    result = []
    for mday in range(1, yesterday.day + 1):
        print("Day {0}: {1}".format(mday, dt.datetime.now()))

        date_s = sf.Time(dt.datetime(year, month, mday))  # Generate statistic_date

        #
        t_stds = [tu.timeseries_std(date_s.today, interval, 52, extend=1) for interval in intervals]  # 标准序列
        t_std_lens = [len(x) - 1 for x in t_stds]  # 标准序列净值样本个数
        t_std_week = tu.timeseries_std(date_s.today, "w", 52, 1)  # 标准序列_本周
        ts_std_total = [tu.timeseries_std(date_s.today, tu.periods_in_interval(date_s.today, t_min, 12), extend=4)
                        for
                        t_min in t_mins]  # 标准序列_成立以来
        ts_std_total = [t_std_total[:len([x for x in t_std_total if x >= t_min]) + 1] for t_std_total, t_min in
                        zip(ts_std_total, t_mins_tstp)]

        # 基准指数的标准序列_成立以来
        matchs = [tu.outer_match4indicator_w(t, t_std_all, False) for t, t_std_all in zip(ts, ts_std_total)]
        idx_matchs = [x[1] for x in matchs]
        prices_total = [[price[ix] if ix is not None else None for ix in idx.values()] for price, idx in
                        zip(prices, idx_matchs)]

        # 基准指标的收益率_不同频率
        rs_total = [fi.gen_return_series(price_total) for price_total in prices_total]

        # 无风险国债的收益率
        r_f_total = prices_total[5][1:]  # the list `y1_treasury_rate` in prices_total is not price, but return
        r_f_total = pd.DataFrame(r_f_total).fillna(method="backfill")[0].tolist()
        r_f_all = [r_f_total[:length - 1] for length in t_std_lens]
        r_f_all.append(r_f_total)

        # for i in range(len(index_used)):
        for i in range(len(index_used)):

            if index_name[index_used[i]] == "y1_treasury_rate": continue

            price_all = []
            r_all = []
            for j in range(7):
                if dt.date.fromtimestamp(
                        (t_mins[i] + relativedelta(months=intervals[j])).timestamp()) <= date_s.today:
                    price_all.append(prices_total[i][:t_std_lens[j]])
                    r_all.append(rs_total[i][:t_std_lens[j] - 1])
                else:
                    price_all.append([])
                    r_all.append([])
            for j in range(7, 11):
                price_all.append(prices_total[i][:t_std_lens[j]])
                if rs_total[i] is not None:
                    r_all.append(rs_total[i][:t_std_lens[j] - 1])
                else:
                    r_all.append([])

            price_all.append(prices_total[i])
            r_all.append(rs_total[i])
            price_all1 = [price_all[i] for i in intervals1]
            price_all3 = [price_all[i] for i in intervals3]
            r_all1 = [r_all[i] for i in intervals1]
            r_all3 = [r_all[i] for i in intervals3]

            r_f_all1 = [r_f_all[i] for i in intervals1][:-1]
            r_f_all3 = [r_f_all[i] for i in intervals3][:-1]
            r_f_all1.append(r_f_all[-1][:len(r_all[-1])])
            r_f_all3.append(r_f_all[-1][:len(r_all[-1])])

            ir = [fi.accumulative_return(price) for price in price_all1]
            ir_a = [fi.return_a(r) for r in r_all1]
            stdev_a = [fi.standard_deviation_a(r) for r in r_all3]
            dd_a = [fi.downside_deviation_a(r, r_f) for r, r_f in zip(r_all3, r_f_all3)]
            mdd = [fi.max_drawdown(price)[0] for price in price_all3]
            sharpe_a = [fi.sharpe_a(r, r_f) for r, r_f in zip(r_all3, r_f_all3)]
            calmar_a = [fi.calmar_a(price, r_f) for price, r_f in zip(price_all3, r_f_all3)]
            sortino_a = [fi.sortino_a(r, r_f) for r, r_f in zip(r_all3, r_f_all3)]
            p_earning_weeks = [fi.periods_positive_return(r) for r in r_all3]
            n_earning_weeks = [fi.periods_npositive_return(r) for r in r_all3]
            con_rise_weeks = [fi.periods_continuous_rise(r)[0] for r in r_all3]
            con_fall_weeks = [fi.periods_continuous_fall(r)[0] for r in r_all3]

            tmp = [ir, ir_a, stdev_a, dd_a, mdd, sharpe_a, calmar_a, sortino_a, p_earning_weeks, n_earning_weeks,
                   con_rise_weeks, con_fall_weeks]
            result_i = [index_used[i], index_name[index_used[i]], date_s.today]
            for x in tmp:
                result_i.extend(x)
            result.append(result_i)

    df = pd.DataFrame(result)
    df[list(range(3, 117))] = df[list(range(3, 117))].astype(np.float64)
    df[list(range(3, 117))] = df[list(range(3, 117))].apply(lambda x: round(x, 6))
    df.columns = columns

    df.index_id = df.index_id.apply(lambda x: x.upper())
    return df


def main():
    result = calculate()
    io.to_sql(table.name, engine_rd, result, "update")


if __name__ == "__main__":
    main()
