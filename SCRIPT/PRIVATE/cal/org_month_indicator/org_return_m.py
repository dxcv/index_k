import calendar as cld, datetime as dt, time
from dateutil.relativedelta import relativedelta
import numpy as np, pandas as pd
from utils.algorithm import fund_indicator as fi, timeutils as tu
from utils.database import sqlfactory as sf, config as cfg, io
import utils.script.scriptutils as su

table = sf.Table("m", "oreturn")
columns = ["org_id", "org_name", "benchmark", "index_method", "index_range", "funds_number",
           "type_code", "type_name", "stype_code", "stype_name", "statistic_date"]
columns.extend(table.columns())

#
engines = cfg.load_engine()
engine_read = engines["2Gb"]
engine_wt = engines["2Gb"]

process_date = dt.date.today() - dt.timedelta(1)


def calculate():
    su.tic("fetching get_data...")
    df_whole = pd.DataFrame()
    conn = engine_read.connect()

    year = process_date.year
    month = process_date.month

    month_range = cld.monthrange(year, month)[1]
    time_to_fill = sf.Time(dt.datetime(year, month, month_range))

    sql_bm = sf.SQL.market_index(time_to_fill.today)  # Get benchmark prices
    sql_pe = sf.SQL.pe_index(time_to_fill.today, freq="m")  ###w->m

    su.tic("preprocessing get_data...")
    bm = pd.read_sql(sql_bm, engine_read)
    bm["y1_treasury_rate"] = bm["y1_treasury_rate"].fillna(method="backfill")
    bm["y1_treasury_rate"] = bm["y1_treasury_rate"].apply(su.annually2monthly)
    bm["statistic_date"] = bm["statistic_date"].apply(su.date2tstp)
    pe = pd.read_sql(sql_pe, engine_read)
    pe["statistic_date"] = pe["statistic_date"].apply(su.date2tstp)
    conn.close()

    prices_bm = [bm["hs300"].tolist(), bm["csi500"].tolist(), bm["sse50"].tolist(), bm["cbi"].tolist(), bm["nfi"]]
    price_pe = pe["index_value"].tolist()
    r_tbond = bm["y1_treasury_rate"].tolist()
    t_bm = bm["statistic_date"].tolist()
    t_pe = pe["statistic_date"].tolist()

    intervals = table.intervals
    intervals4 = [1, 2, 3, 4, 5, 6, 9, 10, 11]
    intervals5 = [1, 2, 3, 4, 5, 6, 10, 11]

    result = []
    conn = engine_read.connect()

    # Get Data
    date_s = sf.Time(process_date - dt.timedelta(process_date.day))  # Generate statistic_date

    sql_fids_updated = sf.SQL.ids_updated_sd(date_s.today, "om")
    ids_updated = tuple(x[0] for x in conn.execute(sql_fids_updated).fetchall())  # 找到当月净值有更新的基金

    sql_o_updated = "SELECT DISTINCT fom.org_id FROM fund_org_mapping fom \
             JOIN org_info oi ON fom.org_id = oi.org_id \
             WHERE org_type_code = 1 AND oi.found_date <= '{0}'  AND fund_id IN {1}".format(
        date_s.today - relativedelta(months=3),
        ids_updated
    )  # 根据净值更新的基金确定需要计算的投顾
    o_updated = tuple(x[0] for x in conn.execute(sql_o_updated).fetchall())

    sql_fom = "SELECT fom.org_id, fom.fund_id, oi.found_date, oi.org_name FROM fund_org_mapping fom \
               JOIN org_info oi ON fom.org_id = oi.org_id \
               JOIN fund_info fi ON fom.fund_id = fi.fund_id \
               WHERE fom.org_id IN {0} AND fom.org_type_code = 1 AND oi.found_date <= '{1}' AND fi.foundation_date <= '{2}'".format(
        o_updated, date_s.today - relativedelta(months=3),
                   date_s.today - relativedelta(months=1))
    fom = pd.read_sql(sql_fom, conn)  # 根据需要计算的投顾找到其旗下管理的所有基金

    fid_used = tuple(fom["fund_id"])
    sql_fnd = sf.SQL.nav(fid_used)
    fnd = pd.read_sql(sql_fnd, conn)
    fnd = fnd.dropna()
    fnd.index = range(len(fnd))

    data = fom.merge(fnd, how="inner", on="fund_id")
    data = data.sort_values(by=["org_id", "fund_id", "statistic_date"], ascending=[True, True, False])
    t_mins = data.groupby(["org_id"])["statistic_date"].min().tolist()
    t_mins_tstp = [time.mktime(x.timetuple()) for x in t_mins]
    data["statistic_date"] = data["statistic_date"].apply(lambda x: time.mktime(x.timetuple()))
    data.index = range(len(data))

    ids_o = data["org_id"].drop_duplicates().tolist()
    names_o = data.drop_duplicates(subset=["org_id"])["org_name"].tolist()
    idx4slice_o = su.idx4slice(data, "org_id")
    dfs = [data[idx4slice_o[i]:idx4slice_o[i + 1]] if i != len(idx4slice_o) - 1 else data[idx4slice_o[i]:] for i
           in range(len(idx4slice_o) - 1)]

    # Proprocess
    # 标准序列
    t_stds = [tu.timeseries_std(date_s.today, interval, periods_y=12, use_lastday=True, extend=1) for interval
              in intervals]
    t_std_y5 = t_stds[6]
    t_stds_len = [len(x) - 1 for x in t_stds]

    # 基金标准序列_成立以来
    t_std_alls = [tu.timeseries_std(date_s.today, tu.periods_in_interval(date_s.today, t_min, 12), periods_y=12,
                                    use_lastday=True, extend=6) for t_min in t_mins]  # 标准序列_成立以来
    t_std_alls = [t_std_all[:len([x for x in t_std_all if x >= t_min]) + 1] for t_std_all, t_min in
                  zip(t_std_alls, t_mins_tstp)]

    # 基准指数的标准序列_成立以来
    matchs_bm = [tu.outer_match4indicator_m(t_bm, t_std_all, False) for t_std_all in t_std_alls]
    idx_matchs_bm = [x[1] for x in matchs_bm]
    price_bm0_all = [[prices_bm[0][ix] if ix is not None else None for ix in idx.values()] for idx in
                     idx_matchs_bm]
    price_bm1_all = [[prices_bm[1][ix] if ix is not None else None for ix in idx.values()] for idx in
                     idx_matchs_bm]
    price_bm2_all = [[prices_bm[2][ix] if ix is not None else None for ix in idx.values()] for idx in
                     idx_matchs_bm]
    price_bm3_all = [[prices_bm[3][ix] if ix is not None else None for ix in idx.values()] for idx in
                     idx_matchs_bm]
    price_bm4_all = [[prices_bm[4][ix] if ix is not None else None for ix in idx.values()] for idx in
                     idx_matchs_bm]

    matchs_pe = [tu.outer_match4indicator_m(t_pe, t_std_all, False) for t_std_all in t_std_alls]
    idx_matchs_pe = [x[1] for x in matchs_pe]
    price_pe_all = [[price_pe[ix] if ix is not None else None for ix in idx.values()] for idx in idx_matchs_pe]

    # 基准指标的收益率_成立以来
    r_bm0_all = [fi.gen_return_series(x) for x in price_bm0_all]
    r_bm1_all = [fi.gen_return_series(x) for x in price_bm1_all]
    r_bm2_all = [fi.gen_return_series(x) for x in price_bm2_all]
    r_bm3_all = [fi.gen_return_series(x) for x in price_bm3_all]
    r_bm4_all = [fi.gen_return_series(x) for x in price_bm4_all]

    r_pe_all = [fi.gen_return_series(x) for x in price_pe_all]

    tmp = [len(idx_matchs_bm[i]) for i in range(len(idx_matchs_bm))]
    tmp_id = tmp.index(max(tmp))
    tmp_list = [r_tbond[ix] if ix is not None else None for ix in idx_matchs_bm[tmp_id].values()]
    tmp = pd.DataFrame(tmp_list)[0].fillna(method="backfill").tolist()

    r_f_all = [[r_tbond[idx[k]] if idx[k] is not None else tmp[k] for k in idx.keys()] for idx in idx_matchs_bm]
    r_f_all = [x[1:] for x in r_f_all]

    # 基准指标的收益率_不同频率
    matchs_bm = tu.outer_match4indicator_m(t_bm, t_std_y5, False)  # 基准指数标准序列_成立以来
    matchs_pe = tu.outer_match4indicator_m(t_pe, t_std_y5, False)
    idx_matchs_bm = matchs_bm[1]
    idx_matchs_pe = matchs_pe[1]
    price_bm0_y5 = [prices_bm[0][ix] if ix is not None else None for ix in idx_matchs_bm.values()]
    price_bm1_y5 = [prices_bm[1][ix] if ix is not None else None for ix in idx_matchs_bm.values()]
    price_bm2_y5 = [prices_bm[2][ix] if ix is not None else None for ix in idx_matchs_bm.values()]
    price_bm3_y5 = [prices_bm[3][ix] if ix is not None else None for ix in idx_matchs_bm.values()]
    price_bm4_y5 = [prices_bm[4][ix] if ix is not None else None for ix in idx_matchs_bm.values()]

    price_pe_y5 = [price_pe[ix] if ix is not None else None for ix in idx_matchs_pe.values()]

    # 基准指标的收益率_不同频率
    r_bm0_y5 = fi.gen_return_series(price_bm0_y5)
    r_bm1_y5 = fi.gen_return_series(price_bm1_y5)
    r_bm2_y5 = fi.gen_return_series(price_bm2_y5)
    r_bm3_y5 = fi.gen_return_series(price_bm3_y5)
    r_bm4_y5 = fi.gen_return_series(price_bm4_y5)
    r_pe_y5 = fi.gen_return_series(price_pe_y5)

    r_f_y5 = [r_tbond[ix] if ix is not None else None for ix in idx_matchs_bm.values()]
    r_f_y5 = r_f_y5[1:]

    rs_bm0 = [r_bm0_y5[:length - 1] for length in t_stds_len]
    rs_bm1 = [r_bm1_y5[:length - 1] for length in t_stds_len]
    rs_bm2 = [r_bm2_y5[:length - 1] for length in t_stds_len]
    rs_bm3 = [r_bm3_y5[:length - 1] for length in t_stds_len]
    rs_bm4 = [r_bm4_y5[:length - 1] for length in t_stds_len]

    rs_pe = [r_pe_y5[:length - 1] for length in t_stds_len]
    rs_f = [r_f_y5[:length - 1] for length in t_stds_len]

    benchmark = {1: rs_bm0, 2: rs_bm1, 3: rs_bm2, 4: rs_pe, 6: rs_bm3, 7: rs_bm4}
    benchmark_all = {1: r_bm0_all, 2: r_bm1_all, 3: r_bm2_all, 4: r_pe_all, 6: r_bm3_all, 7: r_bm4_all}

    su.tic("calculating ...")
    for i in range(len(ids_o)):
        if i % 200 == 0:
            print("%s done..." % str(i * 200))
        df = dfs[i]
        df.index = range(len(df))
        idx4slice = su.idx4slice(df, "fund_id")
        navs = su.slice(df, idx4slice, "nav")
        t_reals = su.slice(df, idx4slice, "statistic_date")

        matchs_all = [tu.outer_match4indicator_m(t_real, t_std_alls[i], drop_none=False) for t_real in t_reals]
        idx_matchs_all = [x[1] for x in matchs_all]
        nav_matchs_all = [[nav[ix] if ix is not None else np.NaN for ix in idx.values()] for nav, idx in
                          zip(navs, idx_matchs_all)]

        nv_matrix = np.array(nav_matchs_all).T
        r_total = np.nanmean((nv_matrix[:-1] / nv_matrix[1:] - 1), axis=1)
        price_total = np.nancumprod(1 + r_total[::-1])[::-1].tolist()
        price_total.append(1)  # 定义基期伪价格为1
        r_total = fi.gen_return_series(price_total)

        prices = []
        for j in range(7):
            if t_mins[i] + relativedelta(months=intervals[j]) <= date_s.today:
                length = min(len(price_total), t_stds_len[j])
                prices.append(price_total[:length])
            else:
                prices.append(None)

        for j in range(7, 11):
            length = min(len(price_total), t_stds_len[j])
            prices.append(price_total[:length])

        prices.append(price_total)
        navs1 = [prices[i] for i in intervals4]
        navs2 = [prices[i] for i in intervals5]
        rs1 = [fi.gen_return_series(x) for x in navs1]
        rs2 = [fi.gen_return_series(x) for x in navs2]

        rs_f_ = rs_f.copy()
        rs_f_.append(r_f_all[i])
        rs_f1_ = [rs_f_[i] for i in intervals4]
        rs_f2_ = [rs_f_[i] for i in intervals5]

        ir = [fi.accumulative_return(r) for r in navs1[:-1]]
        ir.append(fi.accumulative_return([x for x in navs1[-1] if x is not None]))  #
        ir_a = [fi.return_a(r, 12) for r in rs1]
        calmar = [fi.calmar_a(nv, r_f, 12) for nv, r_f in zip(navs2, rs_f2_)]
        sortino = [fi.sortino_a(r, r_f, 12) for r, r_f in zip(rs2, rs_f2_)]

        for k in benchmark.keys():
            rs_bm_ = benchmark[k].copy()  # 指定benchmark
            rs_bm_.append(benchmark_all[k][i])
            rs_bm1 = [rs_bm_[i] for i in intervals4]
            rs_bm2 = [rs_bm_[i] for i in intervals5]

            ier_a = [fi.excess_return_a(r, r_bm, 12) for r, r_bm in zip(rs1, rs_bm1)]
            info = [fi.info_a(r, r_bm, 12) for r, r_bm in zip(rs2, rs_bm2)]
            jensen = [fi.jensen_a(r, r_bm, r_f, 12) for r, r_bm, r_f in zip(rs2, rs_bm2, rs_f2_)]
            treynor = [fi.treynor_a(r, r_bm, r_f, 12) for r, r_bm, r_f in zip(rs2, rs_bm2, rs_f2_)]
            sharpe_a = [fi.sharpe_a(r, r_f, 12) for r, r_f in zip(rs2, rs_f2_)]

            tmp = [ir, ir_a, ier_a, calmar, sortino, treynor, info, jensen, sharpe_a]
            result_i = [ids_o[i], names_o[i], k, 1, 1, nv_matrix.shape[1], 60001, "全产品", 6000101, "全产品",
                        date_s.today]
            for x in tmp:
                result_i.extend(x)
            result.append(result_i)

    df = pd.DataFrame(result)
    df[list(range(11, 78+8))] = df[list(range(11, 78+8))].astype(np.float64)
    df[list(range(11, 78+8))] = df[list(range(11, 78+8))].apply(lambda x: round(x, 6))
    df.columns = columns
    df_whole = df_whole.append(df)

    return df_whole


def main():
    result = calculate()
    io.to_sql(table.name, engine_wt, result)


if __name__ == "__main__":
    main()
