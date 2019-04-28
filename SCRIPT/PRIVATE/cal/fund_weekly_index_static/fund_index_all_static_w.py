import calendar as cld
import datetime as dt, time
import numpy as np, pandas as pd
from dateutil.relativedelta import relativedelta
from functools import partial
from utils.algorithm import timeutils as tu
from utils.database import sqlfactory as sf, config as cfg, io
from utils.script import scriptutils as su
import multiprocessing

engines = cfg.load_engine()
engine_rd = engines["2Gb"]
engine_wt = engines["2Gb"]

process_date = dt.date.today()
table = sf.Table("w", "index")


def calculate(idx, export_path=None):
    dfs = pd.DataFrame()

    PEIndex = sf.PEIndex(idx)
    first_date = PEIndex.firstmonday

    result_r = {}
    components_num = {}
    components = {}

    for year in range(first_date.timetuple().tm_year, process_date.year + 1):

        if year == process_date.year:
            month = process_date.month
            day = tu.date_of_weekday(process_date, 0, (0, 0)).day
            if day > process_date.day:  # 修正周一在上个月，跨月后产生的日期问题
                month -= 1
        else:
            month = 12
            day = 31

        date_s = dt.date(year, month, day)  #

        sql_i = sf.SQL_PEIndex(idx, year).yeardata_w["nv"]
        sql_mindate = sf.SQL_PEIndex(idx, year).yeardata_w["t_min"]

        conn = engine_rd.connect()

        su.tic("Getting Data")
        d = pd.read_sql(sql_i, conn)
        d.index = range(len(d))

        t_min = pd.read_sql(sql_mindate, conn)["statistic_date_earliest"].tolist()
        t_min = [time.mktime(x.timetuple()) for x in t_min]  #
        conn.close()

        su.tic("Preprocessing...")
        d["statistic_date"] = d["statistic_date"].apply(lambda x: time.mktime(x.timetuple()))
        d_dd = d.drop_duplicates("fund_id")
        idx_slice = d_dd.index.tolist()
        idx_slice.append(len(d))
        ids = d_dd["fund_id"].tolist()

        last_monday = date_s - dt.timedelta(cld.weekday(date_s.year, date_s.month, date_s.day))  #
        t_std = tu.timeseries_std(last_monday, "a", 52, extend=1)  #
        if year == first_date.timetuple().tm_year:
            t_std = t_std[:-1]

        #
        su.tic("Slicing")
        t_std_long = tu.timeseries_std(last_monday,
                                       tu.periods_in_interval(last_monday, dt.date(year - 1, 11, 30), 12))
        t_std_long_p1m = [(x + relativedelta(months=1)).timestamp() for x in tu.tr(t_std_long)]
        real_p1m = su.compare(t_min, t_std_long)  # 实际最早日期和标准序列日期比较
        p1m_std = su.compare(t_std_long_p1m, t_std)  # 加一个月的标准序列日期和标准序列日期比较
        data_used = [p1m_std[x - 1] for x in real_p1m]

        su.tic("Grouping...")
        ds = [d[idx_slice[i]:idx_slice[i + 1]] for i in range(len(idx_slice) - 1)]
        ts = [x["statistic_date"].tolist() for x in ds]
        navs = [x["nav"].tolist() for x in ds]

        su.tic("Matching...")
        matchs = [tu.outer_match4index_w(x, t_std, False) for x in ts]

        su.tic("Getting Result...")
        t_matchs = [x[0] for x in matchs]
        t_matchs = [tu.tr(x) for x in t_matchs]
        idx_matchs = [x[1] for x in matchs]
        nav_matchs = [[navs[i][idx] if idx is not None else None for idx in idx_matchs[i].values()] for i in
                      range(len(idx_matchs))]

        su.tic("Calculating Index...")
        nvs = pd.DataFrame(nav_matchs).T.astype(float).as_matrix()
        for i in range(len(ids)):
            nvs[data_used[i] + 1:, i] = np.nan

        rs = nvs[:-1] / nvs[1:] - 1
        rs[rs > 10] = np.nan
        rs[rs < -1] = np.nan
        r = np.nanmean(rs, axis=1)
        r[np.isnan(r)] = 0

        result_r[year] = r
        components_num[year] = np.sum(~np.isnan(rs), axis=1)

        # log samples
        tmp = pd.DataFrame(nvs, columns=ids).T
        tmp["fund_id"] = tmp.index
        tmp = tmp[[tmp.columns[-1], *tmp.columns[:-1]]]
        components[year] = tmp

        su.tic("Year:{0}, Done...".format(year))
    values_r = []
    values_num = []
    for year in range(first_date.timetuple().tm_year, date_s.year + 1):
        if len(values_r) == 0:
            values_r = result_r[year].tolist()[::-1]
            values_num = components_num[year].tolist()[::-1]
        else:
            values_r.extend(result_r[year].tolist()[::-1])
            values_num.extend(components_num[year].tolist()[::-1])

    adjust_periods = 4
    sql_base = "SELECT index_value FROM fund_weekly_index_static WHERE index_id = '{idx_id}' \
    AND statistic_date = '{sd}'".format(idx_id=PEIndex.id, sd=date_s - dt.timedelta(7 * (adjust_periods + 1)))
    base = pd.read_sql(sql_base, engine_rd).get_value(0, "index_value")

    result = (np.array(values_r)[-(adjust_periods+1):] + 1).cumprod() * base
    result = result.tolist()
    values_num = values_num[-(adjust_periods + 1):]

    tag = tu.timeseries_std(
        dt.datetime(year, month, day),
        tu.periods_in_interval(dt.datetime(year, month, day), dt.datetime(first_date.year, 1, 1), 12),
        52,
        extend=5
    )[::-1]
    tag = [x for x in tag if x >= first_date.timestamp()]
    tag = [dt.date.fromtimestamp(x) for x in tag]
    tag = tag[-(adjust_periods + 1):]

    # local debug
    op = pd.DataFrame(list(zip(tag, result, values_num)))
    op.columns = ["statistic_date", "index_value", "funds_num"]

    cols = ["index_id", "index_name", "typestandard_code", "typestandard_name", "type_code", "type_name",
            "stype_code",
            "stype_name", "index_method", "data_source", "data_source_name"]
    values = [PEIndex.id, PEIndex.name, PEIndex.typestandard["code"], PEIndex.typestandard["name"],
              PEIndex.type["code"],
              PEIndex.type["name"], PEIndex.stype["code"], PEIndex.stype["name"], 1, 0, "私募云通"]
    col_dict = dict(zip(cols, values))
    for col, val in col_dict.items():
        op[col] = val

    dfs = dfs.append(op)

    if export_path is not None:
        io.export_to_xl(components, "{sd}_{index_name}_samples(static)".format(sd=last_monday.strftime("%Y%m%d"), index_name=PEIndex.name))

    return dfs


def main(export_path=None):
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    results = pool.map(partial(calculate, export_path=export_path), range(1, 13 + 1))
    pool.close()
    pool.join()

    for result in results:
        io.to_sql("fund_weekly_index_static", engine_wt, result)


if __name__ == "__main__":
    main()
