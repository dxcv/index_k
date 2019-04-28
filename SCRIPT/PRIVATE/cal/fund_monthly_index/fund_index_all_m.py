import datetime as dt, time
import multiprocessing
import numpy as np, pandas as pd
from functools import partial
from utils.algorithm import timeutils as tu
from utils.database import sqlfactory as sf, config as cfg, io
from utils.script import scriptutils as su
from dateutil.relativedelta import relativedelta

engines = cfg.load_engine()
engine_rd = engines["2Gb"]

table = sf.Table("m", "index")
process_date = dt.date.today() - dt.timedelta(1)


def calculate(idx, export_path=None):
    dfs = pd.DataFrame()

    PEIndex = sf.PEIndex(idx)
    first_year = PEIndex.firstyear

    result_r = {}
    components_num = {}
    components = {}

    for year in range(first_year, process_date.year + 1):
        if year == process_date.timetuple().tm_year:
            month = process_date.month
        else:
            month = 12

        sql_i = sf.SQL_PEIndex(PEIndex.idx, year).yeardata_m

        conn = engine_rd.connect()

        su.tic("Getting Data")
        d = pd.read_sql(sql_i, conn)
        conn.close()

        su.tic("Preprocessing...")
        d["statistic_date"] = d["statistic_date"].apply(lambda x: time.mktime(x.timetuple()))
        d_dd = d.drop_duplicates("fund_id")
        idx_slice = d_dd.index.tolist()
        idx_slice.append(len(d))
        ids = d_dd["fund_id"].tolist()

        t_std = tu.timeseries_std(dt.datetime(year, month, 10), month, 12, 1, use_lastday=True)
        t_std1 = t_std[:-1]

        su.tic("Grouping...")
        ds = [d[idx_slice[i]:idx_slice[i + 1]] for i in range(len(idx_slice) - 1)]
        ts = [x["statistic_date"].tolist() for x in ds]
        navs = [x["nav"].tolist() for x in ds]

        su.tic("Matching...")
        matchs1 = [tu.outer_match4index_f7(x, t_std1, False) for x in ts]
        matchs2 = [tu.outer_match4index_b7(x, t_std1) for x in ts]
        matchs3 = [tu.outer_match4index_m(x, t_std, False) for x in ts]
        matchs = [su.merge_result(x1, x2, x3) for x1, x2, x3 in zip(matchs1, matchs2, matchs3)]

        su.tic("Getting Result...")
        t_matchs = [x[0] for x in matchs]
        t_matchs = [tu.tr(x) for x in t_matchs]
        idx_matchs = [x[1] for x in matchs]
        nav_matchs = [[navs[i][idx] if idx is not None else None for idx in idx_matchs[i].values()] for i in
                      range(len(idx_matchs))]

        su.tic("Calculating Index...")
        nvs = pd.DataFrame(nav_matchs).T.astype(float).as_matrix()
        rs = nvs[:-1] / nvs[1:] - 1
        rs[rs > 30] = np.nan
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
    for year in range(first_year, process_date.timetuple().tm_year + 1):
        if len(values_r) == 0:
            values_r = result_r[year].tolist()[::-1]
            values_num = components_num[year].tolist()[::-1]
        else:
            values_r.extend(result_r[year].tolist()[::-1])
            values_num.extend(components_num[year].tolist()[::-1])

    result = (np.array(values_r) + 1).cumprod() * 1000
    result = result.tolist()
    result.insert(0, 1000)
    values_num.insert(0, 0)

    # tag = tu.timeseries_std(dt.datetime(year, month + 1, 10),
    #                         tu.periods_in_interval(dt.datetime(year, month + 1, 10), dt.datetime(first_year, 1, 10),
    #                                                12), 12)[::-1]
    tag = tu.timeseries_std(dt.datetime(year, month, 10) + relativedelta(months=1),
                            tu.periods_in_interval(dt.datetime(year, month, 10) + relativedelta(months=1), dt.datetime(first_year, 1, 10),
                                                   12), 12)[::-1]
    tag = [dt.date.fromtimestamp(x - 864000) for x in tag]

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

    dfs = dfs.append(op[:-1])

    if export_path is not None:
        tmp = tag.copy()
        for year in sorted(components.keys(), reverse=True):
            print(year, len(tmp))
            components[year].columns = ["fund_id", *[tmp.pop() for i in range(len(components[year].columns) - 2)], tmp[-1]]
        io.export_to_xl(components, "{sd}_{index_name}_m_samples".format(sd=tag[-2].strftime("%Y%m%d"), index_name=PEIndex.id), export_path)

    return dfs


def main(export_path=None):
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    results = pool.map(partial(calculate, export_path=export_path), range(1, 14))
    pool.close()
    pool.join()
    for result in results:
        io.to_sql(table.name, engine_rd, result)


if __name__ == "__main__":
    main(None)
