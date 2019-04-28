import datetime as dt
import pandas as pd
from utils.database import config as cfg, io
from utils.script.scriptutils import tic

engines = cfg.load_engine()
engine_rd = engines["2Gb"]
engine_wt = engines["2Gb"]


def modify_full_month(day_diff):
    if day_diff > 0:
        return 1
    elif day_diff < 0:
        return -1
    else:
        return 0


def mapping_result(years):
    if years <= 1:
        return "1"
    elif years > 1 and years <= 3:
        return "2"
    elif years > 3 and years <= 5:
        return "3"
    else:
        return "4"


date_s = dt.date.today()
date_s_ttp = date_s.timetuple()[:3]


def calculate(id_category):
    if id_category == 1:
        key_id, key_fd, key_tb = "fund_id", "foundation_date", "fund_info"
    else:
        key_id, key_fd, key_tb = "org_id", "found_date", "org_info"
    sql_fd = "SELECT {id}, {fd} FROM {tb}".format(id=key_id, fd=key_fd, tb=key_tb)

    d_fd = pd.read_sql(sql_fd, engine_rd).dropna()
    d_fd[key_fd] = d_fd[key_fd].apply(lambda x: x.timetuple()[:3])
    ids_f = d_fd[key_id].tolist()
    t_f = d_fd[key_fd].tolist()

    foundation_days = [str((date_s - dt.date(*x)).days) for x in t_f]
    foundation_years = [
        ((date_s_ttp[0] - x[0]) * 12 + (date_s_ttp[1] - x[1]) + modify_full_month(date_s_ttp[2] - x[2])) / 12 for x in
        t_f]
    foundation_year_ranges = [mapping_result(x) for x in foundation_years]

    datas = []
    for i in range(len(ids_f)):
        res_tmp = {"id": ids_f[i], "id_category": str(id_category), "statistic_date": str(date_s),
                   "foundation_days": foundation_days[i], "foundation_days_range": foundation_year_ranges[i]
                   }
        datas.append(res_tmp)

    result = pd.DataFrame.from_records(datas)
    return result


def main():
    tic("Calculating time_index...")
    fd_f, fd_o = [calculate(x) for x in [1, 2]]
    for d_fd in [fd_f, fd_o]:
        io.to_sql("time_index", engine_wt, d_fd, "update")
    tic("Done...")

if __name__ == "__main__":
    main()
