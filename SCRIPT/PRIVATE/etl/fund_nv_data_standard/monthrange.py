import pandas as pd
from utils.database import config as cfg, io
import calendar as cld
import datetime as dt
from utils.script import swanv
from utils.script.scriptutils import tic

engines = cfg.load_engine()
engine_rd = engines["2Gb"]
engine_wt = engines["2Gb"]


engine_wt.execute("UPDATE `fund_nv_data_standard` SET statistic_date_m = NULL WHERE statistic_date_m IS NOT NULL")


sql_sd = "SELECT fund_id, statistic_date FROM fund_nv_data_standard"
df = pd.read_sql(sql_sd, engine_rd)

df["ym"] = df["statistic_date"].apply(lambda x: (x.year, x.month))

df = df.sort_values(by=["fund_id", "statistic_date"], ascending=[True, False])

tmp = df.drop_duplicates(subset=["fund_id", "ym"])
tmp["statistic_date_m"] = tmp["statistic_date"].apply(lambda x: dt.date(x.year, x.month, cld.monthrange(x.year, x.month)[1]))

io.to_sql("fund_nv_data_standard", engine_wt, tmp[["fund_id", "statistic_date", "statistic_date_m"]], "update")


# sql_sd = "SELECT fund_id, statistic_date, statis"