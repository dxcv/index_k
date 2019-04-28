import datetime as dt
import pandas as pd
from sqlalchemy import create_engine
from utils.crawl import wind_market_index as wmi, const as wcm
from utils.database import io, config as cfg

__engine_rd = create_engine("mysql+pymysql://jr_dev_yu:jr_dev_yu@db.chfdb.cc:4171/test_gt",
                            connect_args={"charset": "utf8"})
__engine_rd = cfg.load_engine()["2Gb"]
__previous_day = 5


def main():
    col_mp = {v: k for k, v in wcm.SWS.price.items()}
    sws = wmi.SWSIndex(dt.date(2017, 12, 31), dt.date(2017, 12, 1))
    data = sws._fetchindex(",".join(wcm.SWS.price.values()))
    df = pd.DataFrame(data.Data).T
    cols = [col_mp[x] for x in data.Codes]
    df.columns = cols
    df["statistic_date"] = [x for x in data.Times]
    for col in cols:
        tmp = df[[col, "statistic_date"]]
        tmp.columns = ["index_value", "statistic_date"]
        tmp["index_id"] = col
        io.to_sql("sws_index", __engine_rd, tmp)


if __name__ == "__main__":
    main()
