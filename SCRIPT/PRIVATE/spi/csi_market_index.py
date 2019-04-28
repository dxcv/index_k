import urllib
import pandas as pd
from utils.database import io, config as cfg
engine = cfg.load_engine()["2Gb"]


# def fetch_xls(url):
#     io = urllib.request.urlopen(url)
#     df = pd.read_excel(io, header=1)
#     df = df[df.columns[:4]]
#     df.columns = ["index_code", "index_name", "statistic_date", "index_level"]
#     return df


def fetch_parse_xlx(url):
    io = urllib.request.urlopen(url)
    df = pd.read_excel(io)
    df = df[["收盘Close", "日期Date"]]
    df.columns = ["cbi", "statistic_date"]
    return df


def main():
    # df = fetch_xls("ftp://115.29.204.48/webdata/spperf.xls")
    # result = df.ix[df["index_code"] == "H11001", ["index_level", "statistic_date"]]
    # result.columns = ["cbi", "statistic_date"]

    #  http://www.csindex.com.cn/zh-CN/indices/index-detail/H11001
    result = fetch_parse_xlx("http://www.csindex.com.cn/uploads/file/autofile/perf/H11001perf.xls")
    io.to_sql("market_index", engine, result, "update")



if __name__ == "__main__":
    main()
