import numpy as np
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlNativeInput

engine_t = cfg.load_engine()["etl_base_test"]
engines = cfg.load_engine()
engine_base, engine_crawl_private = engines["2Gb"], engines["2Gcpri"]


def turn_dict(lst):
    global key, value
    dic = {}
    if all([not isinstance(item, list) for item in lst]):
        if len(lst) == 2:
            key, value = lst
        elif len(lst) == 1:
            key = lst[0]
            value = ''
        elif len(lst) == 3:
            key = lst[0]
            value = lst[1]
        dic[key] = value
    else:
        for item in lst:
            subdic = turn_dict(item)
            dic.update(subdic)
    return dic


def to_list(df):
    a = np.array(df)
    vv = a.tolist()
    return vv


def init_nav():
    sql = "SELECT bb.fund_id,bb.nav FROM (" \
          "SELECT lm.mm,lm.fund_id,lm.source_id,co.nav FROM (" \
          "SELECT MIN(statistic_date) AS mm,fund_id,source_id " \
          "FROM base.fund_nv_data_source_copy2 " \
          "WHERE source_id NOT IN ('03','04','05') AND fund_id IN (SELECT fund_id FROM base.fund_info) " \
          "GROUP BY fund_id,source_id ) AS lm " \
          "JOIN base.fund_nv_data_source_copy2 AS co " \
          "ON lm.mm = co.statistic_date AND lm.fund_id=co.fund_id AND lm.source_id=co.source_id) AS bb " \
          "WHERE bb.nav>90"

    inp = MysqlNativeInput(engine_base, sql)
    df = inp.dataframe
    df2 = df[["fund_id", "nav"]]
    nav = to_list(df2)
    nav2 = []
    for i in nav:
        jr = i[0]
        n = i[1]
        if 900 > n > 90:
            c = 100
            t = [jr, c]
            nav2.append(t)
        elif n > 900:
            c = 1000
            t = [jr, c]
            nav2.append(t)
        else:
            c = 1
            t = [jr, c]
            nav2.append(t)
    list_dict = turn_dict(nav2)
    return list_dict


init_nav = init_nav()


class StreamsMain:
    @classmethod
    def stream_000001(cls):
        sql = "SELECT fund_id FROM base.fund_info"
        inp = MysqlNativeInput(engine_base, sql)
        vm = transform.ValueMap({
            "init_nav": (lambda x: init_nav.get(x), "fund_id"),

        })

        vm2 = transform.ValueMap({
            "init_nav": lambda x: 1 if np.isnan(x) else x

        })

        km = transform.MapSelectKeys(
            {
                "fund_id": "fund_id",
                "init_nav": "init_nav"
            }
        )

        stream = Stream(inp, transform=[vm, vm2, km])
        return stream

    @classmethod
    def confluence(cls):
        streams = [cls.stream_000001()]
        c = Confluence(*streams, on=["fund_id"])
        return c


def main():
    name = StreamsMain.confluence()
    a = name.dataframe
    b = a.reset_index(drop=True)
    io.to_sql("base.fund_info", engine_base, b, type="update")


if __name__ == "__main__":
    main()
