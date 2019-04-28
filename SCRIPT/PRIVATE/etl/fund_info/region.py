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


def region():
    sql = "SELECT fom.fund_id, GROUP_CONCAT(o.region SEPARATOR ',') as region \
            FROM ( \
            SELECT fund_id,org_id FROM base.`fund_org_mapping` WHERE org_type_code = 1 \
            ) as fom \
            LEFT JOIN ( \
            SELECT org_id,region from base.org_info  \
            ) as o ON fom.org_id = o.org_id \
            where region is not NULL \
            GROUP BY fom.fund_id"
    inp = MysqlNativeInput(engine_base, sql)
    df = inp.dataframe
    df2 = df[["fund_id", "region"]]
    nav = to_list(df2)
    list_dict = turn_dict(nav)
    return list_dict


region = region()


class StreamsMain:

    @classmethod
    def stream_000001(cls):
        sql = "SELECT fund_id FROM base.fund_info"
        inp = MysqlNativeInput(engine_base, sql)
        vm = transform.ValueMap({
            "region": (lambda x: region.get(x), "fund_id"),

        })

        km = transform.MapSelectKeys(
            {
                "fund_id": "fund_id",
                "region": "region"
            }
        )

        stream = Stream(inp, transform=[vm, km])
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
    # io.to_sql("data_test.fund_info_test001", engine_base, b, type="update")


if __name__ == "__main__":
    main()





