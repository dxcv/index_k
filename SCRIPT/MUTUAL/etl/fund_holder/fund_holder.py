import re
import pandas as pd
import numpy as np
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput

ENGINE_RD = cfg.load_engine()["2Gb"]


def sub_wrong_to_none(x):
    s = re.sub("\s|-| |--|---|亿份|%", "", x)
    if s == "":
        return None
    else:
        return s


class StreamsMain:
    """
        清洗d_fund_holder
    """

    hold_type_dict = {1: "机构", 2: "个人", 3: "内部"}

    @classmethod
    def stream_020002(cls):
        sql = "\
                SELECT fund_id,statistic_date,holder_type,proportion_held,share_held,holder_num,total_share \
                FROM crawl_public.d_fund_holder  \
                WHERE data_source = '020002' \
                AND fund_id IN (SELECT matched_id FROM \
                base_public.id_match where id_type = 1 AND is_used = 1)"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "proportion_held": lambda x: sub_wrong_to_none(x),
            "total_share": lambda x: sub_wrong_to_none(x),
            "holder_type": lambda x: cls.hold_type_dict.get(x),

        })

        vm2 = transform.ValueMap({

            "total_share": lambda x: float(x) if type(x) is str else x,
            "proportion_held": lambda x: float(x) / 100 if type(x) is str else x
        })

        vm3 = transform.ValueMap({
            "proportion_held": lambda x: round(x, 6) if type(x) is float else x
        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "proportion_held": "proportion_held",
            "total_share": "total_share",
            "holder_type": "holder_type",
            "statistic_date": "statistic_date"
        })

        s = Stream(inp, transform=[vm, vm2, vm3, sk])
        return s

    @classmethod
    def stream_020001(cls):
        sql = "\
                SELECT fund_id, statistic_date, holder_type \
                FROM crawl_public.d_fund_holder  \
                WHERE data_source = '020001' \
                AND fund_id IN (SELECT matched_id FROM \
                base_public.id_match where id_type = 1 AND is_used = 1) \
                AND fund_id NOT in \
                (SELECT DISTINCT fund_id FROM crawl_public.d_fund_holder \
                WHERE data_source = '020002')"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "holder_type": lambda x: cls.hold_type_dict.get(x)

        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "holder_type": "holder_type",
            "statistic_date": "statistic_date"
        })

        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020003(cls):
        sql = "\
                SELECT fund_id, statistic_date, holder_type, share_held  \
                FROM crawl_public.d_fund_holder  \
                WHERE data_source = '020003' \
                AND fund_id IN (SELECT matched_id FROM \
                base_public.id_match where id_type = 1 AND is_used = 1) \
                AND fund_id NOT in \
                (SELECT DISTINCT fund_id FROM crawl_public.d_fund_holder \
                WHERE data_source in('020001', '020002'))"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "holder_type": lambda x: cls.hold_type_dict.get(x),
            "share_held": lambda x: sub_wrong_to_none(x)

        })

        vm2 = transform.ValueMap({
            "share_held": lambda x: float(x) / 10e7 if type(x) is str else x
        })

        vm3 = transform.ValueMap({
            "share_held": lambda x: round(x, 6) if type(x) is float else x


        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "holder_num": "holder_num",
            "statistic_date": "statistic_date",
            "holder_type": "holder_type",
            "share_held": "share_held"

        })

        s = Stream(inp, transform=[vm, vm2, vm3, sk])
        return s

    @classmethod
    def completion_020001(cls):
        sql = "SELECT fund_id, statistic_date, holder_type, share_held \
                FROM crawl_public.d_fund_holder  \
                WHERE data_source = '020001' \
                AND fund_id IN (SELECT DISTINCT fund_id FROM base_public.fund_holder)"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "share_held": lambda x: sub_wrong_to_none(x),
            "holder_type": lambda x: cls.hold_type_dict.get(x)

        })

        vm2 = transform.ValueMap({
            "share_held": lambda x: round(x, 6) if type(x) is float else x
        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "share_held": "share_held",
            "holder_type": "holder_type",
            "statistic_date": "statistic_date"
        })

        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def completion_020003(cls):
        sql = "\
                SELECT fund_id,statistic_date,holder_type,holder_num \
                FROM crawl_public.d_fund_holder  \
                WHERE data_source = '020003' \
                AND fund_id IN (SELECT matched_id FROM \
                base_public.id_match where id_type = 1 AND is_used = 1)"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "holder_type": lambda x: cls.hold_type_dict.get(x),

        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "holder_num": "holder_num",
            "holder_type": "holder_type",
            "statistic_date": "statistic_date"
        })

        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def completion_020003_next(cls):
        sql = "\
                SELECT fund_id,statistic_date,holder_type,total_share,proportion_held  \
                FROM crawl_public.d_fund_holder   \
                WHERE data_source = '020003'  \
                AND fund_id IN (SELECT matched_id FROM \
                base_public.id_match where id_type = 1 AND is_used = 1) \
                AND fund_id NOT IN (SELECT DISTINCT fund_id FROM crawl_public.d_fund_holder \
                WHERE data_source = '020002')"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "proportion_held": lambda x: sub_wrong_to_none(x),
            "holder_type": lambda x: cls.hold_type_dict.get(x),
            "total_share": lambda x: sub_wrong_to_none(x)
        })

        vm2 = transform.ValueMap({

            "proportion_held": lambda x: float(x) / 100 if type(x) is str else x,
            "total_share": lambda x: float(x) / 10e7 if type(x) is str else x
        })

        vm3 = transform.ValueMap({
            "proportion_held": lambda x: round(x, 6) if type(x) is float else x,
            "total_share": lambda x: round(x, 6) if type(x) is float else x
        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "proportion_held": "proportion_held",
            "holder_type": "holder_type",
            "statistic_date": "statistic_date",
            "total_share": "total_share"
        })

        s = Stream(inp, transform=[vm, vm2, vm3, sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.stream_020002(), cls.stream_020003(), cls.stream_020001()]
        c = Confluence(*streams, on=["fund_id", "statistic_date", "holder_type"])
        return c

    @classmethod
    def fund_name(cls):
        sql = "UPDATE base_public.fund_holder AS fh , \
        (SELECT fund_id,fund_name from base_public.fund_info) AS fi \
        SET fh.fund_name = fi.fund_name \
        WHERE fh.fund_id = fi.fund_id"
        ENGINE_RD.execute(sql)

    @classmethod
    def proportion_held(cls):
        sql = "SELECT fh.fund_id, fh.statistic_date, fh.holder_type, fh.proportion_held  \
                FROM base_public.fund_holder AS fh \
                JOIN(SELECT DISTINCT fund_id,statistic_date FROM base_public.fund_holder WHERE proportion_held is NULL)AS b \
                ON fh.fund_id = b.fund_id and fh.statistic_date = b.statistic_date  \
                JOIN (SELECT fund_id, statistic_date FROM base_public.fund_holder GROUP BY fund_id, statistic_date HAVING COUNT(1) >= 2) tmp \
                ON fh.fund_id = tmp.fund_id AND fh.statistic_date = tmp.statistic_date"
        inp = MysqlInput(ENGINE_RD, sql)
        kong = inp.dataframe
        if kong.empty:
            pass
        else:
            ids = kong.fillna("空")
            df_all = ids.groupby(['fund_id', 'statistic_date'], axis=0)
            frames = []
            for i in df_all:
                df = i[1]
                df2 = df.iloc[:, [2, 3]]
                a = np.array(df2)
                list = a.tolist()
                id = df.iloc[0][0]
                date = df.iloc[0][1]
                alist = df["proportion_held"].tolist()
                for i in range(len(alist) - 1, -1,
                               -1):
                    if alist[i] == '空':
                        alist.pop(i)
                sum_all = sum(alist)
                if sum_all == 0:
                    pass
                else:
                    for i in list:
                        type = i[0]
                        n = i[1]
                        if n == '空':
                            a1 = 1 - sum_all
                            if a1 < 0:
                                a1 = 0
                            ll = [id, date, type, a1]
                            dd = pd.DataFrame(ll)
                            ee = dd.T
                            ee.columns = ["fund_id", "statistic_date", "holder_type", "proportion_held"]
                            frames.append(ee)
            result = pd.concat(frames)
            io.to_sql("base_public.fund_holder", ENGINE_RD, result, type='update')

    @classmethod
    def calculate(cls):
        sql = "select fund_id, statistic_date, holder_type, proportion_held, total_share  FROM base_public.fund_holder \
                WHERE share_held is NULL AND proportion_held >0 AND total_share is not NULL"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "share_held": (lambda x, y: x * y, "proportion_held", "total_share")

        })

        vm2 = transform.ValueMap({
            "share_held": lambda x:  round(x, 6) if type(x) is float else x
        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "share_held": "share_held",
            "holder_type": "holder_type",
            "statistic_date": "statistic_date"
        })

        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def calculate2(cls):
        sql = "select fund_id, statistic_date, holder_type, share_held, total_share  FROM base_public.fund_holder \
                WHERE  total_share is not NULL AND share_held >0 AND proportion_held is NULL"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "proportion_held": (lambda x, y: x / y, "share_held", "total_share")

        })

        vm2 = transform.ValueMap({
            "proportion_held": lambda x: round(x, 6) if type(x) is float else x
        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            "proportion_held": "proportion_held",
            "holder_type": "holder_type",
            "statistic_date": "statistic_date"
        })

        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base_public.fund_holder", ENGINE_RD, df.dataframe, type="update")
        df2 = cls.completion_020001()
        df22 = df2.flow()[0]
        sql = "SELECT fund_id,statistic_date,holder_type FROM base_public.fund_holder"
        inp = MysqlInput(ENGINE_RD, sql)
        ids = inp.dataframe
        df_last = pd.merge(ids, df22, on=['fund_id', 'statistic_date', 'holder_type'])
        io.to_sql("base_public.fund_holder", ENGINE_RD, df_last, type="update")
        df3 = cls.completion_020003()
        df33 = df3.flow()[0]
        df_020003 = pd.merge(ids, df33, on=['fund_id', 'statistic_date', 'holder_type'])
        df4 = df_020003.iloc[:, [0, 1, 2, 3]].dropna()
        io.to_sql("base_public.fund_holder", ENGINE_RD, df4, type="update")
        df6 = cls.completion_020003_next()
        df66 = df6.flow()[0]
        io.to_sql("base_public.fund_holder", ENGINE_RD, df66, type="update")
        cls.fund_name()
        cls.proportion_held()
        df7 = cls.calculate()
        df77 = df7.flow()[0]
        io.to_sql("base_public.fund_holder", ENGINE_RD, df77, type="update")
        df8 = cls.calculate2()
        df88 = df8.flow()[0]
        io.to_sql("base_public.fund_holder", ENGINE_RD, df88, type="update")


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.fund_holder_test", "base_public.fund_holder", ENGINE_RD,
                            cols_excluded={"fund_id", "data_source", "entry_time", "source_id", "update_time"})
    t.result.to_csv(os.path.expanduser("~/Desktop/fund_holder_{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()





