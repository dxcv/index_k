import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    sws = {"其它行业": "综合",
           "农、林、牧、渔业": "农林牧渔",
           "制造业": "轻工制造",
           "批发和零售业": "商业贸易",
           "电力、热力、燃气及水生产和供应业": "公用事业",
           "金融业": "非银金融",
           "建筑业": "建筑材料",
           "水利、环境和公共设施管理业": "公用事业",
           "采矿业": "采掘",
           "房地产业": "房地产",
           "信息传输、软件和信息技术服务业": "计算机",
           "卫生和社会工作": "公用事业",
           "文化、体育和娱乐业": "休闲服务",
           "科学研究和技术服务业": "公用事业",
           "交通运输、仓储和邮政业": "交通运输",
           "机械、设备、仪表": "机械设备",
           "金融、保险业": "非银金融",
           "食品、饮料": "食品饮料",
           "批发和零售贸易": "商业贸易",
           "医药、生物制品": "医药生物",
           "租赁和商务服务业": "休闲服务",
           "合计": "ERROR",
           "采掘业": "采掘",
           "信息技术业": "计算机",
           "电子": "电子",
           "金属、非金属": "电子",
           "电力、煤气及水的生产和供应业": "公用事业",
           "综合": "综合",
           "石油、化学、塑胶、塑料": "化工",
           "社会服务业": "公用事业",
           "交通运输、仓储业": "交通运输",
           "住宿和餐饮业": "食品饮料",
           "教育": "综合",
           "传播与文化产业": "休闲服务",
           "纺织、服装、皮毛": "纺织服装",
           "综合类": "综合",
           }

    wd = {"其它行业": "综合",
          "农、林、牧、渔业": "农、林、渔、牧业",
          "制造业": "制造业",
          "批发和零售业": "批发和零售业",
          "电力、热力、燃气及水生产和供应业": "电力、热力、燃气及水生产和供应业",
          "金融业": "金融业",
          "建筑业": "建筑业",
          "水利、环境和公共设施管理业": "水利、环境和公共设施管理业",
          "采矿业": "采矿业",
          "房地产业": "房地产业",
          "信息传输、软件和信息技术服务业": "信息传输、软件和信息技术服务业",
          "卫生和社会工作": "卫生和社会工作",
          "文化、体育和娱乐业": "文化、体育和娱乐业",
          "科学研究和技术服务业": "科学研究和技术服务业",
          "交通运输、仓储和邮政业": "交通运输、仓储和邮政业",
          "机械、设备、仪表": "制造业",
          "金融、保险业": "金融业",
          "食品、饮料": "住宿和餐饮业",
          "批发和零售贸易": "批发和零售业",
          "医药、生物制品": "科学研究和技术服务业",
          "租赁和商务服务业": "租赁和商务服务业",
          "合计": "ERROR",
          "采掘业": "采矿业",
          "信息技术业": "信息传输、软件和信息技术服务业",
          "电子": "科学研究和技术服务业",
          "金属、非金属": "采矿业",
          "电力、煤气及水的生产和供应业": "电力、热力、燃气及水生产和供应业",
          "综合": "综合",
          "石油、化学、塑胶、塑料": "制造业",
          "社会服务业": "卫生和社会工作",
          "交通运输、仓储业": "交通运输、仓储和邮政业",
          "住宿和餐饮业": "住宿和餐饮业",
          "教育": "教育",
          "传播与文化产业": "文化、体育和娱乐业",
          "纺织、服装、皮毛": "制造业",
          "综合类": "综合"}

    @classmethod
    def sub_wrong_to_none(cls, x):
        s = re.sub("\s|-| |--|---|%", "", x)
        if s == "":
            return None
        else:
            return s

    @classmethod
    def stream_020001_sws(cls):
        """
            清洗 d_org_portfolio_industry;

        """

        sql = " \
                SELECT idm.matched_id, doi.data_source, doi.statistic_date, doi.type, \
                doi.proportion, doi.asset_scale FROM (SELECT matched_id,source_id from base_public.id_match \
                WHERE data_source = '020002' AND is_used = 1 and id_type = 2) as idm \
                JOIN crawl_public.d_org_portfolio_industry AS doi \
                ON doi.org_id = idm.source_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "asset_scale": lambda x: float(x)/10e7 if type(x) is str else x,
            "type": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "proportion": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
        })

        vm2 = transform.ValueMap({
            "proportion": lambda x: round(float(x) / 100, 6) if type(x) is str else x,
            "style": 1,
            "type": lambda x: cls.sws.get(x) if type(x) is str else x
        })

        vm3 = transform.ValueMap({
            "scale": (lambda x, y: x * y, "asset_scale", "proportion"),
        })

        vm4 = transform.ValueMap({
            "asset_scale": lambda x: round(x, 6) if type(x) is float else x,
            "scale": lambda x: round(x, 6) if type(x) is float else x
        })

        sk = transform.MapSelectKeys({
            "matched_id": "org_id",
            'proportion': 'proportion',
            'statistic_date': 'statistic_date',
            'asset_scale': 'asset_scale',
            'scale': 'scale',
            'style': 'style',
            'type': 'type',
        })
        s = Stream(inp, transform=[vm, vm2, vm3, vm4, sk])
        return s

    @classmethod
    def stream_020001_wd(cls):
        """
            清洗 d_org_portfolio_industry;

        """

        sql = " \
                SELECT idm.matched_id, doi.data_source, doi.statistic_date, doi.type, \
                doi.proportion, doi.asset_scale FROM (SELECT matched_id,source_id from base_public.id_match \
                WHERE data_source = '020002' AND is_used = 1 and id_type = 2) as idm \
                JOIN crawl_public.d_org_portfolio_industry AS doi \
                ON doi.org_id = idm.source_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "asset_scale": lambda x: float(x)/10e7 if type(x) is str else x,
            "proportion": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "type": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
        })

        vm2 = transform.ValueMap({
            "proportion": lambda x: round(float(x) / 100, 6) if type(x) is str else x,
            "style": 2,
            "type": lambda x: cls.wd.get(x) if type(x) is str else x
        })

        vm3 = transform.ValueMap({
            "scale": (lambda x, y: x * y, "asset_scale", "proportion"),
        })

        vm4 = transform.ValueMap({
            "asset_scale": lambda x: round(x, 6) if type(x) is float else x,
            "scale": lambda x: round(x, 6) if type(x) is float else x,
        })

        sk = transform.MapSelectKeys({
            "matched_id": "org_id",
            'proportion': 'proportion',
            'statistic_date': 'statistic_date',
            'asset_scale': 'asset_scale',
            'scale': 'scale',
            'style': 'style',
            'type': 'type',
        })
        s = Stream(inp, transform=[vm, vm2, vm3, vm4, sk])
        return s


    @classmethod
    def org_name(cls):
        sql = "UPDATE base_public.org_portfolio_industry AS fh , \
        (SELECT org_id, org_name from base_public.org_info) AS fi \
        SET fh.org_name = fi.org_name \
        WHERE fh.org_id = fi.org_id"
        ENGINE_RD.execute(sql)

    @classmethod
    def confluence(cls):
        streams = [cls.stream_020001_sws(), cls.stream_020001_wd()]
        c = Confluence(*streams, on=["org_id", "statistic_date", "style", "type"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence().dataframe
        df_next = df.loc[(df["type"] != "ERROR") & (df["proportion"] >= 0.00001)]
        io.to_sql("base_public.org_portfolio_industry", ENGINE_RD, df_next, type="update")
        cls.org_name()


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.org_portfolio_industry_test", "base_public.org_portfolio_industry", ENGINE_RD,
                            cols_excluded={'org_id', 'statistic_date', 'org_name', 'entry_time', 'update_time',
                                           'data_source'})
    t.result.to_csv(os.path.expanduser("~/Desktop/org_portfolio_industry_test_{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()



