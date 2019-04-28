import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput
from pypinyin import pinyin as py, Style as py_style

ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    BACKGROUND = {'公募': '公募', '其它': "其他", '券商': "券商", '海外': "海外",
                  "民间": "民间", "学者": "学者", '实业': "实业", '保险': "保险",
                  '媒体': "媒体", '期货': '期货'}

    @classmethod
    def sub_wrong_to_none(cls, x):
        s = re.sub("\s|-| |--|---|万股|%", "", x)
        if s == "":
            return None
        else:
            return s

    @classmethod
    def stream_010001(cls):
        """
            清洗 x_org_executive_info;

        """

        sql = "SELECT im.matched_id, `name`, qualifying_way FROM crawl_private.x_org_executive_info tb_main " \
              "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.x_org_executive_info GROUP BY " \
              "person_id) tb_latest " \
              "ON tb_main.version = tb_latest.latest_ver AND tb_main.person_id = tb_latest.person_id " \
              "JOIN base.id_match im ON im.source_id = tb_main.person_id " \
              "AND im.id_type = 3 AND im.source = '010001' AND im.is_used = 1 "

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "name": (lambda x: re.sub("（.*）", "", x), "name"),
            "qualifying_way": lambda x: {"通过考试": "通过考试", "资格认定": "资格认定"}.get(x) if type(x) is str else x,
            "person_name_py": (lambda x: "".join([x[0] for x in py(x, style=py_style.FIRST_LETTER)]).upper(), "name"),
        })

        vm2 = transform.ValueMap({
            "is_fund_qualification": (lambda x: int(bool(x)), "qualifying_way")
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            'qualifying_way': 'fund_qualification_way',
            'name': 'person_name',
            "person_name_py": "person_name_py",
            "is_fund_qualification": "is_fund_qualification"
        })
        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def stream_y_person_info(cls):
        """
            清洗 y_person_info;

        """

        sql = "SELECT person_id, person_name, gender, background, education," \
              "graduate_school, investment_years FROM crawl_private.y_person_info"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "person_name_py": (lambda x: "".join([x[0] for x in py(x, style=py_style.FIRST_LETTER)]).upper(), "person_name"),
        })

        sk = transform.MapSelectKeys({
            "person_id": "person_id",
            'person_name': 'person_name',
            "person_name_py": "person_name_py",
            "gender": "gender",
            "background": "background",
            "graduate_school": "graduate_school",
            "investment_years": "investment_years"
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020001(cls):
        """
            清洗 d_person_info;

        """


        sql = "SELECT im.matched_id, person_name, background FROM crawl_private.d_person_info tb_main " \
              "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.d_person_info GROUP BY person_id) tb_latest " \
              "ON tb_main.version = tb_latest.latest_ver AND tb_main.person_id = tb_latest.person_id " \
              "JOIN base.id_match im ON im.source_id = tb_main.person_id " \
              "AND im.id_type = 3 AND im.source = '020001' AND im.is_used = 1 "

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "background": lambda x: cls.BACKGROUND.get(x) if type(x) is str else x,
            "person_name_py": (lambda x: "".join([x[0] for x in py(x, style=py_style.FIRST_LETTER)]).upper(), "person_name"),
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            'background': 'background',
            'person_name': 'person_name',
            "person_name_py": "person_name_py"
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020001_op(cls):
        """
            清洗 d_org_person;

        """
        sql = "SELECT im.matched_id, person_name FROM crawl_private.d_org_person tb_main " \
              "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.d_org_person GROUP BY person_id) tb_latest " \
              "ON tb_main.version = tb_latest.latest_ver AND tb_main.person_id = tb_latest.person_id " \
              "JOIN base.id_match im ON im.source_id = tb_main.person_id " \
              "AND im.id_type = 3 AND im.source = '020001' AND im.is_used = 1 "

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "person_name_py": (lambda x: "".join([x[0] for x in py(x, style=py_style.FIRST_LETTER)]).upper(), "person_name"),
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            'person_name': 'person_name',
            "person_name_py": "person_name_py"
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020002(cls):
        """
            清洗 d_person_info;

        """
        sql = "SELECT im.matched_id, person_name, background FROM crawl_private.d_person_info tb_main " \
              "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.d_person_info GROUP BY person_id) tb_latest " \
              "ON tb_main.version = tb_latest.latest_ver AND tb_main.person_id = tb_latest.person_id " \
              "JOIN base.id_match im ON im.source_id = tb_main.person_id " \
              "AND im.id_type = 3 AND im.source = '020002' AND im.is_used = 1 "

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "background": lambda x: cls.BACKGROUND.get(x) if type(x) is str else x,
            "person_name_py": (lambda x: "".join([x[0] for x in py(x, style=py_style.FIRST_LETTER)]).upper(), "person_name"),
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            'background': 'background',
            'person_name': 'person_name',
            "person_name_py": "person_name_py"
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020003(cls):
        """
            清洗 d_person_info;

        """


        sql = "SELECT im.matched_id, person_name, background FROM crawl_private.d_person_info tb_main " \
              "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.d_person_info GROUP BY person_id) tb_latest " \
              "ON tb_main.version = tb_latest.latest_ver AND tb_main.person_id = tb_latest.person_id " \
              "JOIN base.id_match im ON im.source_id = tb_main.person_id " \
              "AND im.id_type = 3 AND im.source = '020003' AND im.is_used = 1 "

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "background": lambda x: cls.BACKGROUND.get(x) if type(x) is str else x,
            "person_name_py": (lambda x: "".join([x[0] for x in py(x, style=py_style.FIRST_LETTER)]).upper(), "person_name"),
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            'background': 'background',
            'person_name': 'person_name',
            "person_name_py": "person_name_py"
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.stream_y_person_info(), cls.stream_010001(), cls.stream_020001(), cls.stream_020002(),
                   cls.stream_020001_op(), cls.stream_020003()]
        c = Confluence(*streams, on=["person_id"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base.person_info", ENGINE_RD, df.dataframe, type="update")


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.person_info_test", "base.person_info", ENGINE_RD,
                            cols_excluded={'person_id', 'person_name', 'entry_time', 'update_time',
                                           'person_name_py'})
    t.result.to_csv(os.path.expanduser("~/Desktop/person_info_{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()
