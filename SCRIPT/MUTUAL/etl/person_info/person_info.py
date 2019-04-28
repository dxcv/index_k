import re
from datetime import datetime
from utils.database import io, config as cfg
from utils.etlkit.core import transform
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE = cfg.load_engine()["2Gb"]


def sub_wrong_to_none(x):
    s = re.sub("\s| |--|---|　", "", x)
    if s == "":
        return None
    else:
        return s


def str_time(zwtime):
    try:
        now = datetime.now()
        t = datetime.strptime(zwtime, "%Y-%m-%d")
        a = (now - t).days
        n = int(a/365)
        if n == 0:
            r = str(a)+"天"
            return r
        else:
            r = str(n)+"年"+str(a-n*365-int(n/4))+"天"
            return r
    except BaseException:
        return None


class StreamsMain:

    MSTER_STRATEGY = {"股票型": "股票型基金",
                      "债券型": "债券型基金",
                      "保本型": "混合型基金",
                      "理财型": "货币型基金",
                      "QDII": "QDII基金",
                      "混合型": "混合型基金",
                      "货币型": "货币型基金",
                      }

    @classmethod
    def fetch_gender(cls, string):
        if string is None:
            return None
        else:
            d_gender = {
                "先生": "男",
                "女士": "女",
                "男": "男",
                "女": "女",
            }
            patt = "(?P<gender>(先生|女士))"
            match = re.search(patt, string)

        if match is not None:
            return d_gender[match.groupdict()["gender"]]
        else:
            return None

    @classmethod
    def fetch_education(cls, string):
        if string is None:
            return None
        d_education = {
            "博士后": "博士后",
            "博士": "博士",
            "MBA": "MBA",
            "硕士": "硕士",
            "研究生": "硕士",
            "大学": "学士",
            "学士": "学士",
            "本科": "学士",
            "大专": "大专",
            "初中": "初中"
        }
        pattern = "(MBA|博士后|博士|硕士|研究生|学士|本科|大学|大专|初中)"
        match = re.findall(pattern, string)
        if len(match) != 0:
            order = {
                "初中": 0,
                "大专": 1,
                "大学": 2,
                "本科": 3,
                "学士": 4,
                "研究生": 5,
                "硕士": 6,
                "博士": 7,
                "博士后": 8,
                "MBA": 9
            }
            max_education = max(match, key=lambda x: order[x])
            return d_education[max_education]
        else:
            return None

    @classmethod
    def stream_020001(cls):
        sql = "SELECT idm.matched_id, idm.data_source, dpi.person_name, dpi.master_strategy, \
                dpi.gender, dpi.education, dpi.tenure_date FROM ( \
                SELECT * FROM base_public.id_match WHERE id_type = 3 AND \
                 is_used = 1 AND data_source = '020001' GROUP BY matched_id ) as idm \
                JOIN \
                crawl_public.d_person_info as dpi \
                ON idm.source_id = dpi.person_id"

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            "master_strategy": lambda x: sub_wrong_to_none(x) if type(x) is str else x,
            "education": lambda x: sub_wrong_to_none(x) if type(x) is str else x,
            "gender": lambda x: sub_wrong_to_none(x) if type(x) is str else x,
            "investment_period": (lambda x: str_time(str(x)), "tenure_date")
        })

        vm2 = transform.ValueMap({
            "master_strategy": lambda x: cls.MSTER_STRATEGY.get(x) if type(x) is str else x
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            "person_name": "person_name",
            "master_strategy": "master_strategy",
            "education": "education",
            "gender": "gender",
            "investment_period": "investment_period",
            "data_source": "data_source"
        })

        s = Stream(inp, transform=[vm, vm2,  sk])
        return s

    @classmethod
    def stream_020002(cls):
        sql = "SELECT idm.matched_id, idm.data_source, dpi.person_name, dpi.master_strategy, \
                dpi.gender, dpi.education, dpi.tenure_date FROM ( \
                SELECT * FROM base_public.id_match WHERE id_type = 3 AND \
                 is_used = 1 AND data_source = '020002' GROUP BY matched_id ) as idm \
                JOIN \
                crawl_public.d_person_info as dpi \
                ON idm.source_id = dpi.person_id"

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            "master_strategy": lambda x: sub_wrong_to_none(x) if type(x) is str else x,
            "education": lambda x: sub_wrong_to_none(x) if type(x) is str else x,
            "gender": lambda x: sub_wrong_to_none(x) if type(x) is str else x,
            "investment_period": (lambda x: str_time(str(x)), "tenure_date")
        })

        vm2 = transform.ValueMap({
            "master_strategy": lambda x: cls.MSTER_STRATEGY.get(x) if type(x) is str else x
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            "person_name": "person_name",
            "master_strategy": "master_strategy",
            "education": "education",
            "gender": "gender",
            "investment_period": "investment_period",
            "data_source": "data_source"
        })

        s = Stream(inp, transform=[vm, vm2,  sk])
        return s

    @classmethod
    def stream_030001(cls):

        edu = {"硕士研究生": "硕士",
               "博士研究生": "博士",
               "本科": "本科",
               "大专": "大专"}

        sql = "SELECT idm.matched_id, idm.data_source, x.person_name, x.gender, x.education \
                FROM (SELECT * FROM base_public.id_match WHERE id_type = 3 AND \
                 is_used = 1 AND data_source = '030001' GROUP BY matched_id ) as idm \
                JOIN \
                crawl_public.x_person_info as x \
                ON idm.source_id = x.person_id"

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            "education": lambda x: edu.get(x) if type(x) is str else x
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            "education": "education",
            "gender": "gender",
            "person_name": "person_name",
            "data_source": "data_source"
        })

        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_resume_020001(cls):
        """
            清洗 person_info.education;

        """

        sql = "SELECT idm.matched_id, idm.data_source, person_name, introduction \
                FROM (SELECT * FROM base_public.id_match WHERE id_type = 3 AND \
                 is_used = 1 AND data_source = '020001' GROUP BY matched_id ) as idm \
                JOIN \
                crawl_public.d_person_description as x \
                ON idm.source_id = x.person_id"

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            "introduction": lambda x: sub_wrong_to_none(x) if type(x) is str else x,

        })

        vm2 = transform.ValueMap({
            "education": (lambda x: cls.fetch_education(x) if type(x) is str else x, "introduction"),
            "gender": (lambda x: cls.fetch_gender(x) if type(x) is str else x, "introduction")

        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            "education": "education",
            "gender": "gender",
            "introduction": "resume",
            "data_source": "data_source"

        })

        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def stream_resume_020002(cls):
        """
            清洗 person_info.education;

        """

        sql = "SELECT idm.matched_id, idm.data_source, person_name, introduction \
                FROM (SELECT * FROM base_public.id_match WHERE id_type = 3 AND \
                 is_used = 1 AND data_source = '020002' GROUP BY matched_id ) as idm \
                JOIN \
                crawl_public.d_person_description as x \
                ON idm.source_id = x.person_id"

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            "introduction": lambda x: sub_wrong_to_none(x) if type(x) is str else x,

        })

        vm2 = transform.ValueMap({
            "education": (lambda x: cls.fetch_education(x) if type(x) is str else x, "introduction"),
            "gender": (lambda x: cls.fetch_gender(x) if type(x) is str else x, "introduction")

        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            "education": "education",
            "gender": "gender",
            "introduction": "resume",
            "data_source": "data_source"

        })

        s = Stream(inp, transform=[vm, vm2, sk])
        return s

    @classmethod
    def stream_resume_020003(cls):
        """
            清洗 person_info.education;

        """

        sql = "SELECT idm.matched_id, idm.data_source, person_name, introduction \
                FROM (SELECT * FROM base_public.id_match WHERE id_type = 3 AND \
                 is_used = 1 AND data_source = '020003' GROUP BY matched_id ) as idm \
                JOIN \
                crawl_public.d_person_description as x \
                ON idm.source_id = x.person_id"

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            "introduction": lambda x: sub_wrong_to_none(x) if type(x) is str else x,

        })

        vm2 = transform.ValueMap({
            "education": (lambda x: cls.fetch_education(x) if type(x) is str else x, "introduction"),
            "gender": (lambda x: cls.fetch_gender(x) if type(x) is str else x, "introduction")

        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            "education": "education",
            "gender": "gender",
            "introduction": "resume",
            "data_source": "data_source"

        })

        s = Stream(inp, transform=[vm, vm2, sk])
        return s


    @classmethod
    def confluence(cls):
        streams = [cls.stream_030001(), cls.stream_020001(), cls.stream_020002(), cls.stream_resume_020001(),
                   cls.stream_resume_020002(), cls.stream_resume_020003()]
        c = Confluence(*streams, on=["person_id"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base_public.person_info", ENGINE, df.dataframe, type="update")


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()





