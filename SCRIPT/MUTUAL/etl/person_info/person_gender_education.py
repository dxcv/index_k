import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

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
    def stream_gender(cls):
        """
            清洗 person_info.gender;

        """

        sql = "SELECT person_id, resume FROM base_public.person_info"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "gender": (lambda x: cls.fetch_gender(x) if type(x) is str else x, "resume")

        })

        sk = transform.MapSelectKeys({
            "person_id": "person_id",
            "gender": "gender"
        })

        dr = transform.Dropna(how="any")

        s = Stream(inp, transform=[vm, sk, dr])
        return s

    @classmethod
    def stream_education(cls):
        """
            清洗 person_info.education;

        """

        sql = "SELECT person_id, resume FROM base_public.person_info"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "education": (lambda x: cls.fetch_education(x) if type(x) is str else x, "resume")

        })

        sk = transform.MapSelectKeys({
            "person_id": "person_id",
            "education": "education"
        })

        dr = transform.Dropna(how="any")

        s = Stream(inp, transform=[vm, sk, dr])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.stream_gender(), cls.stream_education()]
        c = Confluence(*streams, on=["person_id"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base_public.person_info", ENGINE_RD, df.dataframe, type="update")


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()
