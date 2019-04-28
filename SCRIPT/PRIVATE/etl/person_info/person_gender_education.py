from utils.database import io, config as cfg
from utils.database.models.base_private import PersonInfo
from utils.etlkit.core import base, transform
from utils.etlkit.reader.mysqlreader import MysqlInput
import re

# 清洗gender, education字段;

ENGINE = cfg.load_engine()["etl01"]


class Streams:
    @classmethod
    def stream_010101(cls):
        sql = "SELECT im.matched_id as person_id, xpi.gender, xpi.education " \
              "FROM crawl_private.x_person_info_010101 xpi " \
              "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.x_person_info_010101 GROUP BY person_id) t " \
              "ON xpi.person_id = t.person_id AND xpi.version = t.latest_ver " \
              "JOIN base.id_match im ON xpi.person_id = im.source_id " \
              "WHERE im.source = '010101' AND im.id_type = 3 AND im.is_used = 1"

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            "education": lambda x: {"硕士研究生": "硕士", "大专": "大专", "本科": "本科", "博士研究生": "博士", "中专": "中专", "高中": "高中"}.get(x),
            "gender": lambda x: {"男": "男", "女": "女"}.get(x)
        })

        sk = transform.MapSelectKeys({
            "person_id": PersonInfo.person_id.name,
            "gender": PersonInfo.gender.name,
            "education": PersonInfo.education.name
        })

        dn = transform.Dropna(subset=[PersonInfo.gender.name, PersonInfo.education.name], how="all")
        s = base.Stream(inp, [vm, sk, dn])
        return s

    @classmethod
    def stream_desc(cls):
        SQL_DESP = "SELECT pi.person_id, pd.resume FROM base.person_info pi " \
                   "JOIN base.person_description pd ON pi.person_id = pd.person_id"

        inp = MysqlInput(ENGINE, SQL_DESP)

        vm = transform.ValueMap({
            PersonInfo.education.name: (lambda x: cls._fetch_education(x), "resume"),
            PersonInfo.gender.name: (lambda x: cls._fetch_gender(x), "resume"),
        })

        sk = transform.MapSelectKeys({
            PersonInfo.person_id.name: None,
            PersonInfo.gender.name: None,
            PersonInfo.education.name: None
        })

        dn = transform.Dropna(subset=[PersonInfo.education.name, PersonInfo.gender.name], how="all")

        s = base.Stream(inp, [vm, sk, dn])
        return s

    @classmethod
    def conflu(cls):
        sc, s11 = cls.stream_desc(), cls.stream_010101()
        c = base.Confluence(s11, sc, on=[PersonInfo.person_id.name])
        return c

    # helper function
    @classmethod
    def _fetch_gender(cls, string):
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
    def _fetch_education(cls, string):
        if string is None:
            return None
        d_education = {
            "博士后": "博士后",
            "博士": "博士",
            "MBA": "MBA",
            "硕士": "硕士",
            "研究生": "硕士",
            "大学": "本科",
            "学士": "本科",
            "本科": "本科",
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
                # "学士": 4,
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
    def stream_010101(cls):
        sql = "SELECT im.matched_id as person_id, xpi.gender, xpi.education " \
              "FROM crawl_private.x_person_info_010101 xpi " \
              "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.x_person_info_010101 GROUP BY person_id) t " \
              "ON xpi.person_id = t.person_id AND xpi.version = t.latest_ver " \
              "JOIN base.id_match im ON xpi.person_id = im.source_id " \
              "WHERE im.source = '010101' AND im.id_type = 3 AND im.is_used = 1"

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            "education": lambda x: {"硕士研究生": "硕士", "大专": "大专", "本科": "本科", "博士研究生": "博士", "中专": "中专", "高中": "高中"}.get(x),
            "gender": lambda x: {"男": "男", "女": "女"}.get(x)
        })

        sk = transform.MapSelectKeys({
            "person_id": PersonInfo.person_id.name,
            "gender": PersonInfo.gender.name,
            "education": PersonInfo.education.name
        })

        dn = transform.Dropna(subset=[PersonInfo.gender.name, PersonInfo.education.name], how="all")
        s = base.Stream(inp, [vm, sk, dn])
        return s

    @classmethod
    def stream_clean(cls):
        SQL_DESP = "SELECT pi.person_id, pd.resume FROM base.person_info pi " \
                   "JOIN base.person_description pd ON pi.person_id = pd.person_id"

        inp = MysqlInput(ENGINE, SQL_DESP)

        vm = transform.ValueMap({
            PersonInfo.education.name: (lambda x: cls.fetch_education(x), "resume"),
            PersonInfo.gender.name: (lambda x: cls.fetch_gender(x), "resume"),
        })

        sk = transform.MapSelectKeys({
            PersonInfo.person_id.name: None,
            PersonInfo.gender.name: None,
            PersonInfo.education.name: None
        })

        dn = transform.Dropna(subset=[PersonInfo.education.name, PersonInfo.gender.name], how="all")

        s = base.Stream(inp, [vm, sk, dn])
        return s

    @classmethod
    def conflu(cls):
        sc, s11 = cls.stream_clean(), cls.stream_010101()
        c = base.Confluence(s11, sc, on=[PersonInfo.person_id.name])
        return c


def main():
    c = Streams.conflu()
    io.to_sql(PersonInfo.__schema_table__, ENGINE, c.dataframe)


if __name__ == "__main__":
    main()
