# -*- coding: utf-8 -*-
import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    @classmethod
    def clean_01(cls, x):
        try:
            a = []
            ll = x.split("，")
            for i in ll:
                if '经验' and '经历' in i:
                    s = re.search('[0-9]*\年', i).group()
                    if len(a) == 1:
                        break
                    a.append(s)
            s = a[0]

        except AttributeError:
            s = ""
        else:
            pass
        if s == "":
            return None
        else:
            if float(re.sub("年", "", s)) > 61:
                return None
            else:
                return s

    @classmethod
    def sub_wrong_to_none(cls, x):
        s = re.sub("\s|-| |--|---|万股|%", "", x)
        if s == "":
            return None
        else:
            return s

    @classmethod
    def stream_020001_investment_years(cls):
        """
            清洗 d_person_info;

        """

        sql = "select idm.matched_id, dd.investment_years FROM \
                (SELECT matched_id,source,source_id FROM base.id_match \
                where id_type = 3 and is_used = 1 AND source = '020001') as idm \
                JOIN \
                (SELECT * FROM \
                (select MAX(version) as mm,person_id as pi,source_id as si  \
                FROM crawl_private.d_person_info GROUP BY person_id) as ma \
                JOIN crawl_private.d_person_info as dpd \
                ON dpd.person_id = ma.pi and ma.mm = dpd.version) dd \
                ON dd.pi = idm.source_id \
                AND dd.si = idm.source"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "investment_years": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            'investment_years': 'investment_years'
        })

        dp = transform.Dropna(how="any", axis=0)
        s = Stream(inp, transform=[vm, sk, dp])
        return s

    @classmethod
    def stream_020002_investment_years(cls):
        """
            清洗 d_person_info;

        """

        sql = "select idm.matched_id, dd.investment_years FROM \
                (SELECT matched_id,source,source_id FROM base.id_match \
                where id_type = 3 and is_used = 1 AND source = '020002') as idm \
                JOIN \
                (SELECT * FROM \
                (select MAX(version) as mm,person_id as pi,source_id as si  \
                FROM crawl_private.d_person_info GROUP BY person_id) as ma \
                JOIN crawl_private.d_person_info as dpd \
                ON dpd.person_id = ma.pi and ma.mm = dpd.version) dd \
                ON dd.pi = idm.source_id \
                AND dd.si = idm.source"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "investment_years": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            'investment_years': 'investment_years'
        })
        dp = transform.Dropna(how="any", axis=0)

        s = Stream(inp, transform=[vm, sk, dp])
        return s

    @classmethod
    def stream_020003_investment_years(cls):

        sql = "select idm.matched_id, dd.investment_years, dd.graduate_school FROM \
                (SELECT matched_id,source,source_id FROM base.id_match \
                where id_type = 3 and is_used = 1 AND source = '020003') as idm \
                JOIN \
                (SELECT * FROM \
                (select MAX(version) as mm,person_id as pi,source_id as si  \
                FROM crawl_private.d_person_info GROUP BY person_id) as ma \
                JOIN crawl_private.d_person_info as dpd \
                ON dpd.person_id = ma.pi and ma.mm = dpd.version) dd \
                ON dd.pi = idm.source_id \
                AND dd.si = idm.source"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "investment_years": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            'graduate_school': 'graduate_school'
        })
        dp = transform.Dropna(how='any', axis=0)
        s = Stream(inp, transform=[vm, sk, dp])
        return s

    @classmethod
    def stream_020003_graduate_school(cls):
        """
            清洗 d_person_info;

        """

        sql = "select idm.matched_id, dd.graduate_school FROM \
                (SELECT matched_id,source,source_id FROM base.id_match \
                where id_type = 3 and is_used = 1 AND source = '020003') as idm \
                JOIN \
                (SELECT * FROM \
                (select MAX(version) as mm,person_id as pi,source_id as si  \
                FROM crawl_private.d_person_info GROUP BY person_id) as ma \
                JOIN crawl_private.d_person_info as dpd \
                ON dpd.person_id = ma.pi and ma.mm = dpd.version) dd \
                ON dd.pi = idm.source_id \
                AND dd.si = idm.source"

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            'graduate_school': 'graduate_school'
        })
        dp = transform.Dropna(how='any', axis=0)
        s = Stream(inp, transform=[sk, dp])
        return s

    @classmethod
    def stream_020001_resume(cls):


        sql = "select idm.matched_id, dd.resume FROM \
                (SELECT matched_id,source,source_id FROM base.id_match \
                where id_type = 3 and is_used = 1 AND source = '020001') as idm \
                JOIN \
                (SELECT * FROM \
                (select MAX(version) as mm,person_id as pi,source_id as si \
                from crawl_private.d_person_description GROUP BY person_id) ma \
                JOIN crawl_private.d_person_description as dpd \
                ON dpd.person_id = ma.pi and ma.mm = dpd.version) dd \
                ON dd.pi = idm.source_id \
                AND dd.si = idm.source"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "investment_years": (lambda x: cls.clean_01(x) if type(x) is str else x, 'resume')
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            'investment_years': 'investment_years'
        })
        dp = transform.Dropna(how='any', axis=0)
        s = Stream(inp, transform=[vm, sk, dp])
        return s

    @classmethod
    def stream_020002_resume(cls):


        sql = "select idm.matched_id, dd.resume FROM \
                (SELECT matched_id,source,source_id FROM base.id_match \
                where id_type = 3 and is_used = 1 AND source = '020002') as idm \
                JOIN \
                (SELECT * FROM \
                (select MAX(version) as mm,person_id as pi,source_id as si \
                from crawl_private.d_person_description GROUP BY person_id) ma \
                JOIN crawl_private.d_person_description as dpd \
                ON dpd.person_id = ma.pi and ma.mm = dpd.version) dd \
                ON dd.pi = idm.source_id \
                AND dd.si = idm.source"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "investment_years": (lambda x: cls.clean_01(x) if type(x) is str else x, 'resume')
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            'investment_years': 'investment_years'
        })
        dp = transform.Dropna(how='any', axis=0)
        s = Stream(inp, transform=[vm, sk, dp])
        return s

    @classmethod
    def stream_020003_resume(cls):


        sql = "select idm.matched_id, dd.resume FROM \
                (SELECT matched_id,source,source_id FROM base.id_match \
                where id_type = 3 and is_used = 1 AND source = '020003') as idm \
                JOIN \
                (SELECT * FROM \
                (select MAX(version) as mm,person_id as pi,source_id as si \
                from crawl_private.d_person_description GROUP BY person_id) ma \
                JOIN crawl_private.d_person_description as dpd \
                ON dpd.person_id = ma.pi and ma.mm = dpd.version) dd \
                ON dd.pi = idm.source_id \
                AND dd.si = idm.source"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "investment_years": (lambda x: cls.clean_01(x) if type(x) is str else x, 'resume')
        })

        sk = transform.MapSelectKeys({
            "matched_id": "person_id",
            'investment_years': 'investment_years'
        })
        dp = transform.Dropna(how='any', axis=0)
        s = Stream(inp, transform=[vm, sk, dp])
        return s

    @classmethod
    def stream_gf(cls):


        sql = "SELECT p.person_id,m.graduateschool \
                FROM base.person_info as p \
                JOIN (SELECT DISTINCT person_id,org_id,person_name from base.org_person_mapping) as op \
                ON p.person_id = op.person_id \
                JOIN \
                (SELECT user_name,org_id,graduateschool FROM base.manager_info \
                WHERE graduateschool NOT like '-' and graduateschool is not NULL and graduateschool not like '') \
                AS m \
                ON op.person_name = m.user_name \
                AND op.org_id = m.org_id \
                WHERE p.graduate_school is NULL"

        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            "person_id": "person_id",
            'graduateschool': 'graduate_school'
        })
        dp = transform.Dropna(how='any', axis=0)
        s = Stream(inp, transform=[sk, dp])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.stream_020001_investment_years(), cls.stream_020002_investment_years(),
                   cls.stream_020003_graduate_school(), cls.stream_020003_investment_years(),
                   cls.stream_020001_resume(), cls.stream_020002_resume(),
                   cls.stream_020003_resume(), cls.stream_gf()]
        c = Confluence(*streams, on=["person_id"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base.person_info", ENGINE_RD, df.dataframe, type="update")


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()
