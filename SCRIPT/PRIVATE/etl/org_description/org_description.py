import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput

ENGINE_RD = cfg.load_engine()["2Gb"]


def sub_wrong_to_none(x):
    s = re.sub("\s|-| |--|---", "", x)
    if s == "":
        return None
    else:
        return s


class StreamsMain:


    @classmethod
    def y_org_description(cls):
        """
            清洗 y_org_description;

        """

        sql = "SELECT org_id,profile,team,investment_idea,prize,major_shareholder,shareholder_structure FROM crawl_private.`y_org_description`"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "investment_idea": lambda x: sub_wrong_to_none(x),
            "profile": lambda x: sub_wrong_to_none(x),
            "team": lambda x: sub_wrong_to_none(x),
            "prize": lambda x: sub_wrong_to_none(x),
            "major_shareholder": lambda x: sub_wrong_to_none(x),
            "shareholder_structure": lambda x: sub_wrong_to_none(x),
        })

        sk = transform.MapSelectKeys({
            'org_id': 'org_id',
            'investment_idea': 'investment_idea',
            'profile': 'profile',
            'team': 'team',
            'prize': 'prize',
            'major_shareholder': 'major_shareholder',
            'shareholder_structure': 'shareholder_structure'
        })

        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020001(cls):
        """
            清洗d_org_description.profile(020001)

        """

        sql = "SELECT matched_id as org_id,profile,team,investment_idea,prize FROM ( \
                SELECT * FROM (SELECT matched_id,source_id FROM base.id_match where source='020001' and id_type=2 and is_used=1) \
                as b LEFT JOIN \
                (SELECT org_id,profile,team,investment_idea,prize FROM \
                crawl_private.`d_org_description` WHERE source_id = '020001' \
                ORDER BY version DESC) as p ON b.source_id=p.org_id \
                WHERE p.org_id is not NULL) AS T \
                GROUP BY T.org_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "investment_idea": lambda x: sub_wrong_to_none(x),
            "profile": lambda x: sub_wrong_to_none(x),
            "team": lambda x: sub_wrong_to_none(x),
            "prize": lambda x: sub_wrong_to_none(x),

        })

        sk = transform.MapSelectKeys({
            'org_id': 'org_id',
            'investment_idea': 'investment_idea',
            'profile': 'profile',
            'team': 'team',
            'prize': 'prize'
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020002(cls):
        """
            清洗d_org_description.profile(020002)

        """

        sql = "SELECT matched_id as org_id,profile,team,investment_idea,prize FROM ( \
                SELECT * FROM (SELECT matched_id,source_id FROM base.id_match where source='020002' and id_type=2 and is_used=1) \
                as b LEFT JOIN \
                (SELECT org_id,profile,team,investment_idea,prize FROM \
                crawl_private.`d_org_description` WHERE source_id = '020002' \
                ORDER BY version DESC) as p ON b.source_id=p.org_id \
                WHERE p.org_id is not NULL) AS T \
                GROUP BY T.org_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "investment_idea": lambda x: sub_wrong_to_none(x),
            "profile": lambda x: sub_wrong_to_none(x),
            "team": lambda x: sub_wrong_to_none(x),
            "prize": lambda x: sub_wrong_to_none(x),

        })

        sk = transform.MapSelectKeys({
            'org_id': 'org_id',
            'investment_idea': 'investment_idea',
            'profile': 'profile',
            'team': 'team',
            'prize': 'prize'
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020003(cls):
        """
            清洗d_org_description.profile(020001)

        """

        sql = "SELECT matched_id as org_id,profile,team,investment_idea,prize FROM ( \
                SELECT * FROM (SELECT matched_id,source_id FROM base.id_match where source='020003' and id_type=2 and is_used=1) \
                as b LEFT JOIN \
                (SELECT org_id,profile,team,investment_idea,prize FROM \
                crawl_private.`d_org_description` WHERE source_id = '020003' \
                ORDER BY version DESC) as p ON b.source_id=p.org_id \
                WHERE p.org_id is not NULL) AS T \
                GROUP BY T.org_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "investment_idea": lambda x: sub_wrong_to_none(x),
            "profile": lambda x: sub_wrong_to_none(x),
            "team": lambda x: sub_wrong_to_none(x),
            "prize": lambda x: sub_wrong_to_none(x),

        })

        sk = transform.MapSelectKeys({
            'org_id': 'org_id',
            'investment_idea': 'investment_idea',
            'profile': 'profile',
            'team': 'team',
            'prize': 'prize'
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_010001(cls):
        """
            清洗x_org_info(010001)

        """

        sql = "SELECT org_id,legal_person_resume,special_tips FROM ( \
                SELECT * FROM (SELECT matched_id,source_id FROM base.id_match WHERE  \
                source='010001' and id_type=2 and is_used=1) \
                as b LEFT JOIN \
                (SELECT org_id,legal_person_resume,special_tips FROM \
                crawl_private.`x_org_info`  \
                ORDER BY version DESC) as p ON b.source_id=p.org_id \
                WHERE p.org_id is not NULL) AS T \
                GROUP BY T.org_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "legal_person_resume": lambda x: sub_wrong_to_none(x),
            "special_tips": lambda x: sub_wrong_to_none(x)
        })

        sk = transform.MapSelectKeys({
            'org_id': 'org_id',
            "special_tips": "special_tips",
            "legal_person_resume": "legal_person_resume"
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_name(cls):
        sql = "SELECT t.org_id,i.org_name FROM ( \
                SELECT org_id FROM base.org_description) as t \
                LEFT JOIN (SELECT org_id,org_name from base.org_info) as i \
                ON t.org_id = i.org_id"
        inp = MysqlInput(ENGINE_RD, sql)

        sk = transform.MapSelectKeys({
            'org_id': 'org_id',
            "org_name": "org_name"
        })
        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.y_org_description(), cls.stream_020001(), cls.stream_020002(), cls.stream_020003(), cls.stream_010001()]
        c = Confluence(*streams, on=["org_id"])
        return c

    @classmethod
    def confluence_2(cls):
        streams = [cls.stream_name()]
        c = Confluence(*streams, on=["org_id"])
        return c

    @classmethod
    def write(cls):
        a = cls.confluence()
        b = a.dataframe
        d = b.set_index(["org_id"])
        e = d.dropna(how="all")
        df = e.reset_index()
        io.to_sql("base.org_description", ENGINE_RD, df, type="update")
        name = StreamsMain.confluence_2()
        io.to_sql("base.org_description", ENGINE_RD, name.dataframe, type="update")


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.org_description_test", "base.org_description", ENGINE_RD,
                            cols_excluded={'org_id', 'org_name', 'entry_time', 'update_time'})
    t.result.to_csv(os.path.expanduser("~/Desktop/org_description_{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()

