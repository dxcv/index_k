import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    @classmethod
    def sub_wrong_to_none(cls, x):
        s = re.sub("\s|-| |--|---|&quot;|？|：", "", x)
        if s == "":
            return None
        else:
            return s

    @classmethod
    def fund_info_subsidiary(cls):
        """
            fund_info_subsidiary 映射;

        """

        sql = " \
               SELECT idm.matched_id, fis.risk_income_character,fis.investment_restriction, fis.investment_target, \
                fis.income_distribution FROM ( \
                SELECT DISTINCT matched_id FROM base.id_match WHERE id_type = 1 and is_used =1  \
                ) as idm \
                JOIN \
                (SELECT fund_id, investment_restriction, investment_target, risk_income_character, \
                income_distribution FROM base.fund_info_subsidiary) as fis \
                ON fis.fund_id = idm.matched_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            'income_distribution': lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            'investment_restriction': lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            'investment_target': lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            'risk_income_character': lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,

        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            "income_distribution": "income_distribution",
            'investment_restriction': 'investment_restriction',
            'investment_target': 'investment_target',
            'risk_income_character': 'risk_income_character'
        })

        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.fund_info_subsidiary()]
        c = Confluence(*streams, on=["fund_id"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base.fund_description", ENGINE_RD, df.dataframe, type="update")


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()

