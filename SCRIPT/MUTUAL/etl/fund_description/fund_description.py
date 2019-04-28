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
    def stream_020001(cls):
        """
            清洗 d_fund_description,020001源;

        """

        sql = "\
              SELECT dfp.fund_id,dfp.investment_target, dfp.investment_scope, dfp.investment_strategy, \
                dfp.investment_idea, dfp.income_distribution, dfp.risk_return_character, dfp.comparison_criterion, \
                dfp.guarantee_institution, dfp.guarantee_period, dfp.guarantee_way, dfp.tracking_benchmark \
                FROM (SELECT DISTINCT matched_id FROM base_public.id_match WHERE id_type=1 and is_used=1) as idm \
                JOIN \
                crawl_public.d_fund_description  AS dfp \
                ON idm.matched_id = dfp.fund_id	 \
                where dfp.data_source='020001'"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            'investment_target': lambda x: sub_wrong_to_none(x),
            'investment_scope': lambda x: sub_wrong_to_none(x),
            'investment_strategy': lambda x: sub_wrong_to_none(x),
            'investment_idea': lambda x: sub_wrong_to_none(x),
            'income_distribution': lambda x: sub_wrong_to_none(x),
            'risk_return_character': lambda x: sub_wrong_to_none(x),
            'comparison_criterion': lambda x: sub_wrong_to_none(x),
            'guarantee_institution': lambda x: sub_wrong_to_none(x),
            'guarantee_period': lambda x: sub_wrong_to_none(x),
            'guarantee_way': lambda x: sub_wrong_to_none(x),
            'tracking_benchmark': lambda x: sub_wrong_to_none(x),
        })

        # vm2 = transform.ValueMap({
        #     "tracking_benchmark": lambda x: None if type(x) is str and x == "该基金无跟踪标的" else x
        # })

        sk = transform.MapSelectKeys({
            'investment_target': 'investment_target',
            'investment_scope': 'investment_scope',
            'investment_strategy': 'investment_strategy',
            'investment_idea': 'investment_idea',
            'income_distribution': 'income_distribution',
            'risk_return_character': 'risk_return_character',
            'comparison_criterion': 'comparison_criterion',
            'guarantee_institution': 'guarantee_institution',
            'guarantee_period': 'guarantee_period',
            'guarantee_way': 'guarantee_way',
            'tracking_benchmark': 'tracking_benchmark',
            'fund_id': 'fund_id'
        })

        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020002(cls):
        """
            清洗 d_fund_description,020002源;

        """

        sql = "\
              SELECT dfp.fund_id,dfp.investment_target, dfp.investment_scope, dfp.investment_strategy, \
                dfp.investment_idea, dfp.income_distribution, dfp.risk_return_character, dfp.comparison_criterion, \
                dfp.guarantee_institution, dfp.guarantee_period, dfp.guarantee_way, dfp.tracking_benchmark \
                FROM (SELECT DISTINCT matched_id FROM base_public.id_match WHERE id_type=1 and is_used=1) as idm \
                JOIN \
                crawl_public.d_fund_description  AS dfp \
                ON idm.matched_id = dfp.fund_id	 \
                where dfp.data_source='020002'"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            'investment_target': lambda x: sub_wrong_to_none(x),
            'investment_scope': lambda x: sub_wrong_to_none(x),
            'investment_strategy': lambda x: sub_wrong_to_none(x),
            'investment_idea': lambda x: sub_wrong_to_none(x),
            'income_distribution': lambda x: sub_wrong_to_none(x),
            'risk_return_character': lambda x: sub_wrong_to_none(x),
            'comparison_criterion': lambda x: sub_wrong_to_none(x),
            'guarantee_institution': lambda x: sub_wrong_to_none(x),
            'guarantee_period': lambda x: sub_wrong_to_none(x),
            'guarantee_way': lambda x: sub_wrong_to_none(x),
            'tracking_benchmark': lambda x: sub_wrong_to_none(x),
        })
        #
        # vm2 = transform.ValueMap({
        #     "tracking_benchmark": lambda x: None if type(x) is str and x == "该基金无跟踪标的" else x
        # })

        sk = transform.MapSelectKeys({
            'investment_target': 'investment_target',
            'investment_scope': 'investment_scope',
            'investment_strategy': 'investment_strategy',
            'investment_idea': 'investment_idea',
            'income_distribution': 'income_distribution',
            'risk_return_character': 'risk_return_character',
            'comparison_criterion': 'comparison_criterion',
            'guarantee_institution': 'guarantee_institution',
            'guarantee_period': 'guarantee_period',
            'guarantee_way': 'guarantee_way',
            'tracking_benchmark': 'tracking_benchmark',
            'fund_id': 'fund_id'
        })

        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_020003(cls):
        """
            清洗 d_fund_description,020003源;

        """

        sql = "\
              SELECT dfp.fund_id,dfp.investment_target, dfp.investment_scope, dfp.investment_strategy, \
                dfp.investment_idea, dfp.income_distribution, dfp.risk_return_character, dfp.comparison_criterion, \
                dfp.guarantee_institution, dfp.guarantee_period, dfp.guarantee_way, dfp.tracking_benchmark \
                FROM (SELECT DISTINCT matched_id FROM base_public.id_match WHERE id_type=1 and is_used=1) as idm \
                JOIN \
                crawl_public.d_fund_description  AS dfp \
                ON idm.matched_id = dfp.fund_id	 \
                where dfp.data_source='020001'"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            'investment_target': lambda x: sub_wrong_to_none(x),
            'investment_scope': lambda x: sub_wrong_to_none(x),
            'investment_strategy': lambda x: sub_wrong_to_none(x),
            'investment_idea': lambda x: sub_wrong_to_none(x),
            'income_distribution': lambda x: sub_wrong_to_none(x),
            'risk_return_character': lambda x: sub_wrong_to_none(x),
            'comparison_criterion': lambda x: sub_wrong_to_none(x),
            'guarantee_institution': lambda x: sub_wrong_to_none(x),
            'guarantee_period': lambda x: sub_wrong_to_none(x),
            'guarantee_way': lambda x: sub_wrong_to_none(x),
            'tracking_benchmark': lambda x: sub_wrong_to_none(x),
        })

        # vm2 = transform.ValueMap({
        #     "tracking_benchmark": lambda x: None if type(x) is str and x == "该基金无跟踪标的" else x
        # })

        sk = transform.MapSelectKeys({
            'investment_target': 'investment_target',
            'investment_scope': 'investment_scope',
            'investment_strategy': 'investment_strategy',
            'investment_idea': 'investment_idea',
            'income_distribution': 'income_distribution',
            'risk_return_character': 'risk_return_character',
            'comparison_criterion': 'comparison_criterion',
            'guarantee_institution': 'guarantee_institution',
            'guarantee_period': 'guarantee_period',
            'guarantee_way': 'guarantee_way',
            'tracking_benchmark': 'tracking_benchmark',
            'fund_id': 'fund_id'
        })

        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def fund_name(cls):
        sql = "UPDATE base_public.fund_description AS fh , \
        (SELECT fund_id,fund_name from base_public.fund_info) AS fi \
        SET fh.fund_name = fi.fund_name \
        WHERE fh.fund_id = fi.fund_id"
        ENGINE_RD.execute(sql)
        print('fund_name over')

    @classmethod
    def confluence(cls):
        streams = [cls.stream_020001(), cls.stream_020002(), cls.stream_020003()]
        c = Confluence(*streams, on=["fund_id"])
        return c


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.fund_description_test", "base_public.fund_description", ENGINE_RD,
                            cols_excluded={'fund_id',	'data_source', 'entry_time', 'update_time', 'fund_name'})
    t.result.to_csv(os.path.expanduser("~/Desktop/fund_description_{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


def main():
    a = StreamsMain.confluence()
    io.to_sql("base_public.fund_description", ENGINE_RD, a.dataframe, type="update")
    StreamsMain.fund_name()


if __name__ == "__main__":
    main()

