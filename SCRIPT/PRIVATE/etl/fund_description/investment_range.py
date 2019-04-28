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
    def stream_fundaccount(cls):
        """
            清洗 x_info_fundaccount（010002）;

        """

        sql = " \
               SELECT id.matched_id, ac.orientation_amac FROM ( \
                SELECT A.matched_id, A.source_id FROM  \
                (SELECT matched_id, source_id, entry_time FROM base.id_match \
                WHERE source = '010002' and is_used = 1 and id_type = 1 ORDER BY entry_time DESC) \
                 AS A  GROUP BY A.matched_id) AS id  \
                JOIN \
                (SELECT * FROM (SELECT MAX(version) mm ,fund_id as maid FROM crawl_private.x_fund_info_fundaccount \
                GROUP BY fund_id) AS p \
                JOIN crawl_private.x_fund_info_fundaccount as ff \
                on ff.fund_id = p.maid and ff.version = p.mm) AS ac \
                ON ac.maid = id.source_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            'orientation_amac': lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x
        })

        dr = transform.Dropna(axis=0, how="any")

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            "orientation_amac": "investment_range"
        })

        s = Stream(inp, transform=[vm, dr, sk])
        return s

    @classmethod
    def stream_private(cls):
        """
            清洗 x_info_private（010003）;

        """

        sql = " \
               SELECT id.matched_id, ac.orientation_amac FROM ( \
                SELECT A.matched_id, A.source_id FROM  \
                (SELECT matched_id, source_id, entry_time FROM base.id_match \
                WHERE source = '010003' and is_used = 1 and id_type = 1 ORDER BY entry_time DESC) \
                 AS A  GROUP BY A.matched_id) AS id  \
                JOIN \
                (SELECT * FROM (SELECT MAX(version) mm ,fund_id as maid FROM crawl_private.x_fund_info_private \
                GROUP BY fund_id) AS p \
                JOIN crawl_private.x_fund_info_private as ff \
                on ff.fund_id = p.maid and ff.version = p.mm) AS ac \
                ON ac.maid = id.source_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            'orientation_amac': lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x
        })

        dr = transform.Dropna(axis=0, how="any")

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            "orientation_amac": "investment_range"
        })

        s = Stream(inp, transform=[vm, dr, sk])
        return s

    @classmethod
    def stream_securities(cls):
        """
            清洗 x_info_securities（010004）;

        """

        sql = " \
               SELECT id.matched_id, ac.orientation_amac FROM ( \
                SELECT A.matched_id, A.source_id FROM  \
                (SELECT matched_id, source_id, entry_time FROM base.id_match \
                WHERE source = '010004' and is_used = 1 and id_type = 1 ORDER BY entry_time DESC) \
                 AS A  GROUP BY A.matched_id) AS id  \
                JOIN \
                (SELECT * FROM (SELECT MAX(version) mm ,fund_id as maid FROM crawl_private.x_fund_info_securities \
                GROUP BY fund_id) AS p \
                JOIN crawl_private.x_fund_info_securities as ff \
                on ff.fund_id = p.maid and ff.version = p.mm) AS ac \
                ON ac.maid = id.source_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            'orientation_amac': lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x
        })
        dr = transform.Dropna(axis=0, how="any")

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            "orientation_amac": "investment_range"
        })

        s = Stream(inp, transform=[vm, dr, sk])
        return s

    @classmethod
    def stream_futures(cls):
        """
            清洗 x_info_futures（010005）;

        """

        sql = " \
               SELECT id.matched_id, ac.orientation_amac FROM ( \
                SELECT A.matched_id, A.source_id FROM  \
                (SELECT matched_id, source_id, entry_time FROM base.id_match \
                WHERE source = '010005' and is_used = 1 and id_type = 1 ORDER BY entry_time DESC) \
                 AS A  GROUP BY A.matched_id) AS id  \
                JOIN \
                (SELECT * FROM (SELECT MAX(version) mm ,fund_id as maid FROM crawl_private.x_fund_info_futures \
                GROUP BY fund_id) AS p \
                JOIN crawl_private.x_fund_info_futures as ff \
                on ff.fund_id = p.maid and ff.version = p.mm) AS ac \
                ON ac.maid = id.source_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            'orientation_amac': lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x
        })

        dr = transform.Dropna(axis=0, how="any")

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            "orientation_amac": "investment_range"
        })

        s = Stream(inp, transform=[vm, dr, sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.stream_fundaccount(), cls.stream_securities(), cls.stream_futures(), cls.stream_private()]
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

