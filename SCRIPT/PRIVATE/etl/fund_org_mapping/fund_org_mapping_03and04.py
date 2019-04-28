from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput

ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:
    @classmethod
    def stream_03xxxx_type2(cls):
        """
            清洗id_match, 03xxx源基金管理人
        """
        sql = "SELECT idm.matched_id, oi.org_id, oi.org_full_name " \
              "FROM (" \
              "SELECT DISTINCT matched_id,source FROM base.id_match " \
              "WHERE id_type = 1 AND is_used = 1 AND source LIKE '030%%'" \
              "AND matched_id NOT IN (" \
              "SELECT fund_id FROM base.fund_org_mapping WHERE org_type_code=2)) idm " \
              "JOIN data_test.source_info_org as sig ON idm.source = sig.source_id " \
              "JOIN base.org_info oi ON sig.org_id = oi.org_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "org_type": "基金管理人",
            "org_type_code": 2

        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            'org_full_name': 'org_name',
            "org_type": "org_type",
            "org_type_code": "org_type_code",
            "org_id": "org_id"
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_03xxxx_type1(cls):
        """
            清洗id_match, 03xxx源投顾
        """
        sql = "SELECT idm.matched_id, oi.org_id, oi.org_full_name " \
              "FROM (SELECT DISTINCT matched_id, source FROM base.id_match " \
              "WHERE id_type = 1 AND is_used = 1 AND source LIKE '030%%' " \
              "AND matched_id NOT IN (" \
              "SELECT fund_id FROM base.fund_org_mapping WHERE org_type_code=1)) idm " \
              "JOIN data_test.source_info_org sig ON idm.source = sig.source_id " \
              "JOIN base.org_info oi ON sig.org_id = oi.org_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "org_type": "投资顾问",
            "org_type_code": 1

        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            'org_full_name': 'org_name',
            "org_type": "org_type",
            "org_type_code": "org_type_code",
            "org_id": "org_id"
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def stream_04xxxx_type2(cls):
        """
            清洗id_match, 04xxx源基金管理人
        """
        sql = "SELECT idm.matched_id,oi.org_id,oi.org_full_name " \
              "FROM (SELECT DISTINCT matched_id,source FROM base.id_match " \
              "WHERE id_type = 1 AND is_used = 1 AND source LIKE '040%%' " \
              "AND matched_id NOT IN (SELECT fund_id FROM base.fund_org_mapping WHERE org_type_code=2)) idm " \
              "JOIN data_test.source_info_org sig ON idm.source = sig.source_id " \
              "JOIN base.org_info oi ON sig.org_id = oi.org_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "org_type": "基金管理人",
            "org_type_code": 2

        })

        sk = transform.MapSelectKeys({
            "matched_id": "fund_id",
            'org_full_name': 'org_name',
            "org_type": "org_type",
            "org_type_code": "org_type_code",
            "org_id": "org_id"
        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def confluence(cls):
        streams = [cls.stream_03xxxx_type1(), cls.stream_03xxxx_type2(), cls.stream_04xxxx_type2()]
        c = Confluence(*streams, on=["fund_id", "org_type_code"])
        return c

    @classmethod
    def write(cls):
        df = cls.confluence().dataframe
        io.to_sql("base.fund_org_mapping", ENGINE_RD, df, type="update")


def main():
    StreamsMain.write()


if __name__ == '__main__':
    main()
