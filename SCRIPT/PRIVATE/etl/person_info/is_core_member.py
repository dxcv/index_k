# -*- coding: utf-8 -*-
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    """
    清洗is_core_member
    """

    @classmethod
    def stream(cls):

        sql = "SELECT idm.person_id FROM( \
                SELECT person_id FROM base.person_info) as idm \
                JOIN \
                base.org_person_mapping as op \
                on op.person_id = idm.person_id \
                WHERE  op.duty_detail IN ( \
                  '基金经理', \
                  '投资总监', \
                  '投资经理', \
                  '投研总监', \
                  '风控总监', \
                  '投资部经理', \
                  '投资决策委员会主席', \
                  '总经理', \
                  '投资部主管' \
                 )GROUP BY idm.person_id"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "is_core_member": "是"
        })

        sk = transform.MapSelectKeys({
            "person_id": "person_id",
            'is_core_member': 'is_core_member'
        })

        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def wirte(cls):
        c = cls.stream().flow()
        df = c[0]
        io.to_sql("base.person_info", ENGINE_RD, df, type="update")


def main():
    StreamsMain.wirte()


if __name__ == '__main__':
    main()
