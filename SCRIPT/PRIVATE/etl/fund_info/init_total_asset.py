from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput

ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:


    @classmethod
    def init_total_asset(cls):
        """
            清洗 fund_asset_scale;

        """

        sql = 'SELECT idm.matched_id, fas.asset_scale FROM  \
                (SELECT DISTINCT matched_id from base.id_match where id_type=1 and is_used =1) AS idm \
                JOIN ( \
                SELECT fund_id,MIN(statistic_date),asset_scale FROM base.fund_asset_scale GROUP BY fund_id \
                ) as fas ON idm.matched_id = fas.fund_id'

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "asset_scale": lambda x: '%.6f' % x if type(x) is float else x

        })

        sk = transform.MapSelectKeys({
            "asset_scale": "init_total_asset",
            'matched_id': 'fund_id'

        })
        s = Stream(inp, transform=[vm, sk])
        return s

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("base.fund_info", ENGINE_RD, df.dataframe, type="update")
        print(df.dataframe)

    @classmethod
    def confluence(cls):
        streams = [cls.init_total_asset()]
        c = Confluence(*streams, on=["fund_id"])
        return c


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()



