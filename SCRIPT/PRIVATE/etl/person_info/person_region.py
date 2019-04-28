from utils.database import io, config as cfg
from utils.etlkit.reader.mysqlreader import MysqlInput


# 清洗region字段;

ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:
    @classmethod
    def stream(cls):
        sql = "SELECT a.person_id,org.region from  \
                (SELECT person_id FROM base.person_info) as a \
                JOIN (SELECT org_id,person_id FROM base.org_person_mapping) as map \
                ON a.person_id = map.person_id \
                JOIN (SELECT org_id,region FROM base.org_info) as org \
                ON org.org_id = map.org_id \
                WHERE org.region is not NULL"
        inp = MysqlInput(ENGINE_RD, sql)

        return inp


def main():
    c = StreamsMain.stream()

    io.to_sql("base.person_info", ENGINE_RD, c.dataframe, type='update')


if __name__ == "__main__":
    main()
