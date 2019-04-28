from utils.database import io, config as cfg
from utils.database.models.base_private import PersonDescription
from utils.etlkit.core import base, transform
from utils.etlkit.reader.mysqlreader import MysqlInput
import re

ENGINE = cfg.load_engine()["etl01"]


class Streams:
    SQL_02 = "SELECT im.matched_id as person_id, pi.person_name, dpd.resume FROM {tb} dpd " \
             "JOIN (SELECT person_id, MAX(version) latest_ver FROM {tb} GROUP BY person_id) t " \
             "ON dpd.person_id = t.person_id AND dpd.version = t.latest_ver " \
             "JOIN base.id_match im " \
             "ON im.source_id = dpd.person_id AND im.source = dpd.source_id " \
             "LEFT JOIN base.person_info pi ON im.matched_id = pi.person_id " \
             "WHERE im.source = {sid} AND im.is_used = 1 AND im.id_type = 3"

    @classmethod
    def stream_000001(cls):
        pass

    @classmethod
    def stream_020001(cls):
        source_id = "020001"
        tb = "crawl_private.d_person_description"
        tmp_sql = cls.SQL_02.format(tb=tb, sid=source_id)
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            "person_name": lambda x: re.sub("\s", "", x),
            "resume": lambda x: re.sub("\s", "", x),
        })

        sk = transform.MapSelectKeys({
            "person_id": PersonDescription.person_id.name,
            "person_name": PersonDescription.person_name.name,
            "resume": PersonDescription.resume.name,
        })

        dn = transform.Dropna(subset=[PersonDescription.resume.name])

        s = base.Stream(inp, [vm, sk, dn])
        return s

    @classmethod
    def stream_020002(cls):
        source_id = "020002"
        tb = "crawl_private.d_person_description"
        tmp_sql = cls.SQL_02.format(tb=tb, sid=source_id)
        inp = MysqlInput(ENGINE, tmp_sql)

        vm = transform.ValueMap({
            "person_name": lambda x: re.sub("\s", "", x),
            "resume": lambda x: re.sub("\s", "", x),
        })

        sk = transform.MapSelectKeys({
            "person_id": PersonDescription.person_id.name,
            "person_name": PersonDescription.person_name.name,
            "resume": PersonDescription.resume.name,
        })

        dn = transform.Dropna(subset=[PersonDescription.resume.name])

        s = base.Stream(inp, [vm, sk, dn])
        return s

    @classmethod
    def conflu(cls):
        s21, s22 = cls.stream_020001(), cls.stream_020002()
        c = base.Confluence(s21, s22, on=[PersonDescription.person_id.name])
        return c


def main():
    c = Streams.conflu()
    io.to_sql(PersonDescription.__schema_table__, ENGINE, c.dataframe, "ignore")


if __name__ == "__main__":
    main()
