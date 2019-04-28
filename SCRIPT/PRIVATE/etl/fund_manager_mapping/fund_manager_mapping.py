from utils.database import config as cfg, io
from utils.etlkit.core import base, transform
from utils.etlkit.reader.mysqlreader import MysqlInput
from utils.database.models.base_private import FundManagerMapping
import datetime as dt


ENGINE_RD = cfg.load_engine()["2Gb"]


class RemainOneSource(transform.BaseTransform):
    def process(self, frame):
        # 对于每只基金, 如果多个源上(好买, 金斧子)都有其经理人信息, 选用其中一个源的; 优先级: 金斧子, 好买
        df_source = frame[[FundManagerMapping.fund_id.name, "source_id"]].drop_duplicates(subset=[FundManagerMapping.fund_id.name])
        df_main = df_source.merge(frame, on=[FundManagerMapping.fund_id.name, "source_id"])
        return df_main


class Streams:
    @classmethod
    def _stream_02_constructor(cls, source_id):
        SQL_02 = "SELECT im_p.matched_id as person_id, im_f.matched_id as fund_id, dpf.source_id, " \
                 "fi.foundation_date, fi.end_date, fi.fund_status, " \
                 "pi.person_name, fi.fund_name, dpf.is_current " \
                 "FROM {tb} dpf " \
                 "JOIN (SELECT person_id, MAX(version) latest_ver FROM {tb} GROUP BY person_id) t " \
                 "ON dpf.person_id = t.person_id AND dpf.version = t.latest_ver " \
                 "JOIN (SELECT matched_id, source_id, source FROM base.id_match WHERE is_used = 1 AND id_type = 3) im_p " \
                 "ON im_p.source_id = dpf.person_id AND im_p.source = dpf.source_id " \
                 "JOIN (SELECT matched_id, source_id, source FROM base.id_match WHERE is_used = 1 AND id_type = 1) im_f " \
                 "ON im_f.source_id = dpf.fund_id AND im_f.source = dpf.source_id " \
                 "LEFT JOIN base.person_info pi ON im_p.matched_id = pi.person_id " \
                 "LEFT JOIN base.fund_info fi ON im_f.matched_id = fi.fund_id " \
                 "WHERE dpf.source_id = {sid} AND dpf.is_used = 1 ".format(tb=source_id)

        table = "crawl_private.d_person_fund"
        tmp_sql = SQL_02.format(tb=table, sid=source_id)
        inp = MysqlInput(ENGINE_RD, tmp_sql)
        s = base.Stream(inp, transform=[])
        return s

    @classmethod
    def clean_is_current(cls, end_date, fund_status, is_current):
        if end_date is not None:
            if end_date > dt.date.today():
                return 1
            else:
                return 0

        if fund_status is not None:
            if fund_status == "运行中":
                return 1
            else:
                return 0

        return is_current

    @classmethod
    def stream_020001(cls):
        source_id = "020001"
        s = cls._stream_02_constructor(source_id)

        vm = transform.ValueMap({
                "is_current": (lambda fs, ic: cls.clean_is_current(fs, ic), "fund_status", "is_current")
            })

        sk = transform.MapSelectKeys({
            "person_id": FundManagerMapping.person_id.name,
            "person_name": FundManagerMapping.person_name.name,
            "fund_id": FundManagerMapping.fund_id.name,
            "fund_name": FundManagerMapping.fund_name.name,
            "is_current": FundManagerMapping.is_current.name,
            "foundation_date": FundManagerMapping.start_date.name,
            "end_date": FundManagerMapping.end_date.name,
            "source_id": None
        })

        s.transform = (vm, sk,)
        return s

    @classmethod
    def stream_020002(cls):
        source_id = "020002"

        s = cls._stream_02_constructor(source_id)

        vm = transform.ValueMap({
                "is_current": (lambda fs, ic: cls.clean_is_current(fs, ic), "fund_status", "is_current")
            })

        sk = transform.MapSelectKeys({
            "person_id": FundManagerMapping.person_id.name,
            "person_name": FundManagerMapping.person_name.name,
            "fund_id": FundManagerMapping.fund_id.name,
            "fund_name": FundManagerMapping.fund_name.name,
            "is_current": FundManagerMapping.is_current.name,
            "foundation_date": FundManagerMapping.start_date.name,
            "end_date": FundManagerMapping.end_date.name,
            "source_id": None
        })

        s.transform = (vm, sk,)
        return s

    @classmethod
    def conflu_1(cls):
        s21, s22 = cls.stream_020001(), cls.stream_020002()
        c = base.Confluence(s22, s21)
        return c

    @classmethod
    def conflu(cls):
        c = cls.conflu_1()

        ros = RemainOneSource()

        sk = transform.MapSelectKeys({
            "person_id": FundManagerMapping.person_id.name,
            "person_name": FundManagerMapping.person_name.name,
            "fund_id": FundManagerMapping.fund_id.name,
            "fund_name": FundManagerMapping.fund_name.name,
            "is_current": FundManagerMapping.is_current.name,
            FundManagerMapping.start_date.name: None,
            FundManagerMapping.end_date.name: None,
        })
        s = base.Stream(c, [ros, sk])
        c = base.Confluence(s)
        return c


def test():
    c = Streams.conflu()

    c.dataframe
    io.delete("base_test.fund_manager_mapping_test", ENGINE_RD, c.dataframe[["fund_id"]].drop_duplicates())
    io.to_sql("base_test.fund_manager_mapping_test", ENGINE_RD, c.dataframe)


def main():
    c = Streams.conflu()

    # io.delete("base_test.fund_manager_mapping", ENGINE_RD, c.dataframe[["fund_id"]].drop_duplicates())
    io.to_sql("base.fund_manager_mapping", ENGINE_RD, c.dataframe)


if __name__ == "__main__":
    main()
