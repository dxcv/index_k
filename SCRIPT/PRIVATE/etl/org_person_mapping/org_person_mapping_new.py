import re
from pandas import DataFrame
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE = cfg.load_engine()["2Gb"]


class Streams:

    @classmethod
    def stream_y(cls):
        sql = "SELECT org_id,org_name,person_id,person_name,duty,duty_detail," \
              "tenure_date,is_current FROM crawl_private.y_org_person_mapping"

        inp = MysqlInput(ENGINE, sql)

        sk = transform.MapSelectKeys(
            {
                "person_id": "person_id",
                "person_name": "person_name",
                "org_id": "org_id",
                "org_name": "org_name",
                "duty": "duty",
                "duty_detail": "duty_detail",
                "is_current": "is_current",
                'tenure_date': "tenure_date"
            }
        )

        s = Stream(inp, transform=[sk])
        return s


    @classmethod
    def stream_010001(cls):
        SOURCE = "010001"

        sql = "SELECT im.matched_id, pi.person_name, im_org.org_id_matched as org_id, oi.org_name, duty " \
              "FROM crawl_private.x_org_executive_info tb_main " \
              "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.x_org_executive_info GROUP BY " \
              "person_id) tb_latest " \
              "ON tb_main.version = tb_latest.latest_ver AND tb_main.person_id = tb_latest.person_id " \
              "JOIN base.id_match im " \
              "ON im.source_id = tb_main.person_id AND im.id_type = 3 AND im.source = '{sid}' AND im.is_used = 1 " \
              "LEFT JOIN base.person_info pi " \
              "ON im.matched_id = pi.person_id " \
              "JOIN (SELECT matched_id as org_id_matched, source_id as org_id_source FROM base.id_match " \
              "WHERE id_type = 2 AND source = '{sid}' AND is_used = 1) im_org " \
              "ON im_org.org_id_source = tb_main.org_id " \
              "LEFT JOIN base.org_info oi " \
              "ON im_org.org_id_matched = oi.org_id".format(sid=SOURCE)
        inp = MysqlInput(ENGINE, sql)

        DUTY_ORD = {
            "执行事务合伙人（委派代表）": 0,
            "合伙人": 1,
            "法定代表人": 2,
            "董事长": 3,
            "总经理": 4,
            "董事总经理": 5,
            "执行董事": 6,
            "副总经理": 7,
            "监事": 8,
            "合规风控": 9,
            "信息填报负责人": 10,
            "其他": 11
        }

        vm = transform.ValueMap({
            "duty_detail": (lambda x: ",".join(sorted(x.split(","), key=lambda d: DUTY_ORD.get(d, 10000))), "duty"),
            "duty": "高管",
            "is_current": 1
        })

        sk = transform.MapSelectKeys(
            {
                "matched_id": "person_id",
                "person_name": "person_name",
                "org_id": "org_id",
                "org_name": "org_name",
                "duty": "duty",
                "duty_detail": "duty_detail",
                "is_current": "is_current"
            }
        )

        s = Stream(inp, transform=[vm, sk])

        return s

    @classmethod
    def stream_010101(cls):
        sql = "SELECT im.matched_id as person_id, pi.person_name, xpc.status, xpc.org_name as org_name_ " \
              "FROM crawl_private.x_person_certificate_010101 xpc " \
              "JOIN base.id_match im ON xpc.person_id = im.source_id " \
              "LEFT JOIN base.person_info pi ON im.matched_id = pi.person_id " \
              "WHERE im.source = '010101' AND im.id_type = 3 AND im.is_used = 1 " \
              "AND xpc.org_name not like '空'"

        def org_name_dict():
            import pandas as pd
            sql_oi = "SELECT org_full_name, org_id, org_name FROM base.org_info"
            res = pd.read_sql(sql_oi, ENGINE)
            v = tuple([(oid, oname) for oid, oname in zip(res["org_id"], res["org_name"])])
            res = dict(zip(res["org_full_name"], v))
            return res

        inp = MysqlInput(ENGINE, sql)

        d = org_name_dict()

        vm = transform.ValueMap({
            "org_id": (lambda x: d.get(x)[0], "org_name_"),
            "org_name": (lambda x: d.get(x)[1], "org_name_"),
            "is_current": (lambda x: {"正常": "1", "离职": "0"}.get(x), "status"),
            "duty": None
        })

        sk = transform.MapSelectKeys({
            "person_id": "person_id",
            "person_name": "person_name",
            "org_id": "org_id",
            "org_name": "org_name",
            "is_current": "is_current",
            "duty": "duty"
        })

        dn = transform.Dropna(
            subset=["person_id", "org_id"]
        )

        s = Stream(inp, [vm, sk, dn])
        return s

    @classmethod
    def stream_op_020001(cls):
        SOURCE = "020001"

        sql = "SELECT im.matched_id, pi.person_name, im_org.org_id_matched as org_id, oi.org_name, duty, " \
              "tb_main.person_id as person_id_, tb_main.org_name as org_name_ " \
              "FROM crawl_private.d_org_person tb_main " \
              "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.d_org_person GROUP BY person_id) tb_latest " \
              "ON tb_main.version = tb_latest.latest_ver AND tb_main.person_id = tb_latest.person_id " \
              "JOIN base.id_match im " \
              "ON im.source_id = tb_main.person_id AND im.id_type = 3 AND im.source = '{sid}' AND im.is_used = 1 " \
              "LEFT JOIN base.person_info pi " \
              "ON im.matched_id = pi.person_id " \
              "JOIN (SELECT matched_id as org_id_matched, source_id as org_id_source FROM base.id_match " \
              "WHERE id_type = 2 AND source = '{sid}' AND is_used = 1) im_org " \
              "ON im_org.org_id_source = tb_main.org_id " \
              "LEFT JOIN base.org_info oi " \
              "ON im_org.org_id_matched = oi.org_id".format(sid=SOURCE)

        inp = MysqlInput(ENGINE, sql)

        def tenure_date():
            import pandas as pd
            sql = "SELECT dpi.person_id, org_name, tenure_date " \
                  "FROM crawl_private.d_person_info dpi " \
                  "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.d_person_info GROUP BY person_id) t " \
                  "ON dpi.person_id = t.person_id AND dpi.version = t.latest_ver "
            tmp = pd.read_sql(sql, ENGINE).dropna(subset=["tenure_date"])
            tmp["org_name"] = tmp["org_name"].apply(lambda x: re.sub("\s", "", x))
            k = tuple([(pid, on) for pid, on in zip(tmp["person_id"], tmp["org_name"])])
            return dict(zip(k, tmp["tenure_date"]))

        class DutyCleanTransform(transform.BaseTransform):

            @classmethod
            def set_up(cls):
                EXECUTIVE = ("总监", "主管", "总裁", "董事", "主席", "合伙", "创办", "创立", "总经理")
                IR = ("研究", "分析", "经济", "策略", "投资", "调研员", "研究",)
                RISK = ("风险", "风控",)
                MANAGER = ("基金经理",)

                EXECUTIVE_PATT = "|".join(EXECUTIVE)
                IR_PATT = "|".join(IR)
                RISK_PATT = "|".join(RISK)
                MANAGER_PATT = "基金经理(?!助理)"

                cls.DUTY = (MANAGER, EXECUTIVE, IR, RISK)
                cls.DUTY_PATT = (MANAGER_PATT, EXECUTIVE_PATT, IR_PATT, RISK_PATT)
                cls.DUTY_NAME = ("基金经理", "高管", "投研", "风控")
                cls.AND_PATT = "兼|&|及"

                cls.DKT = {}
                for duty, kw_lst in dict(zip(cls.DUTY_NAME, cls.DUTY)).items():
                    cls.DKT.update(dict.fromkeys(kw_lst, duty))

            @classmethod
            def _clean_duty(cls, string):
                res = []
                if string is None:
                    return res
                duty_string_lst = re.split(cls.AND_PATT, string)
                for duty_string in duty_string_lst:
                    duty_matched = None
                    for duty_patt in cls.DUTY_PATT:
                        sre = re.search(duty_patt, duty_string)
                        if sre is not None:
                            duty_matched = cls.DKT[sre.group()]
                            break
                    if duty_matched is None:
                        duty_matched = "其他"
                    res.append((duty_matched, duty_string))
                return res

            def process(self, frame):
                self.set_up()
                frame["duty"] = frame["duty"].apply(lambda x: self._clean_duty(x))
                key_cols = [x for x in inp.dataframe.columns if x != "duty"]
                dkt = dict((zip(tuple(zip(*[frame[col] for col in key_cols])), frame["duty"])))
                df_new = DataFrame.from_dict(dkt, orient="index")
                df_new = df_new.T.stack().reset_index(level=[-1])
                df_new.columns = ["ID", "DT"]
                for idx, key in enumerate(key_cols):
                    df_new[key] = df_new["ID"].apply(lambda x: x[idx])

                for idx, key in enumerate(("duty", "duty_detail")):
                    df_new[key] = df_new["DT"].apply(lambda x: x[idx])

                df_new = df_new.drop(["ID", "DT"], axis=1)

                return df_new

        dc = DutyCleanTransform()

        d = tenure_date()

        vm = transform.ValueMap(
            {
                "is_current": 1,
                "tenure_date": (lambda pid, oname: d.get((pid, oname)), "person_id_", "org_name_")
            }
        )

        sk = transform.MapSelectKeys(
            {
                "matched_id": "person_id",
                "person_name": "person_name",
                "org_id": "org_id",
                "org_name": "org_name",
                "duty": "duty",
                "duty_detail": "duty_detail",
                "is_current": "is_current",
                "tenure_date": "tenure_date",
            }
        )

        s = Stream(inp, transform=[dc, vm, sk])

        return s

    @classmethod
    def stream_op_020002(cls):
        SOURCE = "020002"

        sql = "SELECT im.matched_id, pi.person_name, im_org.org_id_matched as org_id, oi.org_name, duty " \
              "FROM crawl_private.d_org_person tb_main " \
              "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.d_org_person GROUP BY person_id) tb_latest " \
              "ON tb_main.version = tb_latest.latest_ver AND tb_main.person_id = tb_latest.person_id " \
              "JOIN base.id_match im " \
              "ON im.source_id = tb_main.person_id AND im.id_type = 3 AND im.source = '{sid}' AND im.is_used = 1 " \
              "LEFT JOIN base.person_info pi " \
              "ON im.matched_id = pi.person_id " \
              "JOIN (SELECT matched_id as org_id_matched, source_id as org_id_source FROM base.id_match " \
              "WHERE id_type = 2 AND source = '{sid}' AND is_used = 1) im_org " \
              "ON im_org.org_id_source = tb_main.org_id " \
              "LEFT JOIN base.org_info oi " \
              "ON im_org.org_id_matched = oi.org_id".format(sid=SOURCE)

        inp = MysqlInput(ENGINE, sql)

        vm = transform.ValueMap({
            "duty_detail": (lambda x: x, "duty"),
            "duty": (lambda x: {"基金经理": "基金经理"}.get(x, "其他"), "duty"),
            "is_current": 1
        })

        sk = transform.MapSelectKeys(
            {
                "matched_id": "person_id",
                "person_name": "person_name",
                "org_id": "org_id",
                "org_name": "org_name",
                "duty": "duty",
                "duty_detail": "duty_detail",
                "is_current": "is_current"
            }
        )

        s = Stream(inp, transform=[vm, sk])

        return s

    @classmethod
    def conflu_1(cls):
        """
            合并010001, 020001, 020002源数据流

        Returns:
            base.Confluence

        """

        s11 = cls.stream_010001()
        s21 = cls.stream_op_020001()
        s22 = cls.stream_op_020002()
        sy = cls.stream_y()
        c = Confluence(s11, s21, s22, sy, on=["person_id", "org_id", "duty"])

        return c

    @classmethod
    def conflu_2(cls):
        """
            合并数据源010101数据流和合流1

        Returns:
            base.Confluence

        """

        c1 = cls.conflu_1()
        s11 = cls.stream_010101()

        c = Confluence(s11, c1, on=["org_id", "person_id"])

        df = c.dataframe.dropna(subset=['duty'])
        return df

    @classmethod
    def write(cls):
        df = cls.conflu_2()
        io.to_sql("data_test.org_person_mapping_test_sm", ENGINE, df)


def main():
    Streams.write()


if __name__ == "__main__":
    main()
