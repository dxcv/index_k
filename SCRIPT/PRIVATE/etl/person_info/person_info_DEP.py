from utils.database import io, config as cfg
from utils.database.models.base_private import PersonInfo
from utils.etlkit.core import base, transform
from utils.etlkit.reader.mysqlreader import MysqlInput
from pypinyin import pinyin as py, Style as py_style
from collections import OrderedDict

ENGINE = cfg.load_engine()["etl01"]


def stream_010001():
    import re
    sql = "SELECT im.matched_id, `name`, qualifying_way FROM crawl_private.x_org_executive_info tb_main " \
          "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.x_org_executive_info GROUP BY person_id) tb_latest " \
          "ON tb_main.version = tb_latest.latest_ver AND tb_main.person_id = tb_latest.person_id " \
          "JOIN base.id_match im ON im.source_id = tb_main.person_id " \
          "AND im.id_type = 3 AND im.source = '010001' AND im.is_used = 1 "

    inp = MysqlInput(ENGINE, sql)

    vm = transform.ValueMap(OrderedDict([
        [PersonInfo.person_name.name, (lambda x: re.sub("（.*）", "", x), "name")],
        [PersonInfo.person_name_py.name, (lambda x: "".join([x[0] for x in py(x, style=py_style.FIRST_LETTER)]).upper(), PersonInfo.person_name.name)],
        [PersonInfo.fund_qualification_way.name, (lambda x: {"通过考试": "通过考试", "资格认定": "资格认定"}.get(x), "qualifying_way")],
        [PersonInfo.is_fund_qualification.name, (lambda x: int(bool(x)), PersonInfo.fund_qualification_way.name)],
    ]))

    sk = transform.MapSelectKeys(
        {
            "matched_id": PersonInfo.person_id.name,
            PersonInfo.person_name.name: None,
            PersonInfo.person_name_py.name: None,
            PersonInfo.fund_qualification_way.name: None,
            PersonInfo.is_fund_qualification.name: None
        }
    )

    s = base.Stream(inp, transform=(vm, sk))

    return s


def stream_020001():
    sql = "SELECT im.matched_id, person_name, background FROM crawl_private.d_person_info tb_main " \
          "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.d_person_info GROUP BY person_id) tb_latest " \
          "ON tb_main.version = tb_latest.latest_ver AND tb_main.person_id = tb_latest.person_id " \
          "JOIN base.id_match im ON im.source_id = tb_main.person_id " \
          "AND im.id_type = 3 AND im.source = '020001' AND im.is_used = 1 "

    inp = MysqlInput(ENGINE, sql)

    vm = transform.ValueMap(OrderedDict([
        [PersonInfo.person_name_py.name,
         (lambda x: "".join([x[0] for x in py(x, style=py_style.FIRST_LETTER)]).upper(), "person_name")],
        [PersonInfo.background.name, (lambda x: {'公募': '公募', '其它': "其他", '券商': "券商", '海外': "海外",
                                                 "民间": "民间", "学者": "学者", '实业': "实业", '保险': "保险",
                                                 '媒体': "媒体", '期货': '期货'}.get(x, "其他"), "background")],
    ]))

    sk = transform.MapSelectKeys(
        {
            "matched_id": PersonInfo.person_id.name,
            "person_name": PersonInfo.person_name.name,
            PersonInfo.person_name_py.name: None,
            PersonInfo.background.name: None,
        }
    )
    s = base.Stream(inp, transform=(vm, sk))

    return s


def stream_020001_op():
    sql = "SELECT im.matched_id, person_name FROM crawl_private.d_org_person tb_main " \
          "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.d_org_person GROUP BY person_id) tb_latest " \
          "ON tb_main.version = tb_latest.latest_ver AND tb_main.person_id = tb_latest.person_id " \
          "JOIN base.id_match im ON im.source_id = tb_main.person_id " \
          "AND im.id_type = 3 AND im.source = '020001' AND im.is_used = 1 "

    inp = MysqlInput(ENGINE, sql)

    vm = transform.ValueMap(OrderedDict([
        [PersonInfo.person_name_py.name,
         (lambda x: "".join([x[0] for x in py(x, style=py_style.FIRST_LETTER)]).upper(), "person_name")],
    ]))

    sk = transform.MapSelectKeys(
        {
            "matched_id": PersonInfo.person_id.name,
            "person_name": PersonInfo.person_name.name,
            PersonInfo.person_name_py.name: None,
        }
    )
    s = base.Stream(inp, transform=(vm, sk))

    return s


def stream_020002():
    sql = "SELECT im.matched_id, person_name, background FROM crawl_private.d_person_info tb_main " \
          "JOIN (SELECT person_id, MAX(version) latest_ver FROM crawl_private.d_person_info GROUP BY person_id) tb_latest " \
          "ON tb_main.version = tb_latest.latest_ver AND tb_main.person_id = tb_latest.person_id " \
          "JOIN base.id_match im ON im.source_id = tb_main.person_id " \
          "AND im.id_type = 3 AND im.source = '020002' AND im.is_used = 1 "

    inp = MysqlInput(ENGINE, sql)
    inp.dataframe
    inp.dataframe["background"].drop_duplicates().tolist()

    vm = transform.ValueMap(OrderedDict([
        [PersonInfo.person_name.name, lambda x: x.strip()],
        [PersonInfo.person_name_py.name, (lambda x: "".join([x[0] for x in py(x, style=py_style.FIRST_LETTER)]).upper(), "person_name")],
        [PersonInfo.background.name, (lambda x: {'公募': '公募', '其它': "其他", '券商': "券商", '海外': "海外",
                                                 "民间": "民间", "学者": "学者", '实业': "实业", '保险': "保险",
                                                 '媒体': "媒体", '期货': '期货'}.get(x, "其他"), "background")],
    ]))

    sk = transform.MapSelectKeys(
        {
            "matched_id": PersonInfo.person_id.name,
            "person_name": PersonInfo.person_name.name,
            PersonInfo.person_name_py.name: None,
            PersonInfo.background.name: None,
        }
    )
    s = base.Stream(inp, transform=(vm, sk))

    return s


def main():
    s11 = stream_010001()
    s21 = stream_020001()
    s22 = stream_020002()

    s21_op = stream_020001_op()

    c = base.Confluence(s11, s21, s22, s21_op, on=[PersonInfo.person_id.name])

    io.to_sql(PersonInfo.__schema_table__, ENGINE, c.dataframe)

if __name__ == "__main__":
    main()
