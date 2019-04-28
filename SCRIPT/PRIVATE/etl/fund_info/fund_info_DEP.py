from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlNativeInput
from utils.database.sqlfactory import SQL
from collections import OrderedDict
import re
import datetime as dt
from dateutil.relativedelta import relativedelta

engine_c = cfg.load_engine()["2Gcpri"]
engine_b = cfg.load_engine()["etl_base_private"]
engine_t = cfg.load_engine()["etl_base_test"]


def fetch_fids_to_update():
    upt = (dt.datetime.now() - relativedelta(minutes=12)).strftime("%Y%m%d%H%M%S")
    fids = []
    sql_srcs = [
        "SELECT DISTINCT matched_id FROM base.id_match WHERE update_time >= '{upt}'".format(upt=upt),
        "SELECT DISTINCT fund_id FROM crawl_private.y_fund_info WHERE update_time >= '{upt}' AND is_used = 1".format(upt=upt),
        "SELECT DISTINCT im.matched_id FROM crawl_private.x_fund_info_fundaccount xfi JOIN base.id_match im ON xfi.fund_id = im.source_id AND im.source='010002' WHERE xfi.update_time >= '{upt}' AND im.id_type=1 AND im.is_used=1".format(upt=upt),
        "SELECT DISTINCT im.matched_id FROM crawl_private.x_fund_info_private xfi JOIN base.id_match im ON xfi.fund_id = im.source_id AND im.source='010003' WHERE xfi.update_time >= '{upt}' AND im.id_type=1 AND im.is_used=1".format(upt=upt),
        "SELECT DISTINCT im.matched_id FROM crawl_private.x_fund_info_securities xfi JOIN base.id_match im ON xfi.fund_id = im.source_id AND im.source='010004' WHERE xfi.update_time >= '{upt}' AND im.id_type=1 AND im.is_used=1".format(upt=upt),
        "SELECT DISTINCT im.matched_id FROM crawl_private.x_fund_info_futures xfi JOIN base.id_match im ON xfi.fund_id = im.source_id AND im.source='010005' WHERE xfi.update_time >= '{upt}' AND im.id_type=1 AND im.is_used=1".format(upt=upt),
        "SELECT DISTINCT im.matched_id FROM crawl_private.d_fund_info dfi JOIN base.id_match im ON dfi.fund_id = im.source_id AND dfi.source_id = im.source WHERE dfi.update_time >= '{upt}' AND im.is_used=1 AND im.id_type=1".format(upt=upt),
        ]
    for sql in sql_srcs:
        tmp = [x[0] for x in engine_b.execute(sql).fetchall()]
        fids.extend(tmp)
    return sorted(set(fids))


def stream_y_fund_info(fund_ids=None):
    sql = "\
    SELECT TB_MAIN.version, TB_MAIN.fund_id, fund_name, fund_full_name, fund_status, liquidation_cause, end_date \
    FROM y_fund_info TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) as latest_ver FROM y_fund_info WHERE is_used = 1 GROUP BY fund_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id "

    if fund_ids is not None:
        fids = SQL.values4sql(fund_ids)
        sql += "WHERE TB_MAIN.fund_id IN {fids}".format(fids=fids)

    inp = MysqlNativeInput(engine_c, sql)

    ac = transform.AddConst({"source_id": "000001"})

    dd = transform.SortDropduplicate(sort_by=["version", "fund_id"], ascending=[False, True], subset=["fund_id"])

    km = transform.MapSelectKeys(
        {"fund_id": None, "fund_name": None, "fund_full_name": None, "fund_name_en": None, "source_id": None,
         "fund_status": None, "liquidation_cause": None, "end_date": None}
    )
    stream = Stream(inp, (ac, dd, km,))
    return stream


def stream_x_fund_info_010002(fund_ids=None):
    sql = "\
    SELECT TB_MAIN.version, im.matched_id, TB_MAIN.fund_id, fund_name_amac, reg_code_amac, reg_time_amac, fund_time_limit_amac, issuing_scale_amac, number_clients_amac \
    FROM x_fund_info_fundaccount TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_fundaccount WHERE is_used = 1 GROUP BY fund_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id \
    JOIN base.id_match im ON im.source_id = TB_MAIN.fund_id AND id_type = 1 AND source = '010002' AND im.is_used = 1 "

    if fund_ids is not None:
        fids = SQL.values4sql(fund_ids)
        sql += "WHERE im.matched_id IN {fids}".format(fids=fids)

    inp = MysqlNativeInput(engine_c, sql)

    def clean_fund_status(reg_time, limit_time):
        from dateutil.relativedelta import relativedelta
        now = dt.date.today()
        if reg_time is not None and limit_time is not None and limit_time > 0:
            if limit_time > 1200:  # 有些无限期的limit_time会被标记为9999月
                return "运行中"
            limit_date = reg_time + relativedelta(months=int(limit_time))
            if limit_date <= now:
                return "终止"
            else:
                return "运行中"
        return None

    vm = transform.ValueMap(
        {
            "fund_name_amac": lambda x: x.strip(),
            "reg_code_amac": lambda x: x.strip(),
            "fund_time_limit_amac": lambda x: int(x),
            "issuing_scale_amac": lambda x: x * 1e4 if x is not None else None,
            "number_clients_amac": lambda x: int(x),
        }
    )

    vm2 = transform.ValueMap(
        {
            "fund_status": (lambda x, y: clean_fund_status(x, y), "reg_time_amac", "fund_time_limit_amac")
        }
    )

    cln = transform.CleanWrongToNone(
        {"reg_code_amac": ""}, repls=None
    )

    # jn = transform.Join(
    #     inp2, left_on="fund_id", right_on="source_id"
    # )

    ac = transform.AddConst({"source_id": "010002"})

    dd = transform.SortDropduplicate(
        sort_by=["version", "matched_id"], ascending=[False, True], subset="matched_id", keep="first"
    )

    km = transform.MapSelectKeys(
        {
            # "version": None,
            "matched_id": "fund_id",
            "source_id": None,
            "reg_code_amac": "reg_code",
            "reg_time_amac": "reg_time",
            "fund_status": "fund_status",
            "fund_time_limit_amac": "limit_time",
            "issuing_scale_amac": "issuing_scale",
            "number_clients_amac": "number_clients",
            "fund_name_amac": "fund_full_name",
        }
    )

    stream = Stream(
        inp, (vm, vm2, cln, dd, ac, km), name="x_fund_info_account"
    )

    return stream


def stream_x_fund_info_010003(fund_ids=None):
    def clean_currency(x):
        patt = "(?P<currency>人民币|美元|多币种|欧元|港元|澳元|其他|其它|日元|英镑)?(?P<type>现钞|现汇)?"
        cpatt = re.compile(patt)
        sre = cpatt.search(x)
        if sre is not None:
            return sre.groupdict()
        return {}

    sql = "\
    SELECT TB_MAIN.version, im.matched_id, TB_MAIN.fund_id, fund_name_amac, reg_code_amac, reg_time_amac, foundation_date_amac, currency_name_amac, \
    fund_status_amac, final_report_time_amac \
    FROM x_fund_info_private TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_private WHERE is_used = 1 GROUP BY fund_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id \
    JOIN base.id_match im ON im.source_id = TB_MAIN.fund_id AND id_type = 1 AND source = '010003' AND im.is_used = 1 "

    if fund_ids is not None:
        fids = SQL.values4sql(fund_ids)
        sql += "WHERE im.matched_id IN {fids}".format(fids=fids)

    inp = MysqlNativeInput(engine_c, sql)

    vm = transform.ValueMap(
        OrderedDict(
            [
                ("fund_name_amac", lambda x: x.strip()),
                ("reg_code_amac", lambda x: x.strip()),
                ("is_abnormal_liquidation",
                 (lambda x: {"延期清算": 1, "提前清算": 1, "投顾协议已终止": 1, "正常清算": 0}.get(x), "fund_status_amac")),
                ("liquidation_cause",
                 (lambda x: {"延期清算": "延期清算", "提前清算": "提前清算", "投顾协议已终止": "投顾协议已终止"}.get(x), "fund_status_amac")),
                ("fund_status_amac", {"正常清算": "终止", "正在运作": "运行中", "延期清算": "终止", "提前清算": "终止", "投顾协议已终止": "终止"}),
                ("currency_type", (lambda x: clean_currency(x).get("type"), "currency_name_amac")),
                ("currency_name_amac", lambda x: clean_currency(x).get("currency")),
            ]
        )
    )

    cln = transform.CleanWrongToNone(
        {"reg_code_amac": ""}, repls=None
    )

    # jn = transform.Join(
    #     inp2, left_on="fund_id", right_on="source_id"
    # )

    dd = transform.SortDropduplicate(
        sort_by=["version", "matched_id"], ascending=[False, True], subset="matched_id", keep="first"
    )

    ac = transform.AddConst({"source_id": "010003"})

    km = transform.MapSelectKeys(
        {
            # "version": None,
            "matched_id": "fund_id",
            "source_id": None,
            "fund_name_amac": "fund_full_name",
            "reg_code_amac": "reg_code",
            "reg_time_amac": "reg_time",
            "foundation_date_amac": "foundation_date",
            "currency_name_amac": "currency",
            "currency_type": None,
            "fund_status_amac": "fund_status",
            "is_abnormal_liquidation": None,
            "liquidation_cause": None
        }
    )

    stream = Stream(inp, (vm, cln, dd, ac, km), name="x_fund_info_private")
    return stream


# sec
def stream_x_fund_info_010004(fund_ids=None):
    def clean_fund_status(limit_date, limit_time):
        if limit_date is not None:
            if limit_date <= dt.date.today():
                return "终止"
            else:
                return "运行中"
        if limit_time is not None:
            if limit_time == "无期限":
                return "运行中"
        return None

    sql = "\
    SELECT TB_MAIN.version, im.matched_id, TB_MAIN.fund_id, fund_name_amac, reg_code_amac, foundation_date_amac, fund_time_limit_amac \
    FROM x_fund_info_securities TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_securities WHERE is_used = 1 GROUP BY fund_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id \
    JOIN base.id_match im ON im.source_id = TB_MAIN.fund_id AND id_type = 1 AND source = '010004' AND im.is_used = 1 "
    if fund_ids is not None:
        fids = SQL.values4sql(fund_ids)
        sql += "WHERE im.matched_id IN {fids}".format(fids=fids)

    inp = MysqlNativeInput(engine_c, sql)

    vm = transform.ValueMap(
        OrderedDict(
            [
                ("fund_name_amac", lambda x: x.strip()),
                ("reg_code_amac", lambda x: x.strip()),
                ("limit_date", (lambda x: dt.datetime.strptime(x, "%Y-%m-%d").date(), "fund_time_limit_amac")),
                ("fund_time_limit_amac", lambda x: x if x == "无期限" else None),
                ("fund_status", (lambda x, y: clean_fund_status(x, y), "limit_date", "fund_time_limit_amac"))
            ]
        )
    )

    ac = transform.AddConst({"source_id": "010004"})

    cln = transform.CleanWrongToNone(
        {"reg_code_amac": ""}, repls=None
    )


    dd = transform.SortDropduplicate(
        sort_by=["version", "matched_id"], ascending=[False, True], subset="matched_id", keep="first"
    )

    km = transform.MapSelectKeys(
        {
            # "version": None,
            "matched_id": "fund_id",
            "source_id": None,
            "fund_name_amac": "fund_full_name",
            "reg_code_amac": "reg_code",
            "foundation_date_amac": "foundation_date",
            "fund_time_limit_amac": "limit_time",
            "limit_date": "limit_date",
            "fund_status": "fund_status"
        }
    )

    stream = Stream(inp, (vm, cln, ac, dd, km), name="x_fund_info_security")
    return stream


# fut
def stream_x_fund_info_010005(fund_ids=None):
    sql = "\
    SELECT TB_MAIN.version, im.matched_id, TB_MAIN.fund_id, fund_name_amac, reg_code_amac, foundation_date_amac, issuing_scale_amac, number_clients_amac \
    FROM x_fund_info_futures TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_futures WHERE is_used = 1 GROUP BY fund_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id \
    JOIN base.id_match im ON im.source_id = TB_MAIN.fund_id AND id_type = 1 AND source = '010005' AND im.is_used = 1 "

    if fund_ids is not None:
        fids = SQL.values4sql(fund_ids)
        sql += "WHERE im.matched_id IN {fids}".format(fids=fids)

    inp = MysqlNativeInput(engine_c, sql)

    vm = transform.ValueMap(
        OrderedDict(
            [
                ("fund_name_amac", lambda x: x.strip()),
                ("reg_code_amac", lambda x: x.strip()),
                ("number_clients_amac", lambda x: int(x)),
            ]
        )
    )

    cln = transform.CleanWrongToNone(
        {"reg_code_amac": ""}, repls=None
    )

    ac = transform.AddConst({"source_id": "010005"})

    # jn = transform.Join(inp2, left_on="fund_id", right_on="source_id")

    dd = transform.SortDropduplicate(
        sort_by=["version", "matched_id"], ascending=[False, True], subset="matched_id", keep="first"
    )

    km = transform.MapSelectKeys(
        {
            "matched_id": "fund_id",
            "source_id": None,
            "fund_name_amac": "fund_full_name",
            "reg_code_amac": "reg_code",
            "foundation_date_amac": "foundation_date",
            "issuing_scale_amac": "issuing_scale",
            "number_clients_amac": "number_clients",
        }
    )

    stream = Stream(inp, (vm, cln, ac, dd, km), name="x_fund_info_future")
    return stream


def stream_d_fund_info_020001(fund_ids=None):
    sql = "\
    SELECT TB_MAIN.version, im.matched_id, TB_MAIN.fund_id, TB_MAIN.source_id, fund_name, fund_full_name, reg_code, fund_status, locked_time_limit, open_date, foundation_date \
    FROM d_fund_info TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) latest_ver, source_id FROM d_fund_info WHERE source_id = '020001' and is_used = 1 GROUP BY fund_id, source_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id AND TB_MAIN.source_id = TB_LATEST.source_id \
    JOIN base.id_match im ON im.source_id = TB_MAIN.fund_id AND id_type = 1 AND source = '020001' AND im.is_used = 1 "

    if fund_ids is not None:
        fids = SQL.values4sql(fund_ids)
        sql += "WHERE im.matched_id IN {fids}".format(fids=fids)

    # JOIN base.id_match im ON im.source_id = TB_MAIN.fund_id AND id_type = 1 AND source = '010004' AND im.is_used = 1"

    inp1 = MysqlNativeInput(engine_c, sql)

    # inp2 = MysqlNativeInput(engine_b, "SELECT matched_id, source_id FROM id_match WHERE id_type=1 AND source='020001' AND is_used = 1")

    vm = transform.ValueMap(
        {
            "fund_name": lambda x: x.strip(),
            "fund_full_name": lambda x: x.strip(),
            "fund_status": {"正常": "运行中", "终止": "终止"},
        }
    )

    cln1 = transform.CleanWrongToNone({
        "locked_time_limit": "-|--",
        "open_date": "-|--"
    })

    cln2 = transform.CleanRight({
        "reg_code": "\w\w\d{4}"
    })
    # jn = transform.Join(inp2, left_on="fund_id", right_on="source_id")

    dd = transform.SortDropduplicate(
        sort_by=["version", "matched_id"], ascending=[False, True], subset="matched_id", keep="first"
    )

    km = transform.MapSelectKeys(
        {
            # "version": None,
            "matched_id": "fund_id",
            "source_id_x": "source_id",
            "fund_name": None,
            "fund_full_name": None,
            "reg_code": None,
            "fund_status": None,
            "open_date": None,
            "foundation_date": None
        }
    )

    stream = Stream(inp1, (vm, cln1, cln2, dd, km), name="d_fund_info")
    return stream


def stream_d_fund_info_020002(fund_ids=None):
    sql = "\
    SELECT TB_MAIN.version, im.matched_id, TB_MAIN.fund_id, TB_MAIN.source_id, fund_name, fund_full_name, fund_status, locked_time_limit, \
    open_date, foundation_date \
    FROM d_fund_info TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) latest_ver, source_id FROM d_fund_info WHERE source_id = '020002' and is_used = 1 GROUP BY fund_id, source_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id AND TB_MAIN.source_id = TB_LATEST.source_id \
    JOIN base.id_match im ON im.source_id = TB_MAIN.fund_id AND id_type = 1 AND source = '020002' AND im.is_used = 1 "

    if fund_ids is not None:
        fids = SQL.values4sql(fund_ids)
        sql += "WHERE im.matched_id IN {fids}".format(fids=fids)


    inp = MysqlNativeInput(engine_c, sql)

    cln1 = transform.CleanWrongToNone(
        {"open_date": "-|--|---"}
    )

    cln2 = transform.CleanRight(
        {
            "locked_time_limit": "(\d*)个月|无封闭期",
        }

    )
    vm = transform.ValueMap(
        {
            "fund_status": {"运行中": "运行中", "已清算": "终止", "封闭运行": "运行中", "开放运行": "运行中", "募集中": "募集发行"}
        }
    )

    dd = transform.SortDropduplicate(
        sort_by=["version", "matched_id"], ascending=[False, True], subset="matched_id", keep="first"
    )

    km = transform.MapSelectKeys(
        {
            # "version": None,
            "matched_id": "fund_id",
            "fund_name": None,
            "fund_full_name": None,
            "source_id_x": "source_id",
            "fund_status": None,
            "locked_time_limit": None,
            "open_date": None,
            "foundation_date": None
        }
    )

    stream = Stream(inp, (cln1, cln2, vm, dd, km))
    return stream


def conflu1(fund_ids=None):
    s00 = stream_y_fund_info(fund_ids)
    s12 = stream_x_fund_info_010002(fund_ids)
    s13 = stream_x_fund_info_010003(fund_ids)
    s14 = stream_x_fund_info_010004(fund_ids)
    s15 = stream_x_fund_info_010005(fund_ids)
    s21 = stream_d_fund_info_020001(fund_ids)
    s22 = stream_d_fund_info_020002(fund_ids)

    p = {
        0: {
            "fund_name": ("source_id", "000001"),
            "fund_full_name": ("source_id", "010002"),
            "reg_time": ("source_id", "010003"),
            "reg_code": ("source_id", "010002"),
        },
        1: {
            "reg_code": ("source_id", "010003"),
            "fund_full_name": ("source_id", "010003"),
        },
        2: {
            "fund_full_name": ("source_id", "010004"),
        },
        3: {
            "fund_full_name": ("source_id", "010005"),
        }
    }
    conflu = Confluence(s00, s12, s13, s14, s15, s21, s22, on=["fund_id"], prio_l1=p)
    return conflu


def stream_main(fund_ids=None):
    c = conflu1(fund_ids)

    fn = transform.FillnaByColumn(
        {
            "foundation_date": "reg_time",
            "fund_name": "fund_full_name"
        }
    )

    s = Stream(c, transform=(fn,))
    return s


def encapsulate_to_old(dataframe):
    res = dataframe.drop(["source_id", "number_clients", "currency_type", "issuing_scale"], errors="ignore", axis=1)
    res["end_date"] = res["end_date"].fillna(res["limit_date"])
    del res["limit_date"]
    res = res.rename(columns={"limit_date": "end_date", "limit_time": "fund_time_limit"})
    return res


def write(dataframe):
    io.to_sql("fund_info", engine_t, dataframe.drop(["source_id"], axis=1))
    io.to_sql("fund_info", engine_b, encapsulate_to_old(dataframe))


def main():
    fund_ids = fetch_fids_to_update()
    if len(fund_ids) > 0:
        s = stream_main(fund_ids)
        s.flow()
        write(s.dataframe)

if __name__ == "__main__":
    main()
