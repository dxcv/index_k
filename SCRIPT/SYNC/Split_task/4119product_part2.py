"""
同步 - 4119product库同步part2(3H)
"""

import sys
import os
try:
    sys.path.append(os.getcwd()[:os.getcwd().index("SCRIPT")])
except:
    pass

import datetime as dt
from dateutil.relativedelta import relativedelta
from utils.database import config as cfg
from utils.synckit.cli import tools
from utils.synckit.mysqlreader import splitter


ENGINE_RD = cfg.load_engine()["2Gb"]
ENGINE_4119PRODUCT = cfg.load_engine()["4Gp"]


tasks = [

    (splitter.MysqlReader("fund_weekly_return", ENGINE_RD), splitter.MysqlReader("fund_weekly_return", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_weekly_risk", ENGINE_RD), splitter.MysqlReader("fund_weekly_risk", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_weekly_risk2", ENGINE_RD), splitter.MysqlReader("fund_weekly_risk2", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_subsidiary_weekly_index", ENGINE_RD), splitter.MysqlReader("fund_weekly_risk", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_subsidiary_weekly_index2", ENGINE_RD), splitter.MysqlReader("fund_weekly_subsidiary", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_subsidiary_weekly_index3", ENGINE_RD), splitter.MysqlReader("fund_weekly_subsidiary2", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_month_return", ENGINE_RD), splitter.MysqlReader("fund_month_return", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_month_risk", ENGINE_RD), splitter.MysqlReader("fund_month_risk", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_month_risk2", ENGINE_RD), splitter.MysqlReader("fund_month_risk2", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_subsidiary_month_index", ENGINE_RD), splitter.MysqlReader("fund_month_risk", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_subsidiary_month_index2", ENGINE_RD), splitter.MysqlReader("fund_month_subsidiary", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_subsidiary_month_index3", ENGINE_RD), splitter.MysqlReader("fund_month_subsidiary2", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_rank", ENGINE_RD), splitter.MysqlReader("fund_rank", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_nv_data_standard", ENGINE_RD), splitter.MysqlReader("fund_nv_data_standard", ENGINE_4119PRODUCT)),
]


def sync(t0, t1):
    WHERE = "WHERE update_time BETWEEN '{t0}' AND '{t1}'".format(
        t0=t0.strftime("%Y%m%d%H%M%S"),
        t1=t1.strftime("%Y%m%d%H%M%S")
    )
    for TASK_NO in range(0, len(tasks)):
        print("task_no: {no}; task: {src} --> {tgt}".format(
            no=TASK_NO, src=tasks[TASK_NO][0]._name, tgt=tasks[TASK_NO][1]._name))

        try:
            if tasks[TASK_NO][0]._name in (
                    "fund_weekly_return", "fund_weekly_risk", "fund_subsidiary_weekly_index",
                    "fund_month_return", "fund_month_risk", "fund_subsidiary_month_index",
                    "fund_month_risk2", "fund_weekly_risk2", "fund_subsidiary_weekly_index2",
                    "fund_subsidiary_weekly_index3", "fund_subsidiary_month_index2", "fund_subsidiary_month_index3",
                    "fund_weekly_subsidiary", "fund_weekly_subsidiary2", "fund_monthly_subsidiary",
                    "fund_monthly_subsidiary2"
            ):
                j = splitter.Job(
                    tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE,
                    where_del="WHERE fund_id LIKE 'JR%%'"
                )
                j.update()

            elif tasks[TASK_NO][0]._name in ("fund_type_mapping",):
                j = splitter.Job(
                    tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE + "AND flag = 1",
                    where_del="WHERE fund_id LIKE 'JR%%'"
                )
                j.sync()

            else:
                j = splitter.Job(tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE)
                j.sync()

        except Exception as e:
            print(TASK_NO, e)


def main():
    start, end, timechunk = tools.parse_argv_time()
    if start is None or end is None:
        end = dt.datetime.now()
        start = end - relativedelta(hours=4, minutes=5)
    if not timechunk:
        timechunk = (0, 1, 0, 0)

    chunks = tools.generate_task(start, end, *timechunk)

    for t0, t1 in chunks:
        print(t0, t1)
        sync(t0, t1)


if __name__ == "__main__":
    print(ENGINE_RD)
    print(ENGINE_4119PRODUCT)
    main()
