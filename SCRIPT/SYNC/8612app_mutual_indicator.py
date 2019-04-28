import sys
import os
try:
    sys.path.append(os.getcwd()[:os.getcwd().index("SCRIPT")])
except:
    pass

import datetime as dt
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from utils.database import config as cfg
from utils.synckit.cli import tools
from utils.synckit.mysqlreader import splitter


ENGINE_RD = create_engine(
    "mysql+pymysql://jr_sync_yu:jr_sync_yu@182.254.128.241:4171/base_public", connect_args={"charset": "utf8"})
ENGINE_8612APP_MUTUAL = cfg.load_engine()["8612app_mutual"]

tasks = [
    (splitter.MysqlReader("fund_dividend", ENGINE_RD), splitter.MysqlReader("fund_dividend", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_split", ENGINE_RD), splitter.MysqlReader("fund_split", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_dividend_split", ENGINE_RD), splitter.MysqlReader("fund_dividend_split", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_nv", ENGINE_RD), splitter.MysqlReader("fund_nv", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_yield", ENGINE_RD), splitter.MysqlReader("fund_yield", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_nv_source", ENGINE_RD), splitter.MysqlReader("fund_nv_source", ENGINE_8612APP_MUTUAL)),

    (splitter.MysqlReader("fund_daily_return", ENGINE_RD), splitter.MysqlReader("fund_daily_return", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_daily_risk", ENGINE_RD), splitter.MysqlReader("fund_daily_risk", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_daily_risk2", ENGINE_RD), splitter.MysqlReader("fund_daily_risk2", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_daily_subsidiary", ENGINE_RD), splitter.MysqlReader("fund_daily_subsidiary", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_daily_subsidiary2", ENGINE_RD), splitter.MysqlReader("fund_daily_subsidiary2", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_daily_subsidiary3", ENGINE_RD), splitter.MysqlReader("fund_daily_subsidiary3", ENGINE_8612APP_MUTUAL)),

    (splitter.MysqlReader("fund_weekly_return", ENGINE_RD), splitter.MysqlReader("fund_weekly_return", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_weekly_risk", ENGINE_RD), splitter.MysqlReader("fund_weekly_risk", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_weekly_risk2", ENGINE_RD), splitter.MysqlReader("fund_weekly_risk2", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_weekly_subsidiary", ENGINE_RD), splitter.MysqlReader("fund_weekly_subsidiary", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_weekly_subsidiary2", ENGINE_RD), splitter.MysqlReader("fund_weekly_subsidiary2", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_weekly_subsidiary3", ENGINE_RD), splitter.MysqlReader("fund_weekly_subsidiary3", ENGINE_8612APP_MUTUAL)),

    (splitter.MysqlReader("fund_monthly_return", ENGINE_RD), splitter.MysqlReader("fund_monthly_return", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_monthly_risk", ENGINE_RD), splitter.MysqlReader("fund_monthly_risk", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_monthly_risk2", ENGINE_RD), splitter.MysqlReader("fund_monthly_risk2", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_monthly_subsidiary", ENGINE_RD), splitter.MysqlReader("fund_monthly_subsidiary", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_monthly_subsidiary2", ENGINE_RD), splitter.MysqlReader("fund_monthly_subsidiary2", ENGINE_8612APP_MUTUAL)),
    (splitter.MysqlReader("fund_monthly_subsidiary3", ENGINE_RD), splitter.MysqlReader("fund_monthly_subsidiary3", ENGINE_8612APP_MUTUAL)),

    (splitter.MysqlReader("fund_announcement", ENGINE_RD), splitter.MysqlReader("fund_announcement", ENGINE_8612APP_MUTUAL)),
]


def sync(t0, t1):
    WHERE = "WHERE update_time BETWEEN '{t0}' AND '{t1}'".format(
        t0=t0.strftime("%Y%m%d%H%M%S"),
        t1=t1.strftime("%Y%m%d%H%M%S")
    )

    for TASK_NO in range(0, len(tasks)):
        print("task_no: {no}; task: {src} --> {tgt}".format(no=TASK_NO, src=tasks[TASK_NO][0]._name,
                                                            tgt=tasks[TASK_NO][1]._name))
        try:
            j = splitter.Job(tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE)
            if tasks[TASK_NO][0]._name == "fund_announcement":
                j.update(chunksize=10)
            elif tasks[TASK_NO][0]._name in {"fund_nv", "fund_yield", "fund_dividend_split"}:
                j.sync()
            else:
                j.update()

        except Exception as e:
            print(TASK_NO, e)


def main():
    start, end, timechunk = tools.parse_argv_time()
    if start is None or end is None:
        end = dt.datetime.now()
        start = end - relativedelta(hours=1, minutes=5)
    if not timechunk:
        timechunk = (0, 1, 0, 0)

    chunks = tools.generate_task(start, end, *timechunk)

    for t0, t1 in chunks:
        print(t0, t1)
        sync(t0, t1)


if __name__ == "__main__":
    main()
