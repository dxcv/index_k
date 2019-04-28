from utils.database import config as cfg, io
from utils.etlkit.reader.mysqlreader import MysqlInput
from utils.etlkit.core import base, transform
from utils.database.models.base_private import OrgTimeseries
import datetime as dt

ENGINE = cfg.load_engine()["2Gb"]


def stream_x_org_info():
    sql = "SELECT xoi.org_id, xoi.final_report_time, real_capital, reg_capital, employee_scale FROM crawl_private.x_org_info xoi " \
          "JOIN (SELECT org_id, MAX(version) latest_ver FROM crawl_private.x_org_info WHERE is_used = 1 GROUP BY org_id) tb " \
          "ON xoi.org_id = tb.org_id AND xoi.version = tb.latest_ver"

    inp = MysqlInput(ENGINE, sql)

    vm = transform.ValueMap(
        {
            "employee_scale": lambda x: int(x.replace(",", "")),
            OrgTimeseries.data_time.name: dt.date.today()
        }

    )

    sk = transform.MapSelectKeys(
        {
            "org_id": OrgTimeseries.org_id.name,
            "final_report_time": OrgTimeseries.statistic_date.name,
            "real_capital": OrgTimeseries.real_capital.name,
            "reg_capital": OrgTimeseries.reg_capital.name,
            "employee_scale": OrgTimeseries.employee_scale.name,
            OrgTimeseries.data_time.name: None
        }
    )

    s = base.Stream(inp, transform=(vm, sk))
    return s


def main():
    s11 = stream_x_org_info()

    c = base.Confluence(s11)

    io.to_sql(OrgTimeseries.__tablename__, ENGINE, c.dataframe)


if __name__ == "__main__":
    main()
