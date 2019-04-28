from utils.database import config as cfg, io
from utils.etlkit.reader.mysqlreader import MysqlInput
from utils.etlkit.core import base, transform
from utils.database.models.base_private import OrgAssetScale
import datetime as dt

ENGINE = cfg.load_engine()["2Gb"]


def stream_x_org_info():
    sql = "SELECT xoi.org_id, xoi.final_report_time, fund_num, fund_scale  FROM crawl_private.x_org_info xoi " \
          "JOIN (SELECT org_id, MAX(version) latest_ver FROM crawl_private.x_org_info WHERE is_used = 1 GROUP BY org_id) tb " \
          "ON xoi.org_id = tb.org_id AND xoi.version = tb.latest_ver"

    inp = MysqlInput(ENGINE, sql)

    vm = transform.ValueMap(
        {
            OrgAssetScale.data_time.name: dt.date.today()
        }

    )

    sk = transform.MapSelectKeys(
        {
            "org_id": OrgAssetScale.org_id.name,
            "final_report_time": OrgAssetScale.statistic_date.name,
            "fund_num": OrgAssetScale.funds_num.name,
            "fund_scale": OrgAssetScale.asset_scale.name,
            OrgAssetScale.data_time.name: None
        }
    )

    dn = transform.Dropna(subset=[OrgAssetScale.asset_scale.name, OrgAssetScale.funds_num.name, OrgAssetScale.statistic_date.name], how="all")

    s = base.Stream(inp, transform=(vm, sk, dn))
    return s


def main():
    s11 = stream_x_org_info()

    c = base.Confluence(s11)

    io.to_sql(OrgAssetScale.__tablename__, ENGINE, c.dataframe)


if __name__ == "__main__":
    main()
