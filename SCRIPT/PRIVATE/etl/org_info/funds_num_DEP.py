import pandas as pd
from utils.database import config as cfg, io

ENGINE = cfg.load_engine()["2Gb"]


def main():

    sql_operating = "\
      SELECT org_id, COUNT(fom.fund_id) as fund_num FROM fund_org_mapping fom \
      JOIN fund_info fi ON fom.fund_id = fi.fund_id \
      WHERE fom.org_type_code = 1 AND fi.fund_status = '运行中' \
      GROUP BY fom.org_id"

    sql_total = "\
      SELECT org_id, COUNT(fom.fund_id) as total_fund_num FROM fund_org_mapping fom \
      JOIN fund_info fi ON fom.fund_id = fi.fund_id \
      WHERE fom.org_type_code = 1 \
      GROUP BY fom.org_id"

    df_operating = pd.read_sql(sql_operating, ENGINE)
    df_total = pd.read_sql(sql_total, ENGINE)

    result = df_operating.merge(df_total, how="outer", on="org_id")
    return result

if __name__ == "__main__":
    result = main()
    io.to_sql("org_info", ENGINE, result)
