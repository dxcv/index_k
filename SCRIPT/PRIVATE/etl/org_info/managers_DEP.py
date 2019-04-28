import pandas as pd
from utils.database import config as cfg, io

ENGINE = cfg.load_engine()["2Gb"]


def main():
    sql_managers = "SELECT mi.org_id, GROUP_CONCAT(user_name SEPARATOR ',') as managers \
    FROM `manager_info` mi \
    JOIN `org_info` oi ON oi.org_id = mi.org_id \
    WHERE is_current = 1 \
    GROUP BY mi.org_id"

    df_managers = pd.read_sql(sql_managers, ENGINE)
    return df_managers

if __name__ == "__main__":
    result = main()
    io.to_sql("org_info", ENGINE, result)
