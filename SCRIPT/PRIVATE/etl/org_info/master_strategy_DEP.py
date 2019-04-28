import pandas as pd
from utils.database import config as cfg, io

ENGINE = cfg.load_engine()["2Gb"]


def main():
    sql_type_num = "\
    SELECT org_id, ftm.stype_code, ftm.stype_name as type_name, COUNT(fom.fund_id) as fund_num FROM fund_org_mapping fom \
    JOIN fund_info fi ON fom.fund_id = fi.fund_id \
    JOIN fund_type_mapping ftm ON ftm.flag = 1 AND ftm.typestandard_code = 601 AND fom.fund_id = ftm.fund_id AND stype_code <> '6010901' \
    WHERE org_type_code = 1 \
    GROUP BY fom.org_id, ftm.type_code"

    df_type = pd.read_sql(sql_type_num, ENGINE).sort_values(["org_id", "fund_num"], ascending=[True, False])
    result = df_type.drop_duplicates("org_id")[["org_id", "type_name"]]
    result.columns = ["org_id", "master_strategy"]
    return result

if __name__ == "__main__":
    result = main()
    io.to_sql("org_info", ENGINE, result)
