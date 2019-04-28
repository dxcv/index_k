import pandas as pd
from utils.database import io, config as cfg
from utils.database import sqlfactory as sf


ENGINE_RD = cfg.load_engine()["2Gb"]


def filter_hyphen():
    cols_with_hyphen = ["fund_manager", "open_date", "fund_stockbroker", "fee_subscription", "fee_redeem",
                        "fee_manage", "reg_time", "fund_time_limit", "fee_trust", "fee_pay", "fund_member"]
    df = fetch_fund_info(cols_with_hyphen)
    df = df.applymap(lambda x: str.strip(x) if type(x) is str else x)
    for col in cols_with_hyphen:
        print(col)
        df.loc[(df[col] == " ") | (df[col] == "") | (df[col] == "-") | (df[col] == "--") | (df[col] == "---"), col] = None
    return df


def fetch_fund_info(ls):
    cols = sf.SQL.values4sql(ls, usage="column")
    sql = "SELECT {cols} FROM fund_info".format(cols=cols)
    df = pd.read_sql(sql, ENGINE_RD)
    return df


def main():
    df = filter_hyphen()
    io.to_sql("fund_info", ENGINE_RD, df)


if __name__ == "__main__":
    main()