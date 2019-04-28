import pandas as pd
from numpy import isnan
from utils.database import config as cfg, io

engine = cfg.load_engine()["2Gbp"]

# SETTING HERE #
file = ""


def parse_xl(fp):
    df = pd.read_excel(fp)

    df["data_source"] = "000001"
    df["fund_id"] = df["fund_id"].apply(lambda x: str(x).zfill(6))

    df["stype_code"] = df["stype_code"].apply(lambda x: str(int(x)).zfill(6) if ~isnan(x) else x)

    assert ~isnan(df["type_code"]).all()
    df["type_code"] = df["type_code"].apply(lambda x: str(int(x)).zfill(4))

    assert ~isnan(df["typestandard_code"]).all()
    df["typestandard_code"] = df["typestandard_code"].apply(lambda x: str(int(x)).zfill(2))


    cols = [
        "fund_id", "fund_name", "data_source",
        "typestandard_code", "typestandard_name", "type_code",
        "type_name", "stype_code", "stype_name"]
    return df[cols]


def main():
    df = parse_xl(file)
    io.to_sql("base_public.fund_type_mapping_source", engine, df)


if __name__ == "__main__":
    main()
