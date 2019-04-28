from utils.database import io, config as cfg
from SCRIPT.MUTUAL.etl.fund_type_mapping_source import (
    fund_type_mapping_source_tsc2 as d2, fund_type_mapping_source_tsc4 as d4
)


ENGINE = cfg.load_engine()["2Gbp"]


def main():
    for res in (d2, d4):
        df_result = res.confluence_merged().dataframe
        io.to_sql("fund_type_mapping", ENGINE, df_result)


if __name__ == "__main__":
    main()
