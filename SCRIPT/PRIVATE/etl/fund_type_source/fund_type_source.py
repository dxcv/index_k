from utils.database import config as cfg, io
from SCRIPT.PRIVATE.etl.fund_type_source.fund_type_source_01 import Streams as s01
from SCRIPT.PRIVATE.etl.fund_type_source.fund_type_source_02 import Streams as s02

ENGINE_RD = cfg.load_engine()["2Gb"]


def main():
    for conflu in (s01.conflu_ts104(), s01.conflu_ts103(), s01.conflu_ts102()):
        io.to_sql("base.fund_type_source", ENGINE_RD, conflu.dataframe)

    for conflu in (s02.conflu_ts20X(), s02.conflu_ts30X(), s02.conflu_ts40X(), s02.conflu_ts501()):
        io.to_sql("base.fund_type_source", ENGINE_RD, conflu.dataframe)


if __name__ == "__main__":
    main()
