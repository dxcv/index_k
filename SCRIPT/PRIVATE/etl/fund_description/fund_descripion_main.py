from utils.database import config as cfg
from SCRIPT.PRIVATE.etl.fund_description import investment_range, fund_info_subsidiary, fund_description


ENGINE_RD = cfg.load_engine()["2Gb"]


def main():
    investment_range.main()
    fund_info_subsidiary.main()
    fund_description.main()


if __name__ == "__main__":
    main()
