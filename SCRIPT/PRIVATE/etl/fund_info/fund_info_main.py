from utils.database import config as cfg
from SCRIPT.PRIVATE.etl.fund_info import fund_info, fund_manager, fund_custodian, init_nav, region, init_total_asset


ENGINE_RD = cfg.load_engine()["2Gb"]


def main():
    fund_info.main()
    fund_custodian.main()
    fund_manager.main()
    init_nav.main()
    region.main()
    init_total_asset.main()


if __name__ == "__main__":
    main()
