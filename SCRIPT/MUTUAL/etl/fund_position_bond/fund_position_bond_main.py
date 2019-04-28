from SCRIPT.MUTUAL.etl.fund_position_bond import position, quantity


def main():
    position.MainStream.clean()
    quantity.MainStream.clean()


def test():
    import os
    from utils.etlkit.ext import tools
    from utils.database import config as cfg
    ENGINER = cfg.load_engine()["2Gbp"]

    tc = tools.TableComparer(
        "base_test.fund_position_bond_test_20180515", "base_public.fund_position_bond",
        ENGINER,
        cols_included={"quantity", "scale", "asset_scale", "proportion_net"},
    )
    res = tc.result
    res.to_csv(os.path.expanduser("~/Desktop/fund_position_bond.csv"), encoding="gbk")


if __name__ == "__main__":
    main()
