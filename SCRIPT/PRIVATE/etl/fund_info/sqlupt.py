from utils.database import config as cfg, cronsql

ENGINE_RD = cfg.load_engine()["2Gb"]


def main():
    # 更新投资范围
    table = cronsql.UptSQL.Private.FundInfo

    # 需要保持执行顺序的sql
    tasks_with_seq = [table.REG_PRI, table.REG_ACC, table.REG_FUT, table.REG_SEC]

    # 不需要保持执行顺序的sql
    tasks_without_seq = [table.IS_DEPOSIT, table.IS_ABNORMAL_LIQUIDATION, table.FUND_STATUS, table.IS_UMBRELLA]
    tasks = [*tasks_with_seq, *tasks_without_seq]
    for sql in tasks:
        try:
            ENGINE_RD.execute(sql)
        except Exception as e:
            continue

if __name__ == "__main__":
    main()
