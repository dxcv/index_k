from utils.database import config as cfg, cronsql

ENGINE_RD = cfg.load_engine()["2Gb"]


def main():
    # 更新投资范围
    table = cronsql.UptSQL.Private.FundInfoSubsidiary

    # 需要保持执行顺序
    tasks_with_sql = [table.INVRNG_PRI, table.INVRNG_ACC, table.INVRNG_FUT, table.INVRNG_SEC]
    for sql in tasks_with_sql:
        try:
            ENGINE_RD.execute(sql)
        except:
            continue

if __name__ == "__main__":
    main()
