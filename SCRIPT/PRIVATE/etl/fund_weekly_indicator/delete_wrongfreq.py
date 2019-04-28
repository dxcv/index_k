import pandas as pd
from utils.database import io, config as cfg
from utils.database import sqlfactory as sf


def delete_wrongfreq(ids, conn):
    fid = sf.SQL.values4sql(ids)

    for table in {"fund_weekly_return", "fund_weekly_risk", "fund_subsidiary_weekly_index",
                  "fund_weekly_risk2", "fund_subsidiary_weekly_index2", "fund_subsidiary_weekly_index3",
                  "fund_weekly_indicator"}:
        sql = "SELECT COUNT(1) FROM {tb} \
        WHERE fund_id IN {fid}".format(fid=fid, tb=table)

        wrong_num = conn.execute(sql).fetchone()[0]
        print(table, wrong_num)
        if wrong_num > 0:
            sql_del = "DELETE FROM {tb} WHERE fund_id IN {fid}".format(fid=fid, tb=table)
            print("DELETING WRONG DATA FROM {tb}...".format(tb=table))
            conn.execute(sql_del)
            print("DONE.")


def get_wrongid(conn):
    sql = "SELECT fund_id as fund_id FROM fund_info WHERE data_freq = '月度'"
    ids = pd.read_sql(sql, conn)["fund_id"].tolist()
    return ids


def main():
    engine = cfg.load_engine()["2Gb"]
    with engine.connect() as conn:
        fid = get_wrongid(conn)
        delete_wrongfreq(fid, conn)
        conn.close()

if __name__ == "__main__":
    main()
