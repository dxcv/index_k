from utils.database import config as cfg, io
from utils.script import swanv
from utils.script.scriptutils import tic

engines = cfg.load_engine()
engine_rd = engines["2Gb"]
engine_wt = engines["2Gb"]


def main():
    conn = engine_wt.connect()
    sql_check = "UPDATE fund_nv_data_standard SET swanav = NULL WHERE nav <> added_nav AND swanav = nav"
    conn.execute(sql_check)

    sql_update = "UPDATE fund_nv_data_standard SET swanav = nav WHERE nav = added_nav"
    conn.execute(sql_update)
    conn.close()

    df = swanv.calculate_swanav()
    if len(df) > 0:
        df.index = range(len(df))
        print("{num} records to update".format(num=len(df)))
        io.to_sql("fund_nv_data_standard", engine_wt, df, "update")


if __name__ == "__main__":
    tic("swanav...")
    main()
    tic("Done...")
