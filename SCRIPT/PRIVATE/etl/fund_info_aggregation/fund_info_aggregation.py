import numpy as np
import pandas as pd
from utils.database import config as cfg
from utils.script import scriptutils as su

engines = cfg.load_engine()
engine_read = engines["2Gb"]
engine_write = engines["2Gb"]

conn_read = engine_read.connect()
conn_write = engine_write.connect()


def get_dict_value(k, dictionary, i=None):
    try:
        if i is not None:
            return dictionary[k][i]
        else:
            return dictionary[k]
    except:
        return np.nan


def refresh(data, conn):
    conn.execute("DELETE FROM fund_info_aggregation")
    data.to_sql("fund_info_aggregation", conn, if_exists="append", index=False, chunksize=10000)


def main():
    su.tic("getting original get_data...")
    sql_fi = "SELECT fund_id, fund_name, fund_name_py, fund_full_name, reg_time,  foundation_date, fund_status, region, data_freq, open_date, fund_custodian, \
    fund_time_limit, fund_stockbroker, fee_subscription, fee_redeem, fee_trust, fee_manage, fee_pay \
    FROM fund_info"
    d_fi = pd.read_sql(sql_fi, conn_read)

    sql_oi = "SELECT org_id, org_name, org_full_name FROM org_info"
    d_oi = pd.read_sql(sql_oi, conn_read)

    sql_fom = "SELECT fund_id, org_id FROM fund_org_mapping WHERE org_type_code = 1"
    d_fom = pd.read_sql(sql_fom, conn_read)

    sql_ftm = "SELECT fund_id, typestandard_code, typestandard_name, type_code, type_name, stype_code, stype_name FROM fund_type_mapping WHERE typestandard_code IN (601, 602, 603, 604) AND flag=1"
    d_ftm = pd.read_sql(sql_ftm, conn_read)

    sql_ti = "SELECT id as fund_id, foundation_days_range FROM time_index WHERE id_category = 1"
    d_ti = pd.read_sql(sql_ti, conn_read)

    sql_fnd = "SELECT fnd1.fund_id, fnd2.nav, fnd2.added_nav, fnd2.swanav, fnd2.statistic_date \
    FROM (SELECT fund_id, MAX(statistic_date) as md FROM fund_nv_data_standard GROUP BY fund_id) fnd1 \
    JOIN fund_nv_data_standard fnd2 ON fnd1.fund_id = fnd2.fund_id AND fnd1.md = fnd2.statistic_date"
    d_fnd = pd.read_sql(sql_fnd, conn_read)

    su.tic("merging get_data...")
    tm = {}
    k_vs = [("typestandard_code", "typestandard_name"), ("type_code", "type_name"), ("stype_code", "stype_name")]
    for k_v in k_vs:
        tmp = d_ftm.loc[:, k_v].sort_values(by=k_v[0], ascending=[True]).drop_duplicates().dropna()
        tmp = dict(zip(tmp[k_v[0]], tmp[k_v[1]]))
        tm.update(tmp)
    tm.update({np.nan: None})

    ftm = dict(
        zip(
            zip(d_ftm["fund_id"], d_ftm["typestandard_code"]),
            zip(d_ftm["type_code"], d_ftm["stype_code"])
        )
    )

    d = d_fi.copy()
    d = d.merge(d_fom, how="left", on="fund_id")
    d = d.merge(d_oi, how="left", on="org_id")
    d = d.merge(d_ti, how="left", on="fund_id")
    d = d.merge(d_fnd, how="left", on="fund_id")

    _trans = {1: 1, 2: 4, 3: 2, 4: 3}

    for tsc in range(1, 5):
        tmp = d_ftm.loc[d_ftm["typestandard_code"] == 600 + tsc, ["fund_id", "typestandard_name"]]
        d = d.merge(tmp, how="left", on="fund_id")
    d.columns = list(d.columns[:-4]) + ["typestandard_name_{tsc}".format(tsc=_trans[tsc]) for tsc in range(1, 5)]

    for tsc in range(1, 5):
        d["type_code_{tsc}".format(tsc=_trans[tsc])] = d["fund_id"].apply(
            lambda fid: get_dict_value((fid, 600 + tsc), ftm, 0))
        d["type_code_name_{tsc}".format(tsc=_trans[tsc])] = d["type_code_{tsc}".format(tsc=_trans[tsc])].apply(
            lambda tc: get_dict_value(tc, tm))
        d["stype_code_{tsc}".format(tsc=_trans[tsc])] = d["fund_id"].apply(
            lambda fid: get_dict_value((fid, 600 + tsc), ftm, 1))
        d["stype_code_name_{tsc}".format(tsc=_trans[tsc])] = d["stype_code_{tsc}".format(tsc=_trans[tsc])].apply(
            lambda tc: get_dict_value(tc, tm))
    d = d.drop_duplicates(subset=["fund_id"])
    d.index = d["fund_id"]

    # 将fund_type_mapping表中typestandard_code = 601的所有
    # 和typestandard_code = 605中的type_code = 60502（股权投资基金），60503（创业投资基金），60504（其他投资基金），
    # 写入的fund_info_aggregation表的type_code_1，type_code_name_1中
    sql_ts5 = "SELECT fund_id, type_code as type_code_1, type_name as type_code_name_1 FROM fund_type_mapping \
               WHERE type_code IN (60502, 60503, 60504) AND flag = 1"
    df_ts5 = pd.read_sql(sql_ts5, engine_read).drop_duplicates(subset=["fund_id", "type_code_1"])
    df_ts5.index = df_ts5["fund_id"]

    d = d.fillna(df_ts5)
    refresh(d, conn_write)
    # return d

if __name__ == "__main__":
    su.tic("fund_info_aggregation...")
    main()
    su.tic("Done...")
