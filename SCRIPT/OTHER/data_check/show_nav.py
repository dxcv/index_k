import numpy as np
import pandas as pd
import os
from sqlalchemy import create_engine


engine_base = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),
    connect_args={"charset": "utf8"}, echo=False, )
PATH = None or os.path.join(os.path.expanduser("~"), 'Desktop', "DIFF_NV")


def _muti_merge(df_list, iteration):
    """
    递归地将一列dataframe进行合并
    :param df_list: dataframe列表
    :param iteration: 迭代深度
    :return:
    how = inner outer left right
    合并后的dataframe
    """
    lenth = len(df_list)
    if lenth == 2:
        df_list = pd.merge(df_list[0], df_list[1], how='outer', on='statistic_date')
        return df_list
    elif lenth == 1:
        return df_list[0]
    elif lenth > 2:
        nt = round(lenth / 2)
        iteration += 1
        df_list = pd.merge(_muti_merge(df_list[:nt], iteration), _muti_merge(df_list[nt:], iteration),  how='outer',on = 'statistic_date')
        return df_list


def to_list(df):
    a = np.array(df)
    vv = a.tolist()
    return vv


def show_nv(fund_id):
    sql1 = "SELECT statistic_date,nav,added_nav,source_id,is_used FROM \
     fund_nv_data_source_copy2 WHERE fund_id ='{fid}'and source_id not IN ('03','04','05')".format(fid=fund_id)
    sql2 = "SELECT source_id FROM config_private.sync_source WHERE pk = '{fid}' and is_used=1".format(fid=fund_id)
    sql3 = "SELECT statistic_date,nav as standard_nav,added_nav as standard_added FROM base.fund_nv_data_standard WHERE " \
           "fund_id = '{fid}'".format(fid=fund_id)
    df = pd.read_sql(sql1, engine_base)
    df2 = pd.read_sql(sql2, engine_base)
    df3 = pd.read_sql(sql3, engine_base)
    sync = to_list(df2["source_id"])
    data = df["source_id"]
    col = data.drop_duplicates()
    source = to_list(col)
    dff = []
    for i in source:
        df1 = df[df["source_id"] == i]
        dff.append(df1)
    df_nv = _muti_merge(dff, 1)
    column = to_list(df_nv.columns)
    los = []
    for i in range(len(column)):
        los.append(i)
    for i in range(len(source)):
        num = i*4+1
        num2 = i*4+2
        num3 = i*4+3
        los.remove(num3)
        col_t = source[i]
        column[num] = col_t
        column[num2] = col_t
        if col_t in sync:
            column[num+3] = 1
        else:
            column[num+3] = 0
    df_nv.columns = column
    dataframe = df_nv.iloc[:, los]
    dataframe2 = pd.merge(dataframe, df3, how='outer', on='statistic_date')
    DA3 = dataframe2.sort_values(by='statistic_date', ascending=False)
    if '000001' in DA3.columns:
        return DA3
    else:
        DA3["y_nav"] = None
        DA3["y_added_nav"] = None
        DA3["y_sync"] = None
        return DA3


def get_one(inp):
    inp = str(inp)
    if len(inp) != 6:
        inp = inp.zfill(6)

    if inp[:2] != "JR":
        inp = "JR" + inp
    try:
        df = show_nv(inp)
    except AttributeError:
        print(inp, "可能没有数据")

    if not os.path.isdir(PATH):
        os.mkdir(PATH)
    fp = os.path.join(PATH, inp + ".csv")
    try:
        df.to_csv(fp, index=False, )
        print("Save to ", fp)
    except PermissionError as e:
        print(fp, "正在被使用, 请关闭文件")


def main():
    while True:
        inp = str(input("Input a fund_id, or Q to exit"))
        if inp.lower() == "q":
            input("Press any key to exit.")
            break
        if inp.lower() == "c":
            try:
                ids = pd.read_clipboard(header=None)[0].tolist()
                for id in ids:
                    get_one(id)
            except:
                continue
        else:
            try:
                get_one(inp)
            except:
                continue


if __name__ == '__main__':
    main()
