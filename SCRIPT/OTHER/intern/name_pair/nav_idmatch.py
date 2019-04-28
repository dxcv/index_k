import pandas as pd
from sqlalchemy import create_engine
import jieba
import numpy as np
import pickle
import xlwt
import re
import math
import os
from multiprocessing.dummy import Pool as ThreadPool


def _init_jieba():
    """
    以脚本目录下的self_dic.txt作为语料库，初始化jieba模块

    Args:
    None

    Returns:
    None

    """
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    dict_file = os.path.join(script_dir, 'self_dic.txt')
    jieba.load_userdict(dict_file)


def _init_fund_info():
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/crawl_private?charset=utf8')
    d_fund_info = pd.read_sql_query("select DISTINCT fund_id,source_id ,fund_name,fund_full_name from d_fund_info",
                                    engine)
    print('d_fund_info')
    s_fund_info = pd.read_sql_query("select DISTINCT fund_id,source_id, fund_name from s_fund_nv", engine)
    print('s_fund_info')
    t_fund_info = pd.read_sql_query("select DISTINCT fund_id,source_id, fund_name from t_fund_nv", engine)
    print('t_fund_info')
    g_fund_info = pd.read_sql_query("select DISTINCT fund_id,source_id, fund_name from g_fund_nv", engine)
    print('g_fund_info')
    return d_fund_info, s_fund_info, t_fund_info, g_fund_info


def _init_fund_match_id():
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
    sql_com = "SELECT DISTINCT matched_id, source, source_id FROM  id_match WHERE is_used=1  AND source IN (SELECT DISTINCT source  FROM  id_match WHERE (source LIKE '02%%' OR source LIKE '03%%' OR source LIKE '04%%' OR source LIKE '05%%') AND source <> '02' AND source <> '03' AND source <> '04' AND source <> '05')"
    matched_id = pd.read_sql_query(sql_com, engine)
    print('matched_id')
    return matched_id


def _source_df(source):
    pattern_02 = re.compile(r'02\d+')
    pattern_03 = re.compile(r'03\d+')
    pattern_04 = re.compile(r'04\d+')
    pattern_05 = re.compile(r'05\d+')
    if re.match(pattern_02, source):
        return 'd_fund_nv'
    elif re.match(pattern_03, source):
        return 's_fund_nv'
    elif re.match(pattern_04, source):
        return 't_fund_nv'
    elif re.match(pattern_05, source):
        return 'g_fund_nv'
    else:
        print('Not matched!')
        return np.nan


def _source_info_df(source):
    pattern_02 = re.compile(r'02\d+')
    pattern_03 = re.compile(r'03\d+')
    pattern_04 = re.compile(r'04\d+')
    pattern_05 = re.compile(r'05\d+')
    if re.match(pattern_02, source):
        return 0
    elif re.match(pattern_03, source):
        return 1
    elif re.match(pattern_04, source):
        return 2
    elif re.match(pattern_05, source):
        return 3
    else:
        print('Not matched!')
        return np.nan


# dstg

def _nav_match(fund_id1, fund_id2, source_1, engine=create_engine(
    'mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/crawl_private?charset=utf8')):
    """
    根据比较fund_id1,fund_id2同期的净值数据，判断是否相同

    Args:
    fund_id1 - str , fund_id2 - str , engine - create_engine.Engine

    Returns:
    is_match - bool

    """
    df1 = _source_df(source_1)
    sql_com = "SELECT b.statistic_date,  a.fund_id, b.fund_id,a.nav as nav1 ,b.nav as nav2 FROM ( SELECT  fund_id,   statistic_date,   nav FROM  " + df1 + " WHERE fund_id ='" + fund_id1 + "' and source_id='" + source_1 + "'  ORDER BY statistic_date DESC) as a ,  (SELECT  fund_id,   statistic_date,  nav FROM  base.fund_nv_data_standard WHERE  fund_id ='" + fund_id2 + "' ORDER BY statistic_date DESC) as b where a.statistic_date=b.statistic_date limit 30;"
    try:
        df = pd.read_sql_query(sql_com, engine)
    except:
        return False
    if df.empty:
        return 'Empty'  # 空表这次认为是真
    is_match = False
    num_data = len(df)
    if num_data == 1:
        return 'Empty'  # 只有一条数据也认为为空
    max_error = math.ceil(0.8 * num_data)
    error_time = 0
    df[['nav1', 'nav2']] = df[['nav1', 'nav2']].applymap(lambda x: x / 100 if x > 6 else x)
    try:
        for indexs in df.index:
            if abs((df.loc[indexs].values[3] - df.loc[indexs].values[4])) > 0.001:
                error_time += 1
    except:
        return 'Match_Error'
    if error_time <= max_error:
        is_match = True
    return is_match


def _get_all_source(matched_id):
    df = match_id[match_id.matched_id == matched_id]
    return df


def _get_mat_name(matched_id):
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
    sql_com = "select fund_id, fund_name from fund_info where fund_id='" + matched_id + "'"
    df = pd.read_sql_query(sql_com, engine)
    return df


def _get_source_name(source, source_id):
    from_1 = _source_info_df(source)
    info_df = info_list[from_1]
    df = info_df[(info_df.fund_id == source_id) & (info_df.source_id == source)]
    return df


def _muti_task(ii):
    print(ii)
    row_list = []
    matched_id = candidate.iloc[ii, 0]
    try:
        name_df = _get_mat_name(str(matched_id))
        match_name = name_df['fund_name'].tolist()[0]
        all_source = _get_all_source(matched_id)
    except:
        return [str(matched_id), 'match_id Error!']

    count_source = len(all_source)
    # row_list.append(str(count_source))
    candidate_source = []  # 备选源表
    candidate_source_id = []  # 备选源表

    for j in range(count_source):
        candidate_source.append(all_source.iloc[j, 1])  # 数值格式
        candidate_source_id.append(all_source.iloc[j, 2])
        # row_list.append(str(all_source.loc[j, 'source']))  # 字符串格式
        # row_list.append(str(all_source.loc[j, 'source_id']))  # 字符串格式
    # row_list.append('右方为净值匹配结果')
    is_first = True
    try:
        for i in range(count_source):
            is_match = _nav_match(candidate_source_id[i], matched_id, candidate_source[i])
            if isinstance(is_match, bool):
                if is_match == False:
                    source_name = _get_source_name(candidate_source[i], candidate_source_id[i])
                    if source_name['fund_name'].empty:
                        row_list.append('')
                    else:
                        if candidate_source_id[i] == '020002':
                            sc_name = source_name['fund_full_name'].tolist()[0]
                        else:
                            sc_name = source_name['fund_name'].tolist()[0]

                    aft_sc_name = ''.join([x for x in jieba.lcut(sc_name) if x not in tyc_word])
                    print(aft_sc_name)
                    print(match_name)
                    if aft_sc_name == match_name:
                        continue
                    if is_first == True:
                        row_list.append(str(matched_id))
                        row_list.append(match_name)
                        is_first = False
                    row_list.append(candidate_source[i])
                    row_list.append(candidate_source_id[i])
                    row_list.append(aft_sc_name)

    except:
        return [str(matched_id), 'nav_match Error!']

    return row_list


def nav_idmatch(input, start, end, is_load=False):
    global tyc_word, match_id, info_list, candidate
    candidate = input
    tyc_word = ['号', '计划', '资产', '基金', '投资', '管理', '私募', '合伙', '信托', '期', '集合', '证券', '专项', '有限', '号', '期', '类', '级',
                '份额']
    _init_jieba()
    if is_load:
        with open('all_data.pkl', 'rb') as f:
            info_list = pickle.load(f)
            match_id = pickle.load(f)
    else:
        d_fund_info, s_fund_info, t_fund_info, g_fund_info = _init_fund_info()
        info_list = [d_fund_info, s_fund_info, t_fund_info, g_fund_info]
        match_id = _init_fund_match_id()
    pool = ThreadPool(4)
    data = pool.map_async(_muti_task, range(start, end))
    pool.close()
    pool.join()
    all_list = data.get()
    all_list = [x for x in all_list if len(x) != 0]
    workbook = xlwt.Workbook()
    worksheet = workbook.add_sheet('My Sheet')
    for i in range(len(all_list)):
        for j in range(len(all_list[i])):
            worksheet.write(i, j, all_list[i][j])
    workbook.save('{}-{}-vs3.xls'.format(str(start), str(end)))


def init_candidate():
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
    sql_com = "SELECT \
        matched_id, \
        count(source) as num_source \
    FROM \
        ( \
            SELECT DISTINCT \
                matched_id, \
                source, \
                source_id \
            FROM \
                id_match \
            WHERE \
                is_used = 1 \
            And id_type = 1 \
            AND source IN ( \
                SELECT DISTINCT \
                    source \
                FROM \
                    id_match \
                WHERE \
                    ( \
                        source LIKE '02%%' \
                        OR source LIKE '03%%' \
                        OR source LIKE '04%%' \
                        OR source LIKE '05%%' \
                    ) \
                AND source <> '02' \
                AND source <> '03' \
                AND source <> '04' \
                AND source <> '05' \
            ) \
        ) as a \
    GROUP BY \
        a.matched_id \
    HAVING \
        count(source) > 1"
    df = pd.read_sql_query(sql_com, engine)
    print('candidate')
    return df


if __name__ == "__main__":
    candidate = init_candidate()
    nav_idmatch(candidate, 10000, 20000, is_load=False)
