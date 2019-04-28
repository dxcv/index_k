import re
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import Levenshtein as ld
import jieba
import time
from gensim.models import word2vec
import logging
import os
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
from collections import Counter
import pickle



def init_1():
    """
    初始化测试集的数据 (来源私募运通数据库 fund_info表)

    Args:
    None

    Returns:
    df-dataframe  ntest-int

    """
    sql_com = 'select bf_m.fund_id, bf_m.source_ID, cd_i.fund_name as raw_fund_name, bf_i.fund_name as match_fund_name from fund_id_match as bf_m, crawl_private.d_fund_info as cd_i,base.fund_info AS bf_i where bf_m.source_ID=cd_i.fund_id and bf_m.fund_id=bf_i.fund_id'
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
    df = pd.read_sql_query(sql_com, engine)
    ntest = len(df)
    print(ntest)
    return df, ntest


def init_1_f():
    """
    初始化测试集的数据

    Args:
    None

    Returns:
    df-dataframe  ntest-int

    """
    sql_com = 'select bf_m.fund_id, bf_m.source_ID, cd_i.fund_full_name as raw_fund_name, bf_i.fund_full_name as match_fund_name  from fund_id_match as bf_m, crawl_private.d_fund_info as cd_i,base.fund_info AS bf_i where bf_m.source_ID=cd_i.fund_id and bf_m.fund_id=bf_i.fund_id'
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
    df = pd.read_sql_query(sql_com, engine)
    ntest = len(df)
    print(ntest)
    return df, ntest


def init_2():
    """
    初始化比对数据， (来源私募运通数据库 fund_info表)

    Args:
    None

    Returns:
    df-dataframe  ntest-int

    """
    sql_com = "SELECT fund_name, fund_id from fund_info"
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
    df = pd.read_sql_query(sql_com, engine)
    nrow = len(df)
    print(nrow)
    return df, nrow


def init_2_f():
    """
    初始化比对数据 (来源私募运通数据库 fund_info表)

    Args:
    None

    Returns:
    df-dataframe  ntest-int

    """
    sql_com = "SELECT fund_full_name, fund_id from fund_info"
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
    df = pd.read_sql_query(sql_com, engine)
    nrow = len(df)
    print(nrow)
    return df, nrow


def init_2_f_aft(fund_name2, colname):
    """
    进一步初始化，将fund_name2中数据标准化（全角半角，过滤字符）

    Args:
    None

    Returns:
    fund_name2-dataframe nprow-int

    """
    fund_name2_aft = fund_name2
    fund_name2_aft.head()
    fund_name2_aft['new_col'] = fund_name2_aft.loc[:, colname].apply(strQ2B).apply(sub_chi2num).apply(col_chi).apply(
        list).apply(del_space).apply(set)
    return fund_name2_aft


def init_update():
    """
    从数据库中读取数据并额外更新本地缓存

    Args:
    None

    Returns:
    fund_name2-dataframe nprow-int
    fund_name-dataframe  ntest-int

    """
    [fund_name, ntest] = df_private()
    [fund_name2, nrow] = init_2_f()
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    fund_name_file = os.path.join(script_dir, 'local_data_3-20.pkl')
    f=open(fund_name_file, "wb")
    pickle.dump(fund_name, f,True)
    pickle.dump(fund_name2, f,True)
    f.close()
    return fund_name, fund_name2, ntest, nrow


def init_local():
    """
    使用存储在本地的缓存数据作为训练用数据

    Args:
    None

    Returns:
    fund_name2-dataframe nprow-int
    fund_name-dataframe  ntest-int

    """
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    fund_name_file = os.path.join(script_dir, 'local_data.pkl')
    f = open(fund_name_file, "rb")
    fund_name = pickle.load(f)
    fund_name2 = pickle.load(f)
    ntest = len(fund_name)
    nrow = len(fund_name2)
    print(ntest)
    print(nrow)
    f.close()
    return fund_name, fund_name2, ntest, nrow


def init_word2vec_model():
    """
    以脚本目录下的fen_full_name.txt作为语料库，训练skip-gram模型

    Args:
    None

    Returns:
    model - word2vec model

    """
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    dict_file = os.path.join(script_dir, 'fen_full_name.txt')
    logging.basicConfig(format='%(asctime)s:%(levelname)s: %(message)s', level=logging.INFO)
    sentences = word2vec.Text8Corpus(dict_file)  # 加载语料
    model = word2vec.Word2Vec(sentences, size=200, min_count=1)  # 训练skip-gram模型
    return model


def init_jieba():
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


def init_tyc(n=15):
    """
    根据语料库的高频词汇，初始化停用词名单，n为停用词个数

    Args:
    n-int default=15

    Returns:
    tyc_word_in - list

    """
    with open('fen_full_name.txt', 'r', encoding='utf-8') as f:
        txt = f.read()
        new_txt = txt.split()
        result = Counter(new_txt)
        raw_tyc = result.most_common(n)
        tyc_word_in = []
        for ii in raw_tyc:
            tyc_word_in.append(ii[0])
        tyc_word_in = [x for x in tyc_word_in if x not in white_word]
        tyc_word_in = tyc_word_in + ['号', '期', '类', '级', '份额']  # 如用于母子基金，这些是关键字
        print(tyc_word_in)
    return tyc_word_in


def strQ2B(ustring):
    """
    将全角字符转化为半角 适用py 3.x版本
    Args:
    ustring - str

   Returns:
    rstring - str
    """
    rstring = ""
    if not isinstance(ustring, str):
        ustring = str(ustring)
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 12288:  # 全角空格直接转换
            inside_code = 32
        elif 65281 <= inside_code <= 65374:  # 全角字符（除空格）根据关系转化
            inside_code -= 65248

        rstring += chr(inside_code)
    return rstring


def col_chi(userstr):
    """
    筛选字符中的中文字符，英文字符与阿拉伯数字 需要import re
    Args:
    userstr - str

    Returns:
    new_str - str
    """
    pattern = re.compile(r'[\u4e00-\u9fa5]|\d|[A-Z]|[a-z]')
    new_str = []
    for i in range(len(userstr)):
        if pattern.match(userstr[i]):
            new_str.append(userstr[i])
        else:
            new_str.append(' ')
    new_str = "".join(new_str)
    return new_str


def del_space(list_in):
    """
    将list中的空格元素删去
    Args:
    list_in - list

    Returns:
    list_out - list
    """
    if len(list_in) == 0:
        return list_in
    len_list = len(list_in)
    for i in range(len_list - 1, -1, -1):
        if list_in[i] == " ":
            del list_in[i]
    list_out = []
    list_out[:] = list_in
    return list_out


def chinese2digits(uchars_chinese):
    """
    将输入的中文数字转化为阿拉伯数字
    Args:
    userstr - str

    Returns:
    total - int
    """
    total = 0
    r = 1  # 表示单位：个十百千...
    is_find = 0  # 判断是否为标准输入
    common_used_numerals = {'零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
                            '十': 10, '百': 100, }  # 筛选数字字典
    for i in range(len(uchars_chinese) - 1, -1, -1):
        if common_used_numerals.get(uchars_chinese[i]) == '10':
            is_find = 1
            break
    if len(uchars_chinese) >= 2 and is_find == 0:  # 不是标准输入
        j = 1  # 十进制位数
        for i in range(len(uchars_chinese) - 1, -1, -1):
            val = common_used_numerals.get(uchars_chinese[i])
            total = total + j * val
            j = 10 * j
        return total

    for i in range(len(uchars_chinese) - 1, -1, -1):
        val = common_used_numerals.get(uchars_chinese[i])
        if val >= 10 and i == 0:  # 应对 十三 十四 十*之类
            if val > r:
                r = val
                total = total + val
            else:
                r = r * val
                # total =total + r * x
        elif val >= 10:
            if val > r:
                r = val
            else:
                r = r * val
        else:
            total = total + r * val
    return total


def sub_chi2num(userstr):
    """
    将输入的基金名称找出数字部分使用chines2digits函数将其替换并返回新的基金名称
    Args:
    userstr - str

    Returns:
    res_str - str
    """
    common_used_numerals = {'零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
                            '十': 10, '百': 100, }  # 筛选数字字典
    if len(userstr) == 0:
        return userstr
    is_find = 0  # 判断是否找到数字
    first = []  # 包含数字的字符首位
    last = []  # 包含数字的字符末尾
    str_list = list(userstr)
    get_num = []  # 包含转换后的数字列表
    nstr = len(userstr)
    for i in range(nstr):
        if is_find == 0:
            if common_used_numerals.get(userstr[i]):
                is_find = 1
                first.append(i)
        else:
            if not common_used_numerals.get(userstr[i]):
                is_find = 0
                last.append(i - 1)
        if i == len(userstr) - 1 and is_find == 1:  # 循环即将结束后还未找到非数字，则必然是该中文数字的最后一位
            last.append(i)
    nfirst = len(first)
    for i in range(nfirst):
        if last[i] <= nstr:
            get_num.append(chinese2digits(userstr[first[i]:last[i] + 1]))
        else:
            get_num.append(chinese2digits(userstr[first[i]:]))
    for i in range(nfirst - 1, -1, -1):
        for j in range(last[i], first[i] - 1, -1):
            del str_list[j]
        str_list.insert(first[i], str(get_num[i]))
    res_str = "".join(str_list)
    return res_str


def raw_list(fund_name, fund_name2_aft, col_name, range, primarykey):
    """
    从fund_name2_aft的col_name列中range范围查询可能匹配为fund_name的基金名称，并返回可能的列表
    Args:
    fund_name - str/series,fund_name2_aft - dataframe , col_name - str , range - range

    Returns:
    fund_found_list - list

    """

    if primarykey:
        fund_name = fund_name.tolist()
    else:
        fund_name = [fund_name, '']
    match1 = set(del_space(list(sub_chi2num(col_chi(strQ2B(fund_name[0]))))))
    fund_found_list = []
    fund_name2_set = fund_name2_aft['new_col'].tolist()
    fund_name2_fullname = fund_name2_aft[col_name].tolist()
    if primarykey:
        fund_name2_primary_key = fund_name2_aft[primarykey].tolist()
    global gg
    gg += 1
    print(gg)

    for j in range:
        match2 = fund_name2_set[j]
        # (len(match1^match2)-max(len(match1-match2),len(match2-match1)))==0  慢速可控准则
        if len(match1) != 0 and len(match2) != 0 and (match1.issubset(match2) or match2.issubset(match1)):  # 快速准则
            if primarykey:
                fund_found_list.append([fund_name, fund_name2_fullname[j], fund_name2_primary_key[j]])
            else:
                fund_found_list.append([fund_name[0], fund_name2_fullname[j]])
    if not fund_found_list:
        return np.nan
    return fund_found_list


def choose_best(fund_found_list, model):
    """
    根据边际相对距离，输入可能匹配列表，返回最高概率匹配基金名称
    Args:
    fund_found_list - list, model - gensim.models.word2vec.Word2Vec

    Returns:
    min_name - str

    """
    global simmi
    if isinstance(fund_found_list, float):
        return np.nan
    nlist = len(fund_found_list)
    min_dis = 10000  # 距离肯定比这个小
    min_ld = 10000  # 距离肯定比这个小
    min_index = -1
    fund_name = fund_found_list[0][0]
    if isinstance(fund_name, str):
        fund_name_t = fund_name
    else:
        fund_name_t = fund_name[0]
    t1=sub_chi2num(col_chi(strQ2B(fund_name_t)))
    t1=''.join([x for x in jieba.lcut(t1) if x not in tyc_word])
    for i in range(nlist):
        temp = simi_vec(fund_name_t, fund_found_list[i][1], model)
        t2 = sub_chi2num(col_chi(strQ2B(fund_found_list[i][1])))
        t2 =''.join([x for x in jieba.lcut(t2) if x not in tyc_word])
        if temp != 0:
            temp = 1 / temp
        else:
            temp = 10000
        if temp <= min_dis:
            temp2 = ld.distance(t1, t2)
            if temp2 < min_ld:
                min_dis = temp
                min_ld = temp2
                min_index = i
    if fund_name_t != fund_found_list[min_index][1] and min_dis != 1:
        simmi.append(1 / min_dis)
    if not isinstance(fund_name, str):
        min_name = [fund_name, fund_found_list[min_index][1], fund_found_list[min_index][2]]
    else:
        min_name = [fund_name, fund_found_list[min_index][1]]
    if min_dis>50: #过滤距离大于100的基金，阈值可调整
        return np.nan
    if min_ld>0:
        return np.nan
    return min_name


def match_name(fund_name, fund_name2, range, range2, col_name, col_name2, primary_key=None, how=1):
    """
    从fund_name2_aft的col_name2的range2范围中匹配fund_name的_col_nam1的range范围中的基金名称，比对基金名称并返回一个dataframe
    返回的dataframe中第一列为fund_id,第二列为souce_ID，第三列为raw_fund_name,第四列为match_fund_name,第五列为可能的列表:raw_list,第六列为程序匹配名match_prog_name
    第七列为数据表主键，如果进行比对的两个dataframe有相同的主键，并且需要输出则可以填写，默认没有，第八列为匹配的输出形式
    1：匹配raw_list，并选择比配程度最高的那一个，输出与fund_name表连接的数据表,是一个dataframe 2：同1，但只输出index和最高程度的匹配，如有主键，也会输出主键，是一个dataframe
    3：匹配raw_list, 不选择匹配程度最高，直接与fund_name表连接，是一个dataframe 4:同3，但只输出index和raw_list，如有主键，也会输出主键，是一个dataframe
    5: 输出包括fund_name，raw_list，最高程度匹配，包含完整的信息，是一个dataframe 6: 只输出index,rawlist,最高匹配，如有主键，也会输出主键，是一个dataframe

    Args:
    fund_name - dataframe , fund_name2_aft, range - range , range2 - range , col_name - str , col_name2 - str

    Returns:
    res_frame - dataframe

    """
    global tyc_word
    is_primary_key = False
    if primary_key:
        if primary_key in fund_name.columns.values and primary_key in fund_name2.columns.values:
            print("Successful in getting primary key.")
            is_primary_key = True
        else:
            raise Exception("Primary_error")
    if how not in [1, 2, 3, 4, 5, 6]:
        raise Exception("How error")
    if not isinstance(fund_name.loc[0, col_name], str) or not isinstance(fund_name2.loc[0, col_name2], str):
        raise Exception("Input error")

    # 更新语料库
    fenci = []
    l1 = fund_name.loc[:, col_name].tolist()
    l2 = fund_name2.loc[:, col_name2].tolist()
    ylk_list = l1 + l2
    for ii in ylk_list:
        fenci.append(' '.join(jieba.cut(sub_chi2num(col_chi(strQ2B(ii))))))
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    dict_file = os.path.join(script_dir, 'fen_full_name.txt')
    with open(dict_file, 'w', encoding='utf-8') as fw:
        for tt in fenci:
            fw.write(tt)
            fw.write('\n')
    # 初始化停用词
    tyc_word = init_tyc()
    # 训练模型
    model = init_word2vec_model()
    # 格式化比对名称
    fund_name2_aft = init_2_f_aft(fund_name2, col_name2)
    if is_primary_key:
        t1 = pd.DataFrame(fund_name.loc[range, [col_name, primary_key]].apply(raw_list, args=(
            fund_name2_aft, col_name2, range2, primary_key), axis=1), columns=['raw_list'])
    else:
        t1 = pd.DataFrame(fund_name.loc[range, col_name].apply(raw_list, args=(
            fund_name2_aft, col_name2, range2, primary_key))).rename(columns={col_name: 'raw_list'})
    t2 = pd.DataFrame(t1.loc[:, "raw_list"].apply(choose_best, args=([model]))).rename(
        columns={'raw_list': 'best_name'})
    if how == 1:
        res_list = pd.merge(fund_name, t2, left_index=True, right_index=True, sort=False)
    elif how == 2:
        res_list = t2
    elif how == 3:
        res_list = pd.merge(fund_name, t1, left_index=True, right_index=True, sort=False)
    elif how == 4:
        res_list = t2
    elif how == 5:
        temp = pd.merge(t1, t2, left_index=True, right_index=True, sort=False)
        res_list = pd.merge(fund_name, temp, left_index=True, right_index=True, sort=False)
    elif how == 6:
        res_list = pd.merge(t1, t2, left_index=True, right_index=True, sort=False)
    return res_list


def reshape(data, primary_key=None, how=2):
    """
    调整由match_name生成的raw_list（如果有）的dataframe，将列表展开
    how为展开方法，how=1为只展开best_match how=2为全展开 其他数字将默认按照全展开来展开

    Args:
    fund_name - dataframe , is_primary - bool , how - int

    Returns:
    res_frame - dataframe

    """
    if how==2:
        if primary_key:
            if data.empty:
                return np.nan
            data_list = data['raw_list'].tolist()
            best_name = data['best_name'].tolist()
            new_list=[]
            for i in range(len(data_list)):
                ii=data_list[i]
                fund_name_ori = ii[0][0][0]
                fund_id_ori = ii[0][0][1]
                if isinstance(best_name[i],float):
                    match_best_name=np.nan
                    match_best_id=np.nan
                else:
                    match_best_name=best_name[i][1]
                    match_best_id = best_name[i][2]
                for jj in ii:
                    match_name = jj[1]
                    match_id = jj[2]
                    row_list = (fund_name_ori, fund_id_ori, match_name, match_id, match_best_name, match_best_id)
                    new_list.append(row_list)
            new_tes = pd.DataFrame(new_list, columns=['match_target', primary_key, 'matched', primary_key,'best_name',primary_key])
        else:
            if data.empty:
                return np.nan
            data_list = data['raw_list'].tolist()
            best_name = data['best_name'].tolist()
            new_list = []
            for i in range(len(data_list)):
                ii = data_list[i]
                fund_name_ori = ii[0][0]
                if isinstance(best_name[i], float):
                    match_best_name=np.nan
                else:
                    match_best_name=best_name[i][1]
                for jj in ii:
                    match_name = jj[1]
                    row_list = (fund_name_ori, match_name,match_best_name)
                    new_list.append(row_list)
            new_tes = pd.DataFrame(new_list, columns=['match_target', 'matched','best_name'])
    elif how==1:
        if primary_key:
            if data.empty:
                return np.nan
            best_name = data['best_name'].tolist()
            new_list=[]
            for i in range(len(best_name)):
                if isinstance(best_name[i],float):
                    continue
                else:
                    match_best_name=best_name[i][1]
                    match_best_id = best_name[i][2]
                    fund_name_ori=best_name[i][0][0]
                    fund_id_ori=best_name[i][0][1]
                row_list = (fund_name_ori, fund_id_ori, match_best_name, match_best_id)
                new_list.append(row_list)
            new_tes = pd.DataFrame(new_list, columns=['match_target', primary_key, 'best_name',primary_key])
        else:
            if data.empty:
                return np.nan
            best_name = data['best_name'].tolist()
            new_list=[]
            for i in range(len(best_name)):
                if isinstance(best_name[i],float):
                    continue
                else:
                    match_best_name=best_name[i][1]
                    fund_name_ori=best_name[i][0]

                row_list = (fund_name_ori, match_best_name)
                new_list.append(row_list)
            new_tes = pd.DataFrame(new_list, columns=['match_target',  'best_name'])
    else:
        print('Method error! Using default methed')
        new_tes=reshape(data, primary_key)
    return new_tes


def simi_vec(str1, str2, model):
    """
    使用model计算str1,str2的相似度，包含剔除潜在母子基金关系部分

    Args:
    str1 - str , str2 - str , model - gensim.models.word2vec.Word2Vec

    Returns:
    sim - float

    """
    set1 = set(del_space(jieba.lcut(sub_chi2num(col_chi(strQ2B(str1))))))
    set2 = set(del_space(jieba.lcut(sub_chi2num(col_chi(strQ2B(str2))))))
    set1 = set([x for x in set1 if x not in tyc_word])
    set2 = set([x for x in set2 if x not in tyc_word])
    sysm_set = set1 ^ set2
    muzi_re = re.compile(r'\d|[A-Z]|[a-z]')
    for ii in sysm_set:
        if muzi_re.match(ii):
            return 0
    ns1 = np.zeros(200)
    ns2 = np.zeros(200)
    for s1 in set1:
        ns1 = ns1 + model.wv[s1]
    for s2 in set2:
        ns2 = ns2 + model.wv[s2]
    dist = np.linalg.norm(ns1 - ns2)
    sim = 1.0 / (1.0 + dist)
    return sim

def df_private():
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/crawl_private?charset=utf8')
    df_private = pd.read_sql("SELECT * FROM (SELECT  fund_id \
    , fund_name_amac,reg_code_amac \
    FROM x_fund_info_private WHERE fund_id not in  \
    (SELECT source_id FROM base.id_match WHERE source='010003' and is_used=1) \
    and entry_time>'2017-11-20' and version>1 \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id", engine)
    df_private.rename(columns={"fund_name_amac": "test_name"}, inplace=True)
    # df_private["private_id"]=df_private["private_id"].apply(lambda x: 'ID:'+str(x))
    print(len(df_private))
    return df_private,len(df_private)

def init_match():
    global gg
    gg=0
    global simmi
    simmi=[]
    global tyc_word
    tyc_word=[]
    global white_word
    white_word = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0']

def match_port(fund_name, fund_name2,ntest, nrow, d1_name='fund_full_name',
                              d2_name='fund_full_name', primary_key='fund_id', how=5,re_meth=2,excel_name='match.xlsx'):
    init_jieba()
    init_match()
    #'''
    try:
        res_data = match_name(fund_name, fund_name2,range(ntest), range(nrow), d1_name,
                              d2_name, primary_key=primary_key, how=how)
    except "Primary_error":
        print("The primary key doesn't exist in both of the dataframe.")
    except "How error":
        print("Output method error.")

    res_data.dropna(how='any',inplace=True)
    res_get=reshape(res_data,'fund_id',re_meth)

    # writer = pd.ExcelWriter(excel_name)
    # res_get.to_excel(writer, "Sheet1")
    # writer.save()
    return res_get

if __name__ == '__main__':
    [fund_name, fund_name2, ntest, nrow] = init_update()
    res_get=match_port(fund_name, fund_name2, ntest, nrow, d1_name='test_name',how=5)