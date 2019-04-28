from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import multiprocessing
from utils.database import io

ENGINE_EASY = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('sm01', 'x6B28Vz9', '58cb57e7707af.sh.cdb.myqcloud.com', 8612, 'test_gt', ),
    connect_args={"charset": "utf8"})

FUND_TYPE_DICT = {6010702: '私募FOF',
                  6010101: '股票多头',
                  6010102: '股票多空',
                  6010103: '市场中性',
                  60105: '债券基金',
                  60102: '管理期货',
                  60106: '宏观策略',
                  60104: '事件驱动',
                  60103: '相对价值',
                  60108: '多策略',
                  60107: '组合投资',
                  60109: '其他策略'}


def fetch_type_indicators(fund_type, static_date, freq='m', engine=ENGINE_EASY):
    indicators = ['return_a', 'return', 'sharp_a', 'max_retracement']
    freq_length = ['total', 'year', 'm3', 'm6', 'y1', 'y2', 'y3', 'y5']
    indicator_list = [lens + '_' + ind for ind in indicators for lens in freq_length]

    _freq_fund_table = {'w': 'weekly', 'm': 'month'}[freq]
    sql = "SELECT tb_re.fund_id, tb_re.fund_name,{inds},tb_re.statistic_date from " \
          "fund_{tb_freq}_return tb_re LEFT JOIN " \
          "fund_{tb_freq}_risk tb_ri ON tb_re.fund_id = tb_ri.fund_id AND " \
          "tb_re.statistic_date = tb_ri.statistic_date AND tb_re.benchmark=tb_ri.benchmark " \
          "JOIN fund_type_mapping ftm ON ftm.fund_id = tb_re.fund_id " \
          "where ftm.typestandard_code = 1 AND (ftm.type_code='{f_type}' or ftm.stype_code='{f_type}') " \
          "AND tb_re.benchmark = 1 AND tb_re.statistic_date IN ({sd})".format(inds=format_sql_columns(indicator_list),
                                                                              tb_freq=_freq_fund_table,
                                                                              f_type=fund_type,
                                                                              sd=format_sql_value(static_date))
    original_data = pd.read_sql(sql, engine)
    original_data.index = original_data['statistic_date']
    original_data.drop('statistic_date', axis=1, inplace=True)
    return original_data


def fetch_type_date(fund_type, freq='m', engine=ENGINE_EASY):
    _freq_fund_table = {'w': 'weekly', 'm': 'month'}[freq]
    sql_date = "SELECT DISTINCT tb_re.statistic_date FROM fund_type_mapping ftm JOIN fund_{tb_freq}_return tb_re ON " \
               "ftm.fund_id = tb_re.fund_id AND tb_re.benchmark = 1 WHERE ftm.typestandard_code = 1 AND " \
               "(ftm.type_code='{f_type}' or ftm.stype_code='{f_type}')".format(tb_freq=_freq_fund_table,
                                                                                f_type=fund_type)
    date_list = pd.read_sql(sql_date, engine)['statistic_date'].tolist()
    return date_list


def _slice_df_by_dates(df, date_list):
    data = {}
    for dates in date_list:
        data[dates] = df.loc[dates]
    return data


def _calculate_fund_ranking(type_data):
    nums = len(type_data)
    ids = type_data[['fund_id', 'fund_name']]
    type_data.drop(['fund_id', 'fund_name'], axis=1, inplace=True)
    rank_data = type_data.rank(ascending=False, method='min')
    freq_length = ['total', 'year', 'm3', 'm6', 'y1', 'y2', 'y3', 'y5']
    for lens in freq_length:
        rank_data[lens + '_' + 'max_retracement'] = type_data[lens + '_' + 'max_retracement'].rank(ascending=True,
                                                                                                   method='min')
    rank_data.fillna('-', inplace=True)
    rank_data = rank_data.applymap(lambda x: "{}/{}".format(int(x), int(nums)) if x != '-' else np.NaN)
    rank_data[['fund_id', 'fund_name']] = ids
    return rank_data


def split_list(li, num):
    result = []
    for i in range(0, len(li), num):
        result.append(li[i:i + num])
    return result


def update_ranking():
    type_list = FUND_TYPE_DICT.keys()
    pool = multiprocessing.Pool(processes=2)
    pool.map(_fund_ranking_calculate, type_list)
    pool.close()
    pool.join()


def _fund_ranking_calculate(fund_type):
    print(FUND_TYPE_DICT.get(fund_type))
    date_list = fetch_type_date(fund_type)
    split_date = split_list(date_list, 5)
    for date_li in split_date:
        type_data = fetch_type_indicators(fund_type, date_li)
        data_list = _slice_df_by_dates(type_data, date_li)
        rank_data = list(map(_calculate_fund_ranking, data_list.values()))
        rank_result = pd.concat(rank_data)
        rank_result['statistic_date'] = rank_result.index
        rank_result['fund_type'] = FUND_TYPE_DICT.get(fund_type)
        # rank_result.to_csv("C:\\Users\\Zhan\\Desktop\\re.csv")
        io.to_sql('fund_type_rank', ENGINE_EASY, rank_result)
#        to_sql('fund_type_rank', ENGINE_, rank_result)


def format_sql_value(value):
    if isinstance(value, list):
        return ",".join(map(lambda x: "'{}'".format(x), value))
    elif isinstance(value, str):
        return "'{}'".format(value)


def format_sql_columns(col, table=None):
    if table is not None:
        if isinstance(col, str):
            return '{}.{}'.format(table, col)
        elif isinstance(col, list):
            return ','.join(map(lambda x: '{}.{}'.format(table, x), col))
    else:
        if isinstance(col, str):
            return col
        elif isinstance(col, list):
            return ','.join(map(lambda x: '{}'.format(x), col))


def main():
    update_ranking()


if __name__ == '__main__':
    main()
