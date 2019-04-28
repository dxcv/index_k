# -*- coding: utf-8 -*-
from WindPy import w
from sqlalchemy import create_engine
import pandas as pd
import re
from utils.database import io

w.start()

# engine = create_engine('mysql+mysqldb://root:5461wen@127.0.0.1:3306/hm', connect_args={'charset': 'utf8'}, pool_size=8)
engine_private = create_engine('mysql+mysqldb://jr_read_zw:jr_read_zw@182.254.128.241:4171/crawl_finance',
                               connect_args={'charset': 'utf8'}, pool_size=8)


def get_id():
    sql = 'SELECT bond_id FROM base_finance.`bond_info` '
    bids = sorted(pd.read_sql(sql, engine_private)["bond_id"].tolist())
    return bids


def filled_id(bid):
    info = w.wss("{}".format(bid), "rate_former,rate_issuerformer,rate_fwdissuer,fullname","tradeDate=20171225;type=1;ratingAgency=101")  # tradeDate参数似乎不影响使用, 传入任意一个值
    dp = pd.DataFrame(info.Data)  # 用结果(list)去构造一个DataFrame二维表
    dp = dp.T  # 把表转置
    dp["bond_id"] = bid
    return dp


def form(dp):
    db = pd.DataFrame()
    rating_former = dp.iloc[0, 0]
    rating_issuerformer = dp.iloc[0, 1]
    outlook = dp.iloc[0, 2]
    name = str(dp.iloc[0, 3])
    if rating_former:
        rating_former_list = re.split('\r', str(rating_former))
        for i in rating_former_list:
            bond_id = dp["bond_id"].item()
            bond_name = name
            rating_outlook = outlook
            rating_type = '债券评级'
            credit_rating = re.search('(.+?)\(', i, re.DOTALL).group(1)
            rating_agency = re.search('评级-(.+?)-', i, re.DOTALL).group(1)
            statistic_date = re.search('\d+\)', i, re.DOTALL).group().replace(')', '')
            source_id = '020004'  # wind
            item = {
                'bond_id': bond_id,
                'bond_name': bond_name,
                'rating_outlook': rating_outlook,
                'rating_type': rating_type,
                'credit_rating': credit_rating,
                'rating_agency': rating_agency,
                'source_id': source_id,
                'statistic_date': statistic_date
            }
            db = db.append(pd.DataFrame([item]))

            # db.to_sql('d_bond_rating', engine, if_exists='append', index=False)
    if rating_issuerformer:
        rating_issuerformer_list = re.split('\r', str(rating_issuerformer))
        for j in rating_issuerformer_list:
            bond_id = dp["bond_id"].item()
            bond_name = name
            source_id = '020004'  # wind
            rating_type = '主体评级'
            rating_outlook = re.search('公司-(.+?)-\d', j, re.DOTALL)
            if rating_outlook:
                rating_outlook = rating_outlook.group(1)
            credit_rating = re.search('(.+?)\(', j, re.DOTALL).group(1)
            rating_agency = re.search('评级-(.+?)-', j, re.DOTALL).group(1)
            statistic_date = re.search('\d+\)', j, re.DOTALL).group().replace(')', '')
            item = {
                'bond_id': bond_id,
                'bond_name': bond_name,
                'rating_outlook': rating_outlook,
                'rating_type': rating_type,
                'credit_rating': credit_rating,
                'rating_agency': rating_agency,
                'source_id': source_id,
                'statistic_date': statistic_date
            }
            db = db.append(pd.DataFrame([item]))
    return db


def crwal(bids):
    mistakes = []
    result = pd.DataFrame()
    for bid in bids:
        try:
            print(bid)
            tmp_resupt = form(filled_id(bid))
            print(tmp_resupt)
            result = result.append(tmp_resupt)
        except:
            mistakes.append(bid)
            continue
    return result


def main():
    bid_whole = get_id()
    step = 500
    for i in range(0, len(bid_whole), step):
        res = crwal(bid_whole[i: i + step])
        print(len(res), res)
        io.to_sql('d_bond_rating', engine_private, res)


if __name__ == '__main__':
    main()
