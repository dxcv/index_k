import pandas as pd
import re
import codecs
from utils.database import config as cfg, io

engines = cfg.load_engine()
engine_rd = engines["2Gb"]
engine_wt = engines["2Gb"]


def info_region_match():
    region_info = pd.read_sql(
        "select fund_id, region from fund_info_aggregation WHERE region IS NOT NULL AND region <> ''",
        engine_rd)
    file = codecs.open("./Scripts/DataQuality/fund_info/TestChooseAddress.js", 'r', "utf-8")
    region_file = file.read()
    region_info_copy = region_info
    prov = {'11': '北京', '12': '天津', '13': '河北', '14': '山西', '15': '内蒙古', '21': '辽宁', '22': '吉林', '23': '黑龙江',
            '31': '上海', '32': '江苏', '33': '浙江', '34': '安徽', '35': '福建', '36': '江西', '37': '山东', '41': '河南', '42': '湖北',
            '43': '湖南', '44': '广东', '45': '广西', '46': '海南', '50': '重庆', '51': '四川', '52': '贵州', '53': '云南', '54': '西藏',
            '61': '陕西', '62': '甘肃', '63': '青海', '64': '宁夏', '65': '新疆', '71': '台湾', '81': '香港', '82': '澳门', '90': '外国'}
    for i in range(len(region_info)):
        region = region_info['region'][i]
        pattern = "'(\d+)','({}.*?)'".format(region)
        try:
            match_result = re.search(pattern, region_file).groups()
            if match_result[0] in (
                    '440300', '440303', '440304', '440305', '440306', '440307', '440308', '440391', '440392'):
                region_info_copy['region'][i] = '深圳'
                # print(region_info['fund_id'][i])
            else:
                prov_code = match_result[0][0:2]
                region_info_copy['region'][i] = prov[prov_code]
                # sql = "update easy.fund_info_aggregation set region = '{}' where fund_id = '{}'".format(
                #     region_info_copy['region'][i], region_info_copy['fund_id'][i])
                # engine_rd.execute(sql)
        except Exception as e:
            print(region_info['fund_id'][i], region)
    return region_info_copy


def main():
    df_region = info_region_match()
    io.to_sql("fund_info", engine_wt, df_region)


if __name__ == "__main__":
    main()
