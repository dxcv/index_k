import pandas as pd
import numpy as np
from utils.database import config as cfg, io
ENGINE_RD = cfg.load_engine()["2Gb"]


class stream:
    """
    公募人员匹配
    """

    @classmethod
    def init_id_match(cls):
        global max_id, matched_info
        sql_com = "SELECT data_source, id_type, source_id, matched_id, accuracy, is_used, is_del " \
                  "from base_public.id_match where id_type = 3"
        data = pd.read_sql_query(sql_com, ENGINE_RD)
        if data.empty:
            return None
        print(len(data))
        max_id = data['matched_id'].max()
        print(max_id)
        matched_info = data
        return data

    @classmethod
    def id_match(cls):
        sql_com = "SELECT data_source, id_type, source_id, matched_id, accuracy, is_used, is_del " \
                  "from base_public.id_match where id_type = 3"
        data = pd.read_sql_query(sql_com, ENGINE_RD)
        return data

    @classmethod
    def init_person_info(cls):
        sql_com = "SELECT * from crawl_public.d_person_info " \
                  "where data_source = '020001'or data_source = '020002'"
        data = pd.read_sql_query(sql_com, ENGINE_RD)
        if data.empty:
            return None
        return data

    @classmethod
    def init_person_info_t(cls):
        sql_com = "SELECT * from crawl_public.d_person_info " \
                  "where (data_source='020001'or data_source='020002')and person_id  REGEXP '^[0-9]{8}$' "
        data = pd.read_sql_query(sql_com, ENGINE_RD)
        if data.empty:
            return None
        return data

    @classmethod
    def init_org_info(cls):
        sql_com = "SELECT * from crawl_public.d_org_info"
        data = pd.read_sql_query(sql_com, ENGINE_RD)
        if data.empty:
            return None
        return data

    @classmethod
    def init_person_org(cls):
        sql_com = "SELECT * from crawl_public.d_person_org"
        data = pd.read_sql_query(sql_com, ENGINE_RD)
        if data.empty:
            return None
        print(len(data))
        return data

    @classmethod
    def get_all_org_name(cls, new_row, person_org, org_info):
        person_id = new_row['source_id']
        person_source = new_row['data_source']
        raw_org_name = person_org['org_name'][
            (person_org.person_id == person_id) & (person_org.data_source == person_source)]
        if raw_org_name.empty:
            print(new_row['data_source'])
            print(new_row['source_id'])
            print('get_all')
            return []
        raw_org_full_name = org_info['org_full_name'][org_info.org_name.isin(raw_org_name.tolist())]
        if raw_org_full_name.empty:
            print("impossible")
            print(raw_org_name)
            return []
        raw_full_list = raw_org_full_name.tolist()
        return raw_full_list

    @classmethod
    def row_new(cls, new_person, org_info, person_org, person_info):
        global max_id, matched_info
        is_new = False
        name = new_person['person_name']
        id = new_person['person_id']
        '''看看这个人有没有现任信息'''
        new_test = person_org['org_id'][(person_org.person_id == id) & (person_org.is_current == 1)]
        if new_test.empty:
            '''如果没有 特殊处理'''
            new_0_cur = person_org['org_id'][(person_org.person_id == id) & (person_org.is_current == 0)]
            org_name_new = org_info['org_full_name'][org_info.org_id.isin(new_0_cur.tolist())].tolist()
            print(org_name_new)
            print(id)
            raw_name = matched_info[['matched_id', 'source_id', 'data_source']][matched_info.person_name == name]
            if raw_name.empty:
                matched_id = str(int(max_id) + 1).zfill(8)
                max_id = matched_id
                is_new = True
            else:
                print(raw_name)
                em_list = []
                for i in range(len(raw_name)):
                    em_list.append([])
                raw_name['all_name_list'] = pd.Series(em_list)
                for i in raw_name.index:
                    raw_name.loc[i, 'all_name_list'] = cls.get_all_org_name(raw_name.loc[i, :], person_org, org_info)
                print(raw_name)
                for i in raw_name.index:
                    if set(org_name_new) & set(raw_name.loc[i, 'all_name_list']):
                        matched_id = raw_name.loc[i, 'matched_id']
                        break
                    if i == max(raw_name.index):
                        print(i)
                        matched_id = str(int(max_id) + 1).zfill(8)
                        max_id = matched_id
                        is_new = True

            res_list = [new_person['data_source'], 3, new_person['person_id'], matched_id, 1, 1, 0]
            res_ser = pd.Series(res_list)
            if is_new:
                app_newrow = pd.DataFrame([res_list],
                                          columns=['data_source', 'id_type', 'source_id', 'matched_id', 'accuracy',
                                                   'is_used', 'is_del'])
                print(app_newrow)
                app_newrow['person_name'] = cls.get_name(app_newrow.loc[0, :], person_info)
                app_newrow['org_full_name'] = cls.get_org_full_name(app_newrow.loc[0, :], person_org, org_info)
                matched_info = matched_info.append(app_newrow, ignore_index=True)
            return res_ser

        '''如果有，正常处理'''
        raw_name = matched_info[['matched_id', 'org_full_name']][matched_info.person_name == name]
        if raw_name.empty:
            matched_id = str(int(max_id) + 1).zfill(8)
            max_id = matched_id
            is_new = True
        else:
            raw_id = person_org[['org_id', 'is_current']][person_org.person_id == id]
            if raw_id.empty:
                matched_id = raw_name['matched_id'].tolist()[0]
            else:
                temp_id = raw_id['org_id'][raw_id.is_current == 1]
                if temp_id.empty:
                    matched_org = raw_id['org_id'].tolist()[0]
                    org_full_name = org_info['org_full_name'][org_info.org_id == matched_org].tolist()[0]
                    matched_list = raw_name['matched_id'][raw_name.org_full_name == org_full_name]
                    if matched_list.empty:
                        temp2_id = raw_id['org_id'][raw_id.is_current == 0]
                        if temp2_id.empty:
                            matched_id = str(int(max_id) + 1).zfill(8)
                            max_id = matched_id
                            is_new = True
                        else:
                            matched_org = temp2_id.tolist()[0]
                            org_full_name = org_info['org_full_name'][org_info.org_id == matched_org].tolist()[0]
                            matched_list = raw_name['matched_id'][raw_name.org_full_name == org_full_name]
                            if matched_list.empty:
                                matched_id = str(int(max_id) + 1).zfill(8)
                                max_id = matched_id
                                is_new = True
                            else:
                                matched_id = matched_list.tolist()[0]
                    else:
                        matched_id = matched_list.tolist()[0]
                else:
                    matched_org = temp_id.tolist()[0]
                    org_full_name = org_info['org_full_name'][org_info.org_id == matched_org]
                    if org_full_name.empty:
                        print(matched_org)
                        org_full_name = ''
                    else:
                        org_full_name = org_full_name.tolist()[0]
                    matched_list = raw_name['matched_id'][raw_name.org_full_name == org_full_name]
                    if matched_list.empty:
                        temp2_id = raw_id['org_id'][raw_id.is_current == 0]
                        if temp2_id.empty:
                            matched_id = str(int(max_id) + 1).zfill(8)
                            max_id = matched_id
                            is_new = True
                        else:
                            matched_org = temp2_id.tolist()[0]
                            org_full_name = org_info['org_full_name'][org_info.org_id == matched_org].tolist()[0]
                            matched_list = raw_name['matched_id'][raw_name.org_full_name == org_full_name]
                            if matched_list.empty:
                                matched_id = str(int(max_id) + 1).zfill(8)
                                max_id = matched_id
                                is_new = True

                            else:
                                matched_id = matched_list.tolist()[0]
                    else:
                        matched_id = matched_list.tolist()[0]
        res_list = [new_person['data_source'], 3, new_person['person_id'], matched_id, 1, 1, 0]
        res_ser = pd.Series(res_list)
        if is_new:
            app_newrow = pd.DataFrame([res_list],
                                      columns=['data_source', 'id_type', 'source_id', 'matched_id', 'accuracy',
                                               'is_used',
                                               'is_del'])
            app_newrow['person_name'] = cls.get_name(app_newrow.loc[0, :], person_info)
            app_newrow['org_full_name'] = cls.get_org_full_name(app_newrow.loc[0, :], person_org, org_info)
            matched_info = matched_info.append(app_newrow, ignore_index=True)
        return res_ser

    @classmethod
    def get_name(cls, new_row, person_info):
        name = person_info['person_name'][
            (person_info.data_source == new_row['data_source']) & (person_info.person_id == new_row['source_id'])]
        if name.empty:
            return np.nan
        name = str(name.tolist()[0])
        return name

    @classmethod
    def get_org_full_name(cls, new_row, person_org, org_info):
        org_name = person_org['org_name'][
            (person_org.data_source == new_row['data_source']) & (person_org.person_id == new_row['source_id']) & (
                    person_org.is_current == 1)]
        if org_name.empty:
            org_name = person_org['org_name'][
                (person_org.data_source == new_row['data_source']) & (person_org.person_id == new_row['source_id']) & (
                        person_org.is_current == 0)]
            if org_name.empty:
                return np.nan
        org_name = str(org_name.tolist()[0])
        org_full_name = org_info['org_full_name'][(org_info.org_name == org_name)]
        if org_full_name.empty:
            return np.nan
        org_full_name = str(org_full_name.tolist()[0])
        return org_full_name

    @classmethod
    def add_new(cls, new_data, org_info, id_match, person_info, person_org):
        global matched_info
        matched_info['person_name'] = id_match.apply(cls.get_name, args=([person_info]), axis=1)
        matched_info['org_full_name'] = id_match.apply(cls.get_org_full_name, args=([person_org, org_info]), axis=1)
        res_data = new_data.apply(cls.row_new, args=([org_info, person_org, person_info]), axis=1).rename(
            columns={0: 'data_source', 1: 'id_type', 2: 'source_id', 3: 'matched_id', 4: 'accuracy', 5: 'is_used',
                     6: 'is_del'})
        print(res_data.head(5))
        return res_data

    @classmethod
    def confluence(cls):
        id_match = cls.init_id_match()
        org_info = cls.init_org_info()
        person_org = cls.init_person_org()
        person_info = cls.init_person_info()
        person_info_t = cls.init_person_info_t()
        res_data = cls.add_new(person_info_t, org_info, id_match, person_info, person_org)
        print(res_data)
        res_data["all"] = list(map(lambda x, y, z: z + y + z, res_data["data_source"], res_data["source_id"],
                                   res_data["matched_id"]))
        base_id = cls.id_match()
        base_id["all"] = list(map(lambda x, y, z: z + y + z, base_id["data_source"], base_id["source_id"],
                                  base_id["matched_id"]))
        id_list = base_id["all"].tolist()
        i = dict.fromkeys(id_list, "在")
        res_data["is_in"] = res_data["all"].apply(lambda x: i.get(x))
        df = res_data[res_data.is_in != "在"].iloc[:, 0:6]
        return df

    @classmethod
    def write(cls):
        df = cls.confluence()
        io.to_sql("data_test.id_match_gm", ENGINE_RD, df, type="update")
        """李佳人工再验证几次看情况，效果可以再改主库"""


def main():
    stream.write()


if __name__ == "__main__":
    main()
