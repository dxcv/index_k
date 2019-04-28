"""
【数据】同步 - 8612test_gt新表同步(1D)
"""
import datetime as dt
import pandas as pd
from utils.database import config as cfg, io

ENGINE_GM = cfg.load_engine()["2Gbp"]
ENGINE_RD = cfg.load_engine()["2Gb"]
ENGINE_8612_gm = cfg.load_engine()["8612app_mutual"]
ENGINE_4119PRODUCT = cfg.load_engine()["4Gp"]
ENGINE_4119PRODUCT_MUTUAL = cfg.load_engine()["4Gpp"]
ENGINE_8612_test_gt = cfg.load_engine()["8612test_sync"]

tasks = [
    ['fund_rank', ENGINE_RD, 'fund_rank', ENGINE_8612_test_gt],
    ["fund_manager_mapping", ENGINE_RD, "fund_manager_mapping", ENGINE_8612_test_gt],
    ["org_integrity", ENGINE_RD, "org_integrity", ENGINE_4119PRODUCT],
    ["org_integrity", ENGINE_RD, "org_integrity", ENGINE_8612_test_gt]
]


class sync_same_table:
    def __init__(self, tb1, Engine1, tb2, Engine2, chunck_size=5):
        self.new = tb1
        self.engine_new = Engine1
        self.old = tb2
        self.engine_old = Engine2
        self.chunck_size = chunck_size
        self.now = dt.datetime.now()
        self.diff_index = []
        self.diff_data = []
        self.del_index = []
        self.del_data = []

    def _get_id(self):
        col = pd.read_sql("SELECT column_name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE`"
                          "WHERE table_name='{}' "
                          "AND CONSTRAINT_SCHEMA='base' "
                          "AND constraint_name='PRIMARY'".format(self.new), self.engine_new)
        col.loc[len(col)] = {'column_name': 'update_time'}
        a = ",".join(col['column_name'].tolist())
        self.target_column = a
        self.target_index = col['column_name'].tolist()

    def get_new_index(self):
        sql_com = "select {} from {}".format(self.target_column, self.new)
        self.new_index = pd.read_sql(sql_com, self.engine_new)
        print("get_new_index", len(self.new_index))
        self.new_index[self.target_index[0]] = self.new_index[self.target_index[0]].apply(lambda x: str(x))
        return self.new_index

    def get_old_index(self):
        sql_com = "select {} from {}".format(self.target_column, self.old)
        self.old_index = pd.read_sql(sql_com, self.engine_old)
        print("get_old_index", len(self.old_index))
        self.old_index[self.target_index[0]] = self.old_index[self.target_index[0]].apply(lambda x: str(x))
        return self.old_index

    def get_new_data(self, ii):
        str_list = "'" + '\',\''.join(ii) + "'"
        sql_com = "select * from {} where {} in ({})".format(self.new, self.first_index, str_list)
        self.new_data = pd.read_sql(sql_com, self.engine_new)
        print("get_new_data")

        return self.new_data

    def get_old_data(self, ii):
        str_list = "'" + '\',\''.join(ii) + "'"
        sql_com = "select * from {} where {} in ({})".format(self.old, self.first_index, str_list)
        self.old_data = pd.read_sql(sql_com, self.engine_old)
        print("get_old_data")
        return self.old_data

    def delete(self, tb_name, conn, dataframe):
        dataframe = dataframe.dropna(how='all').drop_duplicates()
        for i in range(len(dataframe)):
            df = dataframe.iloc[i, :]
            c = pd.DataFrame(df).T
            condition = self.generate_condition(c)
            sql = "DELETE FROM {tb} WHERE {criterion}".format(
                tb=tb_name, criterion=condition
            )
            conn.execute(sql)

    def generate_condition(self, dataframe):
        index = self.target_index[:-1]
        dataframe = dataframe.astype(str)
        del_target = dataframe[index].values.tolist()
        condition = []
        for i in range(len(del_target[0])):
            if i == 0:
                condition.append("{} = '{}' ".format(self.target_index[0], del_target[0][0]))
            else:
                condition.append("and {} = '{}' ".format(self.target_index[i], del_target[0][i]))

        condition_str = ' '.join(condition)
        return condition_str

    def sync(self):
        if not hasattr(self, 'target_column'):
            self._get_id()
        self.first_index = self.target_index[0]

        new_index = self.get_new_index()
        old_index = self.get_old_index()
        new_index.rename(columns={'update_time': 'n_time'}, inplace=True)
        old_index.rename(columns={'update_time': 'o_time'}, inplace=True)
        index = self.target_index[:-1]
        chunck_list = new_index[self.first_index].drop_duplicates().values.tolist()
        chunck_list = [chunck_list[i:i + self.chunck_size] for i in range(0, len(chunck_list), self.chunck_size)]
        for ii in chunck_list:
            print(ii)
            tn_ind = new_index[new_index[self.first_index].isin(ii)]
            tn_ind.set_index(index, inplace=True)
            to_ind = old_index[old_index[self.first_index].isin(ii)]
            to_ind.set_index(index, inplace=True)
            con_index_data = pd.merge(tn_ind, to_ind, left_index=True, right_index=True, how='left')
            diff_index = con_index_data[con_index_data.n_time != con_index_data.o_time]
            del_index_data = pd.merge(tn_ind, to_ind, left_index=True, right_index=True, how='right')
            del_index = del_index_data[del_index_data.n_time.isnull()]
            if diff_index.empty and del_index.empty:
                continue
            if not diff_index.empty:
                df_new = self.get_new_data(ii)
                new_data = df_new.set_index(index)
                self.diff_index.append(diff_index)
                diff_data = pd.merge(diff_index, new_data, left_index=True, right_index=True, how='left')
                diff_data.reset_index(inplace=True)
                del diff_data['o_time']
                del diff_data['n_time']
                self.diff_data.append(diff_data)

                df_old = self.get_old_data(ii)


                old_col = df_old.columns.values.tolist()
                new_col = df_new.columns.values.tolist()
                same = [l for l in old_col if l in new_col]
                last_df = diff_data.loc[:, same]
                io.to_sql(self.old, self.engine_old, last_df, type="update")
            if not del_index.empty:
                old_data = self.get_old_data(ii).set_index(index)
                del_data = pd.merge(del_index, old_data, left_index=True, right_index=True, how='left')
                del_data.reset_index(inplace=True)
                del del_data['o_time']
                del del_data['n_time']
                self.delete(self.old, self.engine_old, del_data)
                self.del_data.append(del_data)


if __name__ == '__main__':
    for i in tasks:
        gg = sync_same_table(i[0], i[1], i[2], i[3], chunck_size=100)
        gg.sync()
        print('finish')
