from utils.database import config as cfg, io, sqlfactory as sf
from utils.decofactory.common import inscache
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
import re
import datetime as dt
from sqlalchemy import create_engine

engines = cfg.load_engine()
engine_base, engine_crawl_private = engines["2Gb"], engines["2Gcpri"]


def get_pk(table_name, engine):
    """

    Args:
        table_name: str
        engine:

    Returns:

    """

    # 不支持跨数据库查询
    # meta = MetaData()
    # table = Table(table_name, meta, autoload=True, autoload_with=engine)
    # pk_columns = table.primary_key.columns.values()[0].name

    ddl_str = engine.execute("SHOW CREATE TABLE {tb}".format(tb=table_name)).fetchone()[1]
    # idxs = [x.replace("`", "") for x in re.findall("KEY `.*` \((`.*`)\)", ddl_str)]
    pk = re.search("PRIMARY KEY \((?P<pk>.*)\)", ddl_str).groupdict()["pk"].replace("`", "").split(",")
    return pk


def replace_abnormalstr(x):
    if type(x) is str:
        if re.sub("-|s", "", x) == "":
            return None
    return x


class DelHelper:
    TABLE_LOGICAL_DEL = {
        1: (
            "crawl_private.y_fund_info", "crawl_private.y_fund_nv", "crawl_private.g_fund_nv",
        ),

        2: (
            "crawl_private.y_org_info",
        ),

        3: [],
    }

    # 需要engine指定为base库
    TABLE_PHYSICAL_DEL = {
        1: (
            "config_private.sync_source",

            "fund_allocation_data", "fund_asset_scale", "fund_change_data", "fund_fee_data", "fund_info",
            "fund_info_aggregation", "fund_info_fundaccount", "fund_info_futures", "fund_info_private",
            "fund_info_securities",
            "fund_info_subsidiary", "fund_manager_mapping", "fund_month_indicator", "fund_month_return",
            "fund_month_risk",
            "fund_month_risk2", "fund_nv_data_source", "fund_nv_data_source_copy2", "fund_nv_data_standard",
            "fund_nv_data_standard_copy2",
            "fund_nv_standard_m", "fund_nv_standard_w", "fund_org_mapping", "fund_portfolio", "fund_security_data",
            "fund_subsidiary_month_index", "fund_subsidiary_month_index2", "fund_subsidiary_month_index3",
            "fund_subsidiary_weekly_index", "fund_subsidiary_weekly_index2",
            "fund_subsidiary_weekly_index3", "fund_type_mapping", "fund_weekly_indicator", "fund_weekly_return",
            "fund_weekly_risk", "fund_weekly_risk2"
        ),

        2: (
            "org_executive_info", "org_integrity", "org_monthly_research", "org_monthly_return", "org_monthly_risk",
            "org_scale_type", "org_info", "fund_org_mapping", "org_person_mapping"
        ),

        3: (
            "base.person_info", "base.org_person_mapping", "base.person_description", "base.fund_manager_mapping"
        )
    }

    def __init__(self, id_type, ids_to_del=None, consistency_check=True):
        """

        Args:
            id_type:
            ids_to_del:
            consistency_check: bool, default False
                除了删除`ids_to_del`, 是否还对库中所有不使用的id作检查与删除

        """

        self._id_type = id_type
        self._ids_to_del = [] if ids_to_del is None else ids_to_del
        self._tpool = ThreadPool(4)
        self.ID_COLUMN = {1: "`fund_id`", 2: "`org_id`", 3: "`person_id`"}[id_type]
        self._consistency_check = consistency_check

    @inscache("_CACHED")
    def get_ids_not_used(self):
        """
        获取基金, 机构, 人员类别下的, 所有数据源都标为不使用的id

        Returns:

        """

        if self._id_type == 1:
            cond = "SELECT DISTINCT matched_id FROM id_match " \
                   "WHERE id_type = 1 AND source NOT IN ('010001', '04', '05') AND is_used = 0 "

        elif self._id_type == 2:
            cond = "SELECT DISTINCT matched_id FROM id_match " \
                   "WHERE id_type = 2 AND is_used = 0 "

        elif self._id_type == 3:
            cond = "SELECT DISTINCT matched_id FROM id_match WHERE id_type = 3 AND is_used = 0 "

        sql = "SELECT matched_id, is_used FROM id_match " \
              "WHERE id_type = {id_type} AND matched_id IN ({cond})".format(id_type=self._id_type, cond=cond)

        df = pd.read_sql(sql, engine_base)
        tf = df.groupby(["matched_id"])["is_used"].any()  # 任何一个源都不被使用时, ID: False
        idx_all_not_used = set(tf.loc[tf == False].index)  # 选出any()为False的

        return idx_all_not_used

    def _physical_del(self, ids_to_del):  # DANGEROUS!
        if not ids_to_del:
            return False

        ids_to_del = sf.SQL.values4sql(ids_to_del)
        for table in self.TABLE_PHYSICAL_DEL[self._id_type]:
            if table == "config_private.sync_source":
                col = "pk"
            else:
                col = self.ID_COLUMN

            sql = "DELETE FROM {tb} WHERE {id_col} IN {ids}".format(tb=table, id_col=col, ids=ids_to_del)

            print("Del:", table)
            engine_base.execute(sql)

        return True

    def _logical_del(self, ids_to_del):
        if not ids_to_del:
            return False

        ids_to_del = sf.SQL.values4sql(ids_to_del)
        for table in self.TABLE_LOGICAL_DEL[self._id_type]:
            if table == "config_private.sync_source":
                sql = "UPDATE {tb} SET is_used = 0 WHERE {id_col} IN {ids}".format(tb=table, id_col="pk",
                                                                                   ids=ids_to_del)
            else:
                sql = "UPDATE {tb} SET is_used = 0 WHERE {id_col} IN {ids}".format(tb=table, id_col=self.ID_COLUMN,
                                                                                   ids=ids_to_del)
            print("Mark:", table)
            engine_base.execute(sql)

        return True

    def _deprecate_in_idmatch(self, ids_to_dep):
        # 更新id_match, 设置为不使用
        if not ids_to_dep:
            return False

        ids_to_dep = sf.SQL.values4sql(ids_to_dep)
        sql = "UPDATE base.id_match im SET im.is_used = 0, im.is_del = 1 WHERE im.matched_id IN {ids} AND id_type={id_type}".format(
            id_col=self.ID_COLUMN, id_type=self._id_type, ids=ids_to_dep)
        engine_base.execute(sql)
        return True

    def delete(self):
        if self._consistency_check:
            ids_whole = sorted(set([*self.get_ids_not_used(), *self._ids_to_del]))
        else:
            ids_whole = sorted(set(self._ids_to_del))

        self._logical_del(ids_whole)
        self._physical_del(ids_whole)
        self._deprecate_in_idmatch(self._ids_to_del)


class ParDelHelper(DelHelper):
    # 需要engine指定为base库
    TABLE_PHYSICAL_DEL = {
        1: (
            "config_private.sync_source",

            "fund_allocation_data", "fund_asset_scale", "fund_change_data", "fund_fee_data", "fund_info",
            "fund_info_aggregation", "fund_info_fundaccount", "fund_info_futures", "fund_info_private",
            "fund_info_securities",
            "fund_info_subsidiary", "fund_manager_mapping", "fund_month_indicator", "fund_month_return",
            "fund_month_risk",
            "fund_month_risk2", "fund_nv_data_source", "fund_nv_data_source_copy2", "fund_nv_data_standard",
            "fund_nv_data_standard_copy2",
            "fund_nv_standard_m", "fund_nv_standard_w", "fund_org_mapping", "fund_portfolio", "fund_security_data",
            "fund_subsidiary_month_index", "fund_subsidiary_month_index2", "fund_subsidiary_month_index3",
            "fund_subsidiary_weekly_index", "fund_subsidiary_weekly_index2",
            "fund_subsidiary_weekly_index3" , "fund_weekly_indicator", "fund_weekly_return",
            "fund_weekly_risk", "fund_weekly_risk2"
        ),

        2: (
            "org_executive_info", "org_integrity", "org_monthly_research", "org_monthly_return", "org_monthly_risk",
            "org_scale_type", "org_info", "fund_org_mapping", "org_person_mapping"
        ),

        3: (
            "base.person_info", "base.org_person_mapping", "base.person_description", "base.fund_manager_mapping"
        )
    }


class ModHelper:
    # engine 需要试用base库
    # MODLIST表中的数据【不应该】出现在DelHelper表中的TABLE_LOGICAL_DEL列表中
    MOD_NOT_KEPP_LIST = {
        1: (
            # 配置表
            "config_private.sync_source",

            # 基础库表
            "fund_allocation_data", "fund_asset_scale", "fund_change_data", "fund_fee_data", "fund_info",
            "fund_info_aggregation", "fund_info_fundaccount", "fund_info_futures", "fund_info_private",
            "fund_info_securities",
            "fund_info_subsidiary", "fund_manager_mapping", "fund_month_indicator", "fund_month_return",
            "fund_month_risk",
            "fund_month_risk2", "fund_nv_data_source", "fund_nv_data_source_copy2", "fund_nv_data_standard",
            "fund_nv_data_standard_copy2",
            "fund_nv_standard_m", "fund_nv_standard_w", "fund_org_mapping", "fund_portfolio", "fund_security_data",
            "fund_subsidiary_month_index", "fund_subsidiary_month_index2", "fund_subsidiary_month_index3",
            "fund_subsidiary_weekly_index", "fund_subsidiary_weekly_index2",
            "fund_subsidiary_weekly_index3", "fund_weekly_indicator", "fund_weekly_return",
            "fund_weekly_risk", "fund_weekly_risk2", "fund_rank",
            # "fund_type_mapping", 移除重复基金时, 由于表主键有更新时间, 所以;
        ),
        2: (
            "crawl_private.y_org_info",

            "org_executive_info", "org_integrity", "org_monthly_research", "org_monthly_return", "org_monthly_risk",
            "org_scale_type", "org_info", "fund_org_mapping", "manager_info", "org_person_mapping"
        ),

        3: ("base.person_info", "base.org_person_mapping", "base.person_description", "base.fund_manager_mapping")
    }

    MOD_AND_KEEP_LIST = {
        # 采集库表
        1: ("id_match", "crawl_private.y_fund_info", "crawl_private.y_fund_nv", "crawl_private.g_fund_nv",),

        2: ("id_match",),

        3: ("id_match",)

    }

    def __init__(self, id_type, mapper):
        """

        Args:
            id_type: int, optional {1, 2, 3}
                1: 基金; 2: 机构; 3: 人员;
            mapper: dict
                e.g. {id_wrong: id_right, ...}

        """

        self._mapper = {}
        self._id_type = id_type
        self.ID_COLUMN = {1: "`fund_id`", 2: "`org_id`", 3: "`person_id`"}[id_type]

        # initialize mapper<right: [wrong_id1, wonrg_id2, ...,]>
        [self._mapper.setdefault(right, []).append(wrong) for wrong, right in mapper.items()]

    @property
    def wrong_ids_duplicated(self):
        wrong_list = []
        for val in self._mapper.values():
            wrong_list.extend(val)
        return sorted(set(wrong_list))

    def _merge_records(self, table, id_right, ids_wrong, engine):
        pks = get_pk(table, engine)
        if len(pks) == 1:
            sql_right = "SELECT * FROM {tb} WHERE {id_col} = '{id_right}'".format(
                tb=table, id_col=self.ID_COLUMN, id_right=id_right
            )
            sql_wrong = "SELECT * FROM {tb} WHERE {id_col} IN {ids_wrong}".format(
                tb=table, id_col=self.ID_COLUMN, ids_wrong=ids_wrong
            )
            df_right, df_wrong = [pd.read_sql(sql, engine) for sql in (sql_right, sql_wrong)]

            df_right = df_right.applymap(replace_abnormalstr)  # 将空字符, 异常字符视作空值;
            df_res = df_right.append(df_wrong).fillna(method="bfill")[:1]

            sql_del = "DELETE FROM {tb} WHERE {id_col} IN {ids_wrong}".format(
                tb=table, id_col=self.ID_COLUMN, ids_wrong=ids_wrong
            )

            io.to_sql(table, engine, df_res, type="update")  # 将合并后的结果更新至原表
            engine.execute(sql_del)

        else:
            sql_upt = "UPDATE IGNORE {tb} SET {id_col} = '{id_right}' WHERE {id_col} IN {ids_wrong}".format(
                tb=table, id_col=self.ID_COLUMN, id_right=id_right, ids_wrong=ids_wrong
            )
            engine.execute(sql_upt)

    def _migrate_data(self, ids_to_mig):
        """

        Args:
            ids_to_mig: dict<str: list>
                <id_right: [id_wrong1, id_wrong2, ...,]>

        Returns:

        """
        for id_right, ids_wrong in ids_to_mig.items():
            ids_wrong = sf.SQL.values4sql(ids_wrong)

            for table in self.MOD_NOT_KEPP_LIST.get(self._id_type, []):
                # 使用忽略更新, 遇到重复主键无法更新时, 忽略这些id, 并交由DelHelper类处理.
                if table == "config_private.sync_source":
                    sql_upt = "UPDATE IGNORE {tb} SET {id_col} = '{id_right}' WHERE {id_col} IN {ids_wrong}".format(
                        tb=table, id_col="`pk`", id_right=id_right, ids_wrong=ids_wrong
                    )
                    engine_base.execute(sql_upt)

                elif table == "id_match":
                    sql_upt = "UPDATE IGNORE {tb} SET {id_col} = '{id_right}' WHERE {id_col} IN {ids_wrong}".format(
                        tb=table, id_col="`matched_id`", id_right=id_right, ids_wrong=ids_wrong
                    )
                    engine_base.execute(sql_upt)

                else:
                    self._merge_records(table, id_right, ids_wrong, engine_base)

                print("Mov:", table)

            for table in self.MOD_AND_KEEP_LIST.get(self._id_type, []):
                if table == "config_private.sync_source":
                    col = "pk"

                elif table == "id_match":
                    col = "matched_id"

                else:
                    col = self.ID_COLUMN.replace("`", "")

                sql_upt = "SELECT * FROM {tb} WHERE {id_col} IN {ids_wrong}".format(
                    tb=table, id_col=col, id_right=id_right, ids_wrong=ids_wrong
                )
                df = pd.read_sql(sql_upt, engine_base)
                df[col] = id_right

                io.to_sql(table, engine_base, df, type="ignore")  # MAY BE WRONG

        return True

    def migrate(self):
        self._migrate_data(self._mapper)

        dh = DelHelper(id_type=self._id_type, ids_to_del=self.wrong_ids_duplicated, consistency_check=False)
        dh.delete()


class PartialDelHelper:
    def __init__(self, fund_id, wrong_source, wrong_source_id, **kwargs):
        """

        Args:
            fund_id: str
                基金ID
            wrong_source: str
                错误的数据源
            wrong_source_id: str
                错误的数据源上的基金ID

        """

        self.fund_id = fund_id
        self.wrong_source = wrong_source
        self.wrong_source_id = wrong_source_id
        self.original_data = {}

        self.engine = kwargs.get("engine", engine_base)

    def fetch_original_data(self):
        sql_id_match = "SELECT id_type, matched_id, source, source_id, is_used, is_del, entry_time, update_time " \
                       "FROM base.id_match " \
                       "WHERE matched_id ='{fid}'".format(fid=self.fund_id)
        sql_sync_source = "SELECT * FROM config_private.sync_source WHERE pk = '{pk}'".format(pk=self.fund_id)

        self.original_data["id_match"] = pd.read_sql(sql_id_match, self.engine)
        self.original_data["sync_source"] = pd.read_sql(sql_sync_source, self.engine)

    def del_wrongdata(self):
        dh = ParDelHelper(1, [self.fund_id], False)  # id_type为1(基金)
        dh.delete()

    def _modify_id_match(self):
        # 将错误源id_match表记录is_used设置为0, is_del=1
        self.original_data["id_match"].loc[(self.original_data["id_match"].source == self.wrong_source) & (self.original_data["id_match"].source_id == self.wrong_source_id), "is_used"] = 0
        self.original_data["id_match"].loc[(self.original_data["id_match"].source == self.wrong_source) & (self.original_data["id_match"].source_id == self.wrong_source_id), "is_del"] = 1
        self.original_data["id_match"]["update_time"] = dt.datetime.now()

    def write_original_data(self):
        self._modify_id_match()
        io.to_sql("base.id_match", self.engine, self.original_data["id_match"])
        io.to_sql("config_private.sync_source", self.engine, self.original_data["sync_source"])

    def delete(self):
        self.fetch_original_data()
        self.del_wrongdata()
        self.write_original_data()


def test_mod():
    ID_TYPE = 1

    WRONG_RIGHT = {
    }

    mh = ModHelper(ID_TYPE, WRONG_RIGHT)
    mh.migrate()


def test_del(fund_id):
    ID_TYPE = 1

    WRONG_IDS = [fund_id]

    dh = DelHelper(ID_TYPE, WRONG_IDS, False)
    dh.delete()


def test_partialdel():
    fund_id = "JR082558"
    wrong_src = "020002"
    wrong_srcid = "05222225BV"

    pdh = PartialDelHelper(fund_id, wrong_src, wrong_srcid)
    pdh.delete()
