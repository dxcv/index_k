import datetime as dt
import pandas as pd
from sqlalchemy import and_, distinct
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.database.models.base_public import IdMatch, FundInfo, FundNvSource, FundNv
from utils.database.models.config import ConfigSource

_engine_cfg = cfg.load_engine()["2Gcfg"]

_db_session = sessionmaker(bind=_engine_cfg)
_session = _db_session()


class SourceAdjustor:
    def __init__(self, table):
        self._table = table

    def configured_obk(self):
        configured_obj = _session.query(ConfigSource).filter(
            and_(ConfigSource.table_ == self._table.__tablename__)
        ).with_entities(
            ConfigSource.pk
        )
        configured_obj = set(x[0] for x in configured_obj.all())
        return sorted(configured_obj)

    def unconfigured_obj(self):
        """
        获取`config_source`表中未配置选用源的对象;

        Args:
            table: sqlalchemy.ext.declarative.api.DeclarativeMeta
                需要检查的表, 以sqlalchemy.ext.declarative.api.DeclarativeMeta类型传入, 需要定义__tablename__方法;

        Returns:

        """
        configured_obj = set(self.configured_obk())

        if self._table.__tablename__ == "fund_nv":
            all_obj = _session.query(FundNvSource).with_entities(
                FundNvSource.fund_id
            ).group_by(
                FundNvSource.fund_id
            )
            all_obj = set(x[0] for x in all_obj.all())

        unconfigured_obj = all_obj - configured_obj
        return sorted(unconfigured_obj)

    def new_obj(self, data_sources, method):
        """
        新增`config_source`配置表中的对象及配置;

        Args:
\            data_sources: list<str>
                e.g. ["020001", "020002", "020003"]
            method: int, optional {1, 2}
                1-多源整合; 2-依次选用单源;

        Returns:

        """
        result = pd.DataFrame()
        unconfigured = self.unconfigured_obj()
        result[ConfigSource.pk.name] = unconfigured
        result[ConfigSource.schema_.name] = self._table.__table_args__["schema"]
        result[ConfigSource.table_.name] = self._table.__tablename__
        result[ConfigSource.data_sources.name] = ",".join(data_sources)
        result[ConfigSource.method.name] = method
        io.to_sql(ConfigSource.__tablename__, _engine_cfg, result)

    def update_obj(self, pk, data_sources, method):
        """
            更新`config_source`表中对象的配置;

        Args:
            pk:
            data_sources:
            method:

        Returns:

        """
        result = pd.DataFrame()
        result[ConfigSource.pk.name] = pk
        result[ConfigSource.schema_.name] = self._table.__table_args__["schema"]
        result[ConfigSource.table_.name] = self._table.__tablename__
        result[ConfigSource.data_sources.name] = ",".join(data_sources)
        result[ConfigSource.method.name] = method
        io.to_sql(ConfigSource.__tablename__, _engine_cfg, result)


def main():
    SourceAdjustor(FundNv).new_obj(["020002", "020001", "020003"], method=1)


if __name__ == "__main__":
    main()

# @classmethod
# def checkkey(self, *keys, table, bind=ConfigSource):
#
#
#     query_all = _session.query(
#         table
#     ).group_by(
#         *keys
#     ).with_entities(
#         *keys
#     )
#
#     df_all = pd.DataFrame(query_all.all())
#
#     query_exist = _session.query(
#         bind
#     ).group_by(
#         *keys
#     ).with_entities(
#         *keys
#     )
#
#     q1 = _session.query(
#         DFundDividend
#     ).filter(
#         and_(DFundDividend.fund_id == "000001", DFundDividend.data_source == "020001")
#     ).with_entities(
#         DFundDividend.fund_id, DFundDividend.statistic_date
#     ).subquery()
#
#     q2 = _session.query(
#         DFundDividend
#     ).filter(
#         and_(DFundDividend.fund_id == "000001", DFundDividend.data_source == "020002")
#     ).with_entities(
#         DFundDividend.fund_id, DFundDividend.statistic_date
#     ).subquery()
#
#     q = _session.query(q1, q2).with_entities(
#         q1.c.statistic_date, q2.c.statistic_date
#     ).filter(
#         q1.statistic_date == q2.statistic_date
#     )
#
#     q = _session.query(FundNvSource).group_by(
#         DFundDividend.fund_id
#     ).with_entities(
#         DFundDividend.fund_id, DFundDividend.statistic_date
#     )
#
#     q.statement
#
# q = pd.read_sql("SELECT DISTINCT fund_id FROM fund_split where ")