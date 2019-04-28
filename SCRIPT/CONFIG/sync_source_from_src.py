from utils.database import config as cfg, io
from utils.database.models.base_private import FundNvDataStandard, FundNvDataSource
from sqlalchemy.orm import sessionmaker
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta

TO = dt.datetime.now()
FROM = TO - relativedelta(minutes=7)


class SyncInitializer:
    INIT_FROM = {
        FundNvDataStandard.__tablename__: (FundNvDataSource.__tablename__,)
    }

    CONFIG_TABLE = "config_private.sync_source"

    ENGINE = cfg.load_engine()["2Gb"]

    def __init__(self, target_table, default_prio=1):
        self._target_table = target_table
        splitted = target_table.split(".")
        self._target_table_name = splitted[-1]
        if len(splitted) == 2:
            self._target_table_db = splitted[0]
        self._default_prio = default_prio
        self._cfg = pd.DataFrame()

    def _constructor(self):
        if self._target_table_name == "fund_nv_data_standard":
            dbsession = sessionmaker()
            session = dbsession(bind=self.ENGINE)
            stmt = session.query(FundNvDataSource).filter(
                FundNvDataSource.is_used == 1, FundNvDataSource.source_id.notin_({"04", "05"}), FundNvDataSource.update_time.between(FROM, TO)
            ).group_by(
                FundNvDataSource.fund_id, FundNvDataSource.source_id
            ).with_entities(
                FundNvDataSource.fund_id.label("pk"), FundNvDataSource.source_id.label("source_id")
            )
            df = pd.DataFrame(stmt.all())
        return df

    def _load_default_cfg(self):
        if len(self._cfg) == 0:
            return
        self._cfg = self._cfg.drop_duplicates()
        self._cfg["priority"] = self._default_prio
        self._cfg["target_table"] = self._target_table_name

    def initialize(self):
        self._cfg = self._cfg.append(self._constructor())
        self._load_default_cfg()

    def update_syncsource(self):
        io.to_sql(self.CONFIG_TABLE, self.ENGINE, self.cfg, type="ignore")  # `type` must be ignore

    @property
    def cfg(self):
        return self._cfg


def main():
    si = SyncInitializer("base.fund_nv_data_standard")
    si.initialize()
    si.update_syncsource()


if __name__ == "__main__":
    main()
