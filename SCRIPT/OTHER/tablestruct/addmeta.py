import pandas as pd
from sqlalchemy import create_engine

DB = "crawl_private"
_engine_wt = create_engine("mysql+pymysql://root:smyt0317@182.254.128.241:4171/{db}?charset=utf8".format(db=DB))

tables = pd.read_sql("SHOW TABLES", _engine_wt)["Tables_in_{db}".format(db=DB)].tolist()


def execute_sqls(sqls, engine):
    for sql in sqls:
        try:
            engine.execute(sql)
        except Exception as e:
            print(e)


def addmeta_ud(tb, engine):
    sqls = (
        "ALTER TABLE {tb} ADD COLUMN `is_used` tinyint(1) unsigned DEFAULT '1' COMMENT '是否使用本条数据'".format(tb=tb),
        "ALTER TABLE {tb} ADD COLUMN `is_del` tinyint(1) unsigned DEFAULT '0' COMMENT '是否删除本条数据'".format(tb=tb)
    )
    execute_sqls(sqls, engine)


def add_tmcol(tb, engine):
    sqls = (
        "ALTER TABLE {tb} ADD COLUMN `entry_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库日期'".format(tb=tb),
        "ALTER TABLE {tb} ADD COLUMN `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'".format(tb=tb),
    )
    execute_sqls(sqls, engine)


def add_tmidx(tb, engine):
    sqls = (
        "ALTER TABLE {tb} ADD INDEX `idx_entry_time`(entry_time) USING BTREE".format(tb=tb),
        "ALTER TABLE {tb} ADD INDEX `idx_update_time`(update_time) USING BTREE".format(tb=tb)
    )
    execute_sqls(sqls, engine)


def mod_order(tb, engine):
    sqls = (
        "ALTER TABLE {tb} MODIFY COLUMN `entry_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库日期' AFTER `is_del`".format(tb=tb),
        "ALTER TABLE {tb} MODIFY COLUMN `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '入库日期' AFTER `entry_time`".format(tb=tb),
    )
    execute_sqls(sqls, engine)


def addmeta(tb, engine):
    addmeta_ud(tb, engine)
    add_tmcol(tb, engine)
    add_tmidx(tb, engine)
    mod_order(tb, engine)

for table in tables:
    addmeta(table, _engine_wt)
