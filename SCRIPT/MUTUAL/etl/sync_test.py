import datetime as dt
import pandas as pd
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from utils.database import io
from utils.database.models.crawl_public import DOrgInfo
from utils.database.models.base_public import IdMatch

engine_cp = create_engine("mysql+pymysql://root:smyt0317@58cb57c164977.sh.cdb.myqcloud.com:4171/crawl_public", echo=True, connect_args={"charset": "utf8"})
db_session_cp = sessionmaker(bind=engine_cp)
session_cp = db_session_cp()

engine_bp = create_engine("mysql+pymysql://root:smyt0317@58cb57c164977.sh.cdb.myqcloud.com:4171/base_public", echo=True, connect_args={"charset": "utf8"})
db_session_bp = sessionmaker(bind=engine_bp)
session_bp = db_session_bp()

engine = create_engine("mysql+pymysql://root:smyt0317@58cb57c164977.sh.cdb.myqcloud.com:4171", echo=True, connect_args={"charset": "utf8"})
db_session = sessionmaker(bind=engine)
session = db_session()

query_oi = session.query(
    IdMatch.source_id, DOrgInfo.org_id, DOrgInfo.org_type_code, IdMatch.update_time, DOrgInfo.org_name, DOrgInfo.org_full_name, DOrgInfo.data_source
).join(
    DOrgInfo, and_(IdMatch.source_id == DOrgInfo.org_id, IdMatch.data_source == DOrgInfo.data_source)
)


query_oi_crawl = session.query(DOrgInfo.org_id, DOrgInfo.org_name, DOrgInfo.org_full_name, DOrgInfo.data_source).filter(DOrgInfo.update_time >= dt.date(2016, 12, 31))
df = pd.DataFrame(query_oi.all())

query_oi_matched = session.query(IdMatch.matched_id, IdMatch.source_id, )

query_oi = session.query()
query_oi = session_cp.query(IdMatch.source_id, IdMatch.update_time).join(DOrgInfo, IdMatch.source_id == DOrgInfo.org_id).filter(IdMatch.matched_id == "01000001")
query_oi.all()


# init_data org ids in id_match
# query org_type_code
query_otc = session_cp.query(
    DOrgInfo.org_type_code
).group_by(
    DOrgInfo.org_type_code
)


org_type_codes = pd.read_sql(query_otc.statement, query_otc.session.bind)[DOrgInfo.org_type_code.name].tolist()

# query org_type_code
source_code = 2  # 三方源
data_source_code = 2  # 天天
id_type = 2
query_oi = session_cp.query(
    DOrgInfo.org_id, DOrgInfo.org_name, DOrgInfo.org_type_code, DOrgInfo.org_full_name, DOrgInfo.foundation_date
).filter(
    DOrgInfo.data_source == data_source_code, DOrgInfo.org_type_code.in_(org_type_codes)
)
df = pd.DataFrame(query_oi.all()).sort_values(by=[DOrgInfo.org_type_code.name, DOrgInfo.foundation_date.name])

for otc in org_type_codes:
    init_ids = [(2-len(str(otc))) * "0" + str(otc) + (6 - len(str(i))) * "0" + str(i) for i in range(1, len(df.ix[df[DOrgInfo.org_type_code.name]==otc]) + 1)]
    df.ix[df[DOrgInfo.org_type_code.name] == otc, "matched_id"] = init_ids
df[IdMatch.data_source.name] = source_code
df[IdMatch.data_source.name] = data_source_code
df[IdMatch.source_id.name] = df[DOrgInfo.org_id.name]
df[IdMatch.accuracy.name] = 1
df[IdMatch.id_type.name] = id_type
df = df.ix[:, [col.name for col in IdMatch.__table__.c._all_columns]]

io.to_sql(IdMatch.__tablename__, session_bp.bind, df)

# DataFrame Style
query_oi = session_cp.query(
    DOrgInfo.org_id, DOrgInfo.org_name, DOrgInfo.org_full_name, DOrgInfo.foundation_date
).filter(
    DOrgInfo.data_source == 1, DOrgInfo.org_type_code == 1
)


query_oi = session_cp.query(DOrgInfo).join(
    IdMatch, DOrgInfo.org_id == IdMatch.source_id
).filter(
    IdMatch.source_id=="80000080"
)

df = pd.read_sql(query_oi.statement, query_oi.session.bind).sort_values(by="foundation_date")
ids_matched = ["P" + "0" * (7-len(str(i))) + str(i) for i in range(1, len(df)+1)]


query_oi = session_bp.query(
    IdMatch.matched_id, IdMatch.data_source, IdMatch.data_source, IdMatch.source_id
).filter(
    IdMatch.id_type == 2
)


df_im = pd.read_sql(query_oi.statement, query_oi.session.bind)


ids = df_im.matched_id.tolist()
