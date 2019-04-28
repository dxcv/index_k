import pandas as pd
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from utils.database.models.crawl_public import DPersonInfo
from utils.database.models.base_public import IdMatch
from utils.database import io

engine_rd = create_engine("mysql+pymysql://root:smyt0317@58cb57c164977.sh.cdb.myqcloud.com:4171", echo=True,
                          connect_args={"charset": "utf8"})
engine_wt = create_engine("mysql+pymysql://root:smyt0317@58cb57c164977.sh.cdb.myqcloud.com:4171/{schema}".format(
    schema=IdMatch.__table_args__["schema"]), echo=True, connect_args={"charset": "utf8"})
db_session = sessionmaker(bind=engine_rd)
session = db_session()


# Fetch all org info from crawl_public.d_org_info, base_public.org_info
def fetch_unmatched_person(data_source):
    fields = [IdMatch.source_id, IdMatch.matched_id, DPersonInfo.person_id]

    query_pi = session.query(IdMatch).outerjoin(
        IdMatch, and_(IdMatch.source_id == DFundInfo.fund_id, IdMatch.data_source == DFundInfo.data_source, IdMatch.id_type == 1)
    ).filter(
        DFundInfo.data_source == data_source
    ).with_entities(
        *fields
    )
    df = pd.DataFrame(query_pi.all())
    df_unmatched = df.ix[df[IdMatch.matched_id.name].apply(lambda x: x is None)]
    df_unmatched.index = range(len(df_unmatched))
    return df_unmatched


def match(data_source):
    unmatched = fetch_unmatched_person(data_source)
    result = pd.DataFrame()
    result[IdMatch.source_id.name] = result[IdMatch.matched_id.name] = unmatched[DFundInfo.fund_id.name]
    result[IdMatch.data_source.name] = data_source
    result[IdMatch.id_type.name] = 1
    result[IdMatch.accuracy.name] = 1

    return result


def main():
    data_sources = ["020001", "020002", "020003"]
    for data_source in data_sources:
        print(data_source)
        print(match(data_source))
        io.to_sql(IdMatch.__tablename__, engine_wt, match(data_source))


if __name__ == "__main__":
    main()


# FIRST TIME INITIALIZATION -PERSON INFO
import re
query = session.query(DPersonInfo).filter(DPersonInfo.data_source=="020001").with_entities(DPersonInfo.person_id, DPersonInfo.person_name, DPersonInfo.data_source, DPersonInfo.master_strategy, DPersonInfo.gender, DPersonInfo.education, DPersonInfo.investment_period)
q = pd.DataFrame(query.all())
q = q.sort_values("person_id")
q["master_strategy"] = q["master_strategy"].apply(lambda x: x if x in {"股票型", "混合型", "债券型", "货币型", "QDII", "保本型", "指数型", "封闭式", "理财型", "结构型"} else None)
q["gender"] = q["gender"].apply(lambda x: x if x in {"男", "女"} else None)
# q["source_id"] = q["person_id"]
q["id_type"] = 3
q["accuracy"] = 1
q["source_id"] = q["person_id"]
q1 = q.ix[q["person_id"].apply(lambda x: x[0] != "8")]

q2 = q.ix[q["person_id"].apply(lambda x: x[0] == "8")]
q2["person_id"] = q2["person_id"].apply(lambda x: re.search("(?P<oid>\d*)", x).groupdict().get("oid"))
# id_match

q1["matched_id"] = ["01" + (6-len(str(i))) * "0" + str(i) for i in range(1, len(q1)+1, 1)]
q1 = q1[["data_source", "id_type", "source_id", "matched_id", "accuracy"]]

q2["matched_id"] = ["02" + (6-len(str(i))) * "0" + str(i) for i in range(1, len(q2)+1, 1)]
q2 = q2[["data_source", "id_type", "source_id", "matched_id", "accuracy"]]

io.to_sql("id_match", engine_wt, q1)
io.to_sql("id_match", engine_wt, q2)


import hashlib as h
engine_rd = create_engine(
    "mysql+pymysql://root:smyt0317@182.254.128.241:4171/crawl_public",
    echo=True, connect_args={"charset": "utf8"}
)
sql = "SELECT * FROM d_person_info WHERE data_source = '020001' AND person_id LIKE '8%%'"
q = pd.read_sql(sql, engine_rd)
q["person_id"] = q["person_id"].apply(lambda x: h.md5(x.encode("utf8")).hexdigest())
engine_rd.execute("DELETE FROM d_person_info WHERE data_source = '020001' AND person_id LIKE '8%%'")
q.to_sql("d_person_info", engine_rd, index=False, if_exists="append")
io.to_sql("d_person_info", engine_rd, q)

sql = "SELECT * FROM d_org_person WHERE data_source = '020001' AND person_id LIKE '8%%'"
q = pd.read_sql(sql, engine_rd)
q["person_id"] = q["person_id"].apply(lambda x: h.md5(x.encode("utf8")).hexdigest())
engine_rd.execute("DELETE FROM d_org_person WHERE data_source = '020001' AND person_id LIKE '8%%'")
q.to_sql("d_org_person", engine_rd, index=False, if_exists="append")

