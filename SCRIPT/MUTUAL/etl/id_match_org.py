import pandas as pd
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from utils.algorithm import etl
from utils.database.models.crawl_public import DOrgInfo
from utils.database.models.base_public import IdMatch, OrgInfo
from functools import partial
from utils.database import io

engine_rd = create_engine("mysql+pymysql://root:smyt0317@58cb57c164977.sh.cdb.myqcloud.com:4171", echo=True,
                          connect_args={"charset": "utf8"})
engine_wt = create_engine("mysql+pymysql://root:smyt0317@58cb57c164977.sh.cdb.myqcloud.com:4171/{schema}".format(
    schema=OrgInfo.__table_args__["schema"]), echo=True, connect_args={"charset": "utf8"})
db_session = sessionmaker(bind=engine_rd)
session = db_session()


# Fetch all org info from crawl_public.d_org_info, base_public.org_info
def fetch_unmatched_org(data_source):
    fields = [IdMatch.source_id, DOrgInfo.org_id, IdMatch.matched_id, DOrgInfo.org_name, DOrgInfo.org_full_name,
              DOrgInfo.legal_person, DOrgInfo.foundation_date, DOrgInfo.data_source, DOrgInfo.org_type_code]

    query_doi = session.query(DOrgInfo).outerjoin(
        IdMatch, and_(IdMatch.source_id == DOrgInfo.org_id, IdMatch.data_source == DOrgInfo.data_source, IdMatch.id_type == 2)
    ).filter(
        and_(DOrgInfo.data_source == data_source, IdMatch.is_used == 1)
    ).with_entities(
        *fields
    ).order_by(
        DOrgInfo.data_source
    )
    df = pd.DataFrame(query_doi.all())
    df_unmatched = df.ix[df[IdMatch.matched_id.name].apply(lambda x: x is None)]
    df_unmatched.index = range(len(df_unmatched))
    return df_unmatched


def fetch_matched_org():
    fields = [IdMatch.matched_id, OrgInfo.org_id, OrgInfo.org_name, OrgInfo.org_full_name, OrgInfo.legal_person,
              OrgInfo.foundation_date, IdMatch.data_source, IdMatch.data_source, OrgInfo.org_type_code]

    query_oi = session.query(IdMatch).outerjoin(
        OrgInfo, and_(IdMatch.matched_id == OrgInfo.org_id, IdMatch.id_type == 2)
    ).join(
        DOrgInfo, and_(IdMatch.source_id == DOrgInfo.org_id, IdMatch.data_source == DOrgInfo.data_source, IdMatch.id_type == 2)
    ).with_entities(
        *fields
    )
    df_oi = pd.DataFrame(query_oi.all())
    return df_oi


def new_oid(org_type_code, num):
    fields = [IdMatch.matched_id]
    query_oi = session.query(IdMatch).join(
        OrgInfo, and_(IdMatch.matched_id == OrgInfo.org_id, IdMatch.id_type == 2)
    ).filter(
        OrgInfo.org_type_code == org_type_code
    ).with_entities(
        *fields
    )
    oid_max = max([int(x[0]) for x in query_oi.all()])
    new_oids = [oid_max + i for i in range(1, num + 1)]
    new_oids = ["0" * (8 - len(str(oid))) + str(oid) for oid in new_oids]
    return new_oids


class Fields:
    def __init__(self, fields: list):
        self.fields = fields
        self.fields_name = [x.name for x in self.fields]


# data_source = "020001"
# data_source = "020002"
# data_source = "020003"


def compare_dkt(lc, attr_dict: dict, field, method, num=1):
    result = []
    for k, v in attr_dict.items():
        result.append((k, method(lc, v[field])))
    if num == 1:
        return sorted(result, key=lambda x: x[1])[::-1][0]
    else:
        return sorted(result, key=lambda x: x[1])[::-1][:num]


def affinity(*affinity_mtx, weights):
    result = []
    record_num = len(affinity_mtx[0])
    for i in range(record_num):
        tmp = {}
        for mtx, weight in zip(affinity_mtx, weights):
            for likeness_tuple in mtx[i]:
                tmp[likeness_tuple[0]] = tmp.get(likeness_tuple[0], 0) + likeness_tuple[1] * weight
        result.append(tmp)
    return result


def reduce(similarity_list):
    original = similarity_list.copy()
    threshold = 0.85
    confident_match = {x[0][0]: x[0][1] for x in original if len(x) == 1 and x[0][1] >= threshold}
    for i in range(len(original)):
        if len(original[i]) >= 2:
            tmp = tuple([x for x in original[i] if x[0] not in confident_match and x[1] <= confident_match.get(x[0], threshold)])
            original[i] = tmp
    if original == original:
        return original
    else:
        return reduce(original)


def match(data_source):
    unmatched = fetch_unmatched_org(data_source)
    matched = fetch_matched_org()

    key_fields = Fields([OrgInfo.org_id])
    value_fields = Fields([OrgInfo.org_name, OrgInfo.org_full_name, OrgInfo.foundation_date, OrgInfo.legal_person])
    d_matched = etl.generate_attr_dict(matched, key_fields.fields_name, value_fields.fields_name)

    mtx1 = unmatched["org_name"].apply(
        lambda x: compare_dkt(x, d_matched, "org_name", partial(etl.Similarity.similarity_name, ignore=["股份", "有限", "公司"]), 5)
    ).as_matrix()

    mtx2 = unmatched["org_full_name"].apply(
        lambda x: compare_dkt(x, d_matched, "org_full_name", partial(etl.Similarity.similarity_name, ignore=["股份", "有限", "公司"]), 5)
    ).as_matrix()

    mtx3 = unmatched["org_name"].apply(
        lambda x: compare_dkt(x, d_matched, "org_full_name", partial(etl.Similarity.similarity_name, ignore=["股份", "有限", "公司"]), 5)
    ).as_matrix()

    mtx4 = unmatched["foundation_date"].apply(
        lambda x: compare_dkt(x, d_matched, "foundation_date", etl.Similarity.similarity_time, 5)
    ).as_matrix()

    affinity_comprehensive = affinity(mtx1, mtx2, mtx3, mtx4, weights=[0.05, 0.7, 0.1, 0.15])
    likelihood = []
    for dictionary in affinity_comprehensive:
        tmp = tuple([k, v] for k, v in dictionary.items() if v == max(dictionary.values()))
        likelihood.append(tmp)

    unmatched["q"] = likelihood

    maximum_likelihood = reduce(likelihood)
    unmatched["q"] = maximum_likelihood
    unmatched[IdMatch.matched_id.name] = unmatched["q"].apply(lambda x: x[0][0] if len(x) >= 1 else None)
    unmatched[IdMatch.accuracy.name] = unmatched["q"].apply(lambda x: x[0][1] if len(x) >= 1 else None)
    unmatched[IdMatch.source_id.name] = unmatched["org_id"]
    unmatched[IdMatch.data_source.name] = data_source
    unmatched[IdMatch.id_type.name] = 2

    matched_items = unmatched[Fields([IdMatch.id_type, IdMatch.matched_id, IdMatch.data_source, IdMatch.source_id, IdMatch.accuracy, DOrgInfo.org_type_code]).fields_name]
    existed = matched_items.ix[matched_items[IdMatch.accuracy.name].apply(lambda x: round(x, 6) >= 0.8)]
    # newly increased
    new = matched_items.ix[~(matched_items[IdMatch.accuracy.name].apply(lambda x: round(x, 6) >= 0.8))]

    new.ix[new[OrgInfo.org_type_code.name] == 1, IdMatch.matched_id.name] = new_oid(1, len(new.ix[new[OrgInfo.org_type_code.name] == 1]))
    new.ix[new[OrgInfo.org_type_code.name] == 2, IdMatch.matched_id.name] = new_oid(1, len(new.ix[new[OrgInfo.org_type_code.name] == 2]))

    output_fields = Fields([IdMatch.id_type, IdMatch.matched_id, IdMatch.data_source, IdMatch.source_id, IdMatch.accuracy])
    return existed[output_fields.fields_name], new[output_fields.fields_name]


def main():
    data_sources = ["020001", "020002", "020003"]
    result = {"existed": pd.DataFrame(), "new": pd.DataFrame()}
    for data_source in data_sources:
        existed, new = match(data_source)
        result["existed"] = result["existed"].append(existed)
        result["new"] = result["new"].append(new)

    if len(result["existed"]) >= 0:
        io.to_sql("id_match", engine_wt, result["existed"])
    if len(result["new"]) >= 0:
        io.to_sql("id_match", engine_wt, result["new"])


if __name__ == "__main__":
    main()

