import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput

ENGINE_RD = cfg.load_engine()["2Gb"]


class StreamsMain:

    """
        清洗d_fund_info

    """

    form_dict = {'国有企业': '中资企业(国有)',
                 '中资企业': '中资企业',
                 '中外合资企业': '合资企业',
                 '国有相对控股企业': '中资企业(国有)',
                 '民营企业': '中资企业(民营)',
                 '民营相对控股企业': '中资企业(民营)',
                 '合资企业': '合资企业'}

    @classmethod
    def sub_wrong_to_none(cls, x):
        s = re.sub("\s| |--|---|亿元", "", x)
        if s == "":
            return None
        else:
            return s

    @classmethod
    def sub_wrong_to_none2(cls, x):
        s = re.sub("\s| |-|--|---|亿元", "", x)
        if s == "":
            return None
        else:
            return s

    @classmethod
    def stream_020001(cls):

        sql = "select idm.matched_id,doi.org_id as org_id2, doi.org_name, doi.org_full_name, doi.org_type_code, \
                doi.foundation_date, doi.form, doi.scale, doi.legal_person, doi.chairman, doi.org_name_en, \
                doi.general_manager, doi.reg_capital, doi.reg_address, doi.address, doi.tel, \
                doi.fax, doi.email, doi.website, doi.tel_service FROM \
                (SELECT matched_id, source_id from base_public.id_match WHERE id_type = 2 \
                AND is_used = 1 AND data_source = '020001') AS idm \
                JOIN crawl_public.d_org_info AS doi \
                ON idm.source_id = doi.org_id \
                WHERE doi.data_source = '020001' AND doi.org_type_code = 1"

        inp = MysqlInput(ENGINE_RD, sql)


        vm = transform.ValueMap({
            "form": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "scale": lambda x: cls.sub_wrong_to_none2(x) if type(x) is str else x,
            "reg_capital": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "reg_address": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "address": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "tel": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fax": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "email": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "website": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "tel_service": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "legal_person": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "chairman": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "general_manager": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x

        })


        vm2 = transform.ValueMap({
            "form": lambda x: cls.form_dict.get(x) if type(x) is str else x,
            "reg_capital": lambda x: float(x) if type(x) is str else x,
            "scale": lambda x: float(x) if type(x) is str else x,
            "org_type": "公募基金公司"
        })

        vm3 = transform.ValueMap({
            "reg_capital": lambda x: None if type(x) is float and x < 0.0000001 else x,
            "scale": lambda x: None if type(x) is float and x < 0.0000001 else x
        })

        st = transform.MapSelectKeys({
            'matched_id': 'org_id',
            'org_name': 'org_name',
            'data_source': 'data_source',
            'org_name_en': 'org_name_en',
            'org_full_name': 'org_full_name',
            'org_type_code': 'org_type_code',
            'org_type': 'org_type',
            'foundation_date': 'foundation_date',
            'form': 'form',
            'scale': 'scale',
            'legal_person': 'legal_person',
            'chairman': 'chairman',
            'general_manager': 'general_manager',
            'reg_capital': 'reg_capital',
            'reg_address': 'reg_address',
            'address': 'address',
            'tel': 'tel',
            'fax': 'fax',
            'email': 'email',
            'website': 'website',
            'tel_service': 'tel_service'

        })
        r = Stream(inp, transform=[vm, vm2, vm3, st])
        return r

    @classmethod
    def stream_020002(cls):

        sql = "select idm.matched_id,doi.org_id as org_id2, doi.org_name, doi.org_full_name, doi.org_type_code, \
                doi.foundation_date, doi.form, doi.scale_mgt, doi.legal_person, doi.chairman, doi.org_name_en, \
                doi.general_manager, doi.reg_capital, doi.reg_address, doi.address, doi.tel, \
                doi.fax, doi.email, doi.website, doi.tel_service FROM \
                (SELECT matched_id, source_id from base_public.id_match WHERE id_type = 2 \
                AND is_used = 1 AND data_source = '020002') AS idm \
                JOIN crawl_public.d_org_info AS doi \
                ON idm.source_id = doi.org_id \
                WHERE doi.data_source = '020002' AND doi.org_type_code = 1"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "form": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "scale_mgt": lambda x: cls.sub_wrong_to_none2(x) if type(x) is str else x,
            "reg_capital": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "reg_address": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "address": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "tel": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fax": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "email": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "website": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "tel_service": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "legal_person": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "chairman": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "general_manager": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,


        })

        vm2 = transform.ValueMap({
            "form": lambda x: cls.form_dict.get(x) if type(x) is str else x,
            "reg_capital": lambda x: float(x) if type(x) is str else x,
            "scale_mgt": lambda x: float(x) if type(x) is str else x,
            "org_type": "公募基金公司"

        })

        vm3 = transform.ValueMap({
            "reg_capital": lambda x: None if type(x) is float and x < 0.0000001 else x,
            "scale_mgt": lambda x: None if type(x) is float and x < 0.0000001 else x
        })

        st = transform.MapSelectKeys({
            'matched_id': 'org_id',
            'org_name': 'org_name',
            'org_name_en': 'org_name_en',
            'org_full_name': 'org_full_name',
            'org_type_code': 'org_type_code',
            'org_type': 'org_type',
            'foundation_date': 'foundation_date',
            'form': 'form',
            'scale_mgt': 'scale',
            'legal_person': 'legal_person',
            'chairman': 'chairman',
            'general_manager': 'general_manager',
            'reg_capital': 'reg_capital',
            'reg_address': 'reg_address',
            'address': 'address',
            'tel': 'tel',
            'fax': 'fax',
            'email': 'email',
            'website': 'website',
            'tel_service': 'tel_service',
        })
        r = Stream(inp, transform=[vm, vm2, vm3, st])
        return r

    @classmethod
    def stream_020003(cls):

        sql = "select idm.matched_id,doi.org_id as org_id2, doi.org_name, doi.org_full_name, doi.org_type_code, \
                doi.foundation_date, doi.form, doi.legal_person, doi.chairman, doi.org_name_en, \
                doi.general_manager, doi.reg_capital, doi.reg_address, doi.address, doi.tel, \
                doi.fax, doi.email, doi.website, doi.tel_service FROM \
                (SELECT matched_id, source_id from base_public.id_match WHERE id_type = 2 \
                AND is_used = 1 AND data_source = '020001') AS idm \
                JOIN crawl_public.d_org_info AS doi \
                ON idm.source_id = doi.org_id \
                WHERE doi.data_source = '020001' AND doi.org_type_code = 1"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "form": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "reg_capital": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "reg_address": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "address": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "tel": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fax": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "email": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "website": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "tel_service": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "legal_person": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "chairman": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "general_manager": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x
        })
        vm2 = transform.ValueMap({
            "reg_capital": lambda x: x/10e7,
            "form": lambda x: cls.form_dict.get(x) if type(x) is str else x
        })

        vm3 = transform.ValueMap({
            "reg_capital": lambda x: None if type(x) is float and x == 0 else x,
            "org_type": "公募基金公司"
        })

        st = transform.MapSelectKeys({
            'matched_id': 'org_id',
            'org_name': 'org_name',
            'org_name_en': 'org_name_en',
            'org_full_name': 'org_full_name',
            'org_type_code': 'org_type_code',
            'org_type': 'org_type',
            'foundation_date': 'foundation_date',
            'form': 'form',
            'scale': 'scale',
            'legal_person': 'legal_person',
            'chairman': 'chairman',
            'general_manager': 'general_manager',
            'reg_capital': 'reg_capital',
            'reg_address': 'reg_address',
            'address': 'address',
            'tel': 'tel',
            'fax': 'fax',
            'email': 'email',
            'website': 'website',
            'tel_service': 'tel_service'
        })
        r = Stream(inp, transform=[vm, vm2, vm3, st])
        return r

    @classmethod
    def stream_bank(cls):

        sql = "select idm.matched_id,doi.org_id as org_id2, doi.org_name, doi.org_full_name, doi.org_type_code, \
                doi.foundation_date,doi.legal_person, doi.org_name_en, \
                doi.reg_capital, doi.address, doi.tel, \
                doi.fax, doi.email, doi.website FROM \
                (SELECT matched_id, source_id from base_public.id_match WHERE id_type = 2 \
                AND is_used = 1 AND data_source = '020002') AS idm \
                JOIN crawl_public.d_org_info AS doi \
                ON idm.source_id = doi.org_id \
                WHERE doi.data_source = '020002' AND doi.org_type_code = 2"

        inp = MysqlInput(ENGINE_RD, sql)

        vm = transform.ValueMap({
            "reg_capital": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "address": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "tel": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "fax": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "email": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "website": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x,
            "legal_person": lambda x: cls.sub_wrong_to_none(x) if type(x) is str else x
        })

        vm2 = transform.ValueMap({
            "reg_capital": lambda x: float(x) if type(x) is str else x,
            "org_type": "托管银行"

        })

        vm3 = transform.ValueMap({
            "reg_capital": lambda x: None if type(x) is float and x < 0.0000001 else x
        })

        st = transform.MapSelectKeys({
            'matched_id': 'org_id',
            'org_name': 'org_name',
            'org_name_en': 'org_name_en',
            'org_full_name': 'org_full_name',
            'org_type_code': 'org_type_code',
            'org_type': 'org_type',
            'foundation_date': 'foundation_date',
            'legal_person': 'legal_person',
            'reg_capital': 'reg_capital',
            'address': 'address',
            'tel': 'tel',
            'fax': 'fax',
            'email': 'email',
            'website': 'website',

        })
        r = Stream(inp, transform=[vm, vm2, vm3, st])
        return r

    @classmethod
    def confluence(cls):
        streams = [cls.stream_020001(), cls.stream_020002(), cls.stream_020003(), cls.stream_bank()]
        c = Confluence(*streams, on=["org_id"])
        return c

    @classmethod
    def write(cls):
        b = cls.confluence()
        io.to_sql("base_public.org_info", ENGINE_RD, b.dataframe, type="update")


def test():
    from utils.etlkit.ext import tools
    import os
    import datetime as dt
    t = tools.TableComparer("data_test.org_info_test", "base_public.org_info", ENGINE_RD,
                            cols_excluded={"org_id", "org_name", "entry_time", "org_type_code", "update_time",
                                           "org_type", "data_source", "profile", "scale_mgt"})
    t.result.to_csv(os.path.expanduser("~/Desktop/org_info_{tm}.csv".format(tm=dt.date.today().strftime("%Y%m%d"))))
    print(t.result)
    return t


def main():
    StreamsMain.write()


if __name__ == "__main__":
    main()
