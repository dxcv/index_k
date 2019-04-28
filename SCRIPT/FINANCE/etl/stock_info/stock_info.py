from utils.database import config as cfg, io
from utils.etlkit.core import base, transform
from utils.etlkit.reader.mysqlreader import MysqlInput
import re


class StreamMain:
    engine = cfg.load_engine()["2Gb"]

    def _validurl(self, url):
        url = re.sub("https?|:|：|//|\\\\|\s", "", url).lower()
        if url == "-":
            return None
        if url[-1] == "/":
            return url[:-1]
        return url

    @property
    def stream_010001(self):
        """
        上交所
        """
        sql = "SELECT stock_id, stock_id_a, stock_id_b, name, name_en, " \
              "full_name, full_name_en, reg_address, contact_address, legal_person," \
              "email, website, region, listing_date_a, listing_date_b " \
              "FROM crawl_finance.stock_info_010001 "
        inp = MysqlInput(self.engine, sql)

        rw = transform.CleanWrongToNone({
            "stock_id_a": "-",
            "stock_id_b": "-",
            "name": "-",
            "name_en": "-",
            "full_name": "-",
            "full_name_en": "-",
            "reg_address": "-",
            "contact_address": "-",
            "legal_person": "-",
            "email": "-",
        })

        vm = transform.ValueMap({
            "website": lambda x: self._validurl(x),
            "stock_id": lambda x: x + ".SH" if x is not None else x,
            "stock_id_a": lambda x: x + ".SH" if x is not None else x,
            "stock_id_b": lambda x: x + ".SH" if x is not None else x,
        })

        sk = transform.MapSelectKeys({
            "stock_id": None,
            "stock_id_a": None,
            "stock_id_b": None,
            "name": None,
            "name_en": None,
            "full_name": None,
            "full_name_en": None,
            "reg_address": None,
            "contact_address": None,
            "legal_person": None,
            "email": None,
            "website": None,
            "region": None,
            "listing_date_a": None,
            "listing_date_b": None,
        })
        s = base.Stream(inp, [rw, vm, sk])
        return s

    @property
    def stream_010002(self):
        """
        深交所
        """
        sql = "SELECT stock_id, stock_id_a, stock_id_b, name, full_name," \
              "full_name_en, reg_address, website, region, listing_date_a, " \
              "listing_date_b " \
              "FROM crawl_finance.stock_info_010002 "
        inp = MysqlInput(self.engine, sql)

        vm = transform.ValueMap({
            "website": lambda x: self._validurl(x),
            "stock_id": lambda x: x + ".SZ" if x is not None else x,
            "stock_id_a": lambda x: x + ".SZ" if x is not None else x,
            "stock_id_b": lambda x: x + ".SZ" if x is not None else x,
        })

        sk = transform.MapSelectKeys({
            "stock_id": None,
            "stock_id_a": None,
            "stock_id_b": None,
            "name": None,
            "name_en": None,
            "full_name": None,
            "full_name_en": None,
            "reg_address": None,
            "contact_address": None,
            "legal_person": None,
            "email": None,
            "website": None,
            "region": None,
            "listing_date_a": None,
            "listing_date_b": None,
        })
        s = base.Stream(inp, [vm, sk])
        return s

    @property
    def conflu(self):
        c = base.Confluence(self.stream_010001, self.stream_010002)
        return c

    def write(self):
        io.to_sql("base_finance.stock_info", self.engine, self.conflu.dataframe)


def main():
    StreamMain().write()


if __name__ == "__main__":
    main()
