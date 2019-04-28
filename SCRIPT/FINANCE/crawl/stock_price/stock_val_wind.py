from utils.crawl.windcrawler import WindStockValCrawler, WindStockCapitalCrawler, AmtSHStockCrawler
import datetime as dt


def main():
    WindStockValCrawler(start=dt.date(2018, 6, 1), end=dt.date(2018, 6, 30)).crawl()
    # WindStockValCrawler(start=dt.date(2016, 3, 1), end=dt.date(2016, 10, 31)).crawl()


def tmp():
    WindStockCapitalCrawler(start=dt.date(2017, 1, 1), end=dt.date(2018, 6, 28)).crawl()
    # AmtSHStockCrawler(date_s=dt.date(2016, 1, 1), date_e=dt.date(2016, 12, 31)).crawl()


if __name__ == "__main__":
    main()
