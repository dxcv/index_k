import datetime as dt
from utils.crawl.windcrawler import WindStockSeasonDataCrawler


def main():
    s = WindStockSeasonDataCrawler(dt.date.today(), dt.date.today())
    s.crawl()

if __name__ == "__main__":
    main()
