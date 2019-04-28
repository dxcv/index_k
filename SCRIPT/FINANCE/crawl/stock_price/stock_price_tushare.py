import datetime as dt
from utils.crawl.tucrawler import StockKdataCrawler


def main():
    t1 = dt.date.today()
    t0 = t1 - dt.timedelta(1)
    StockKdataCrawler(date_start=t0, date_end=t1).crawl()


if __name__ == "__main__":
    main()
