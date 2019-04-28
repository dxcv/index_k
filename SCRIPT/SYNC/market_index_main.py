import datetime as dt
from dateutil.relativedelta import relativedelta
from SCRIPT.OTHER.desktop.sync_market_index import sync_market_index
from SCRIPT.OTHER.desktop.sync_sws_index import sync_sws_index
from SCRIPT.OTHER.desktop.sync_stock_price import sync_stock_price


def main():
    now = dt.datetime.now()
    start = now - relativedelta(days=0, minutes=6)

    sync_market_index(start, now)
    sync_sws_index(start, now)
    sync_stock_price(start, now)


if __name__ == '__main__':
    main()
