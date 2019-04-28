import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from SCRIPT.OTHER.desktop.sync_market_index import job_sync_market_index
from SCRIPT.OTHER.desktop.sync_sws_index import job_sync_sws_index
from SCRIPT.OTHER.desktop.sync_stock_price import job_sync_stock_price
from SCRIPT.PRIVATE.etl.fund_info.fund_info_main import main
from SCRIPT.PRIVATE.etl.fund_nv_data_standard.fund_nv_data_standard import main
print('启动当天')
job_sync_market_index()
job_sync_sws_index()
job_sync_stock_price()



# def my_job():
#     job_sync_market_index()
#     job_sync_sws_index()
#     job_sync_stock_price()
#     main()
#     print('my_job2 is running, Now is %s' % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#
#
# sched = BlockingScheduler()
# sched.add_job(my_job, 'interval', hours=0, minutes=30)
# sched.start()



# """
# 移除作业
# """
# scheduler.add_job(myfunc, 'interval', minutes=2, id='my_job_id')
# scheduler.remove_job('my_job_id')


#
# cron参数如下：
#
#     year (int|str) – 年，4位数字
#     month (int|str) – 月 (范围1-12)
#     day (int|str) – 日 (范围1-31)
#     week (int|str) – 周 (范围1-53)
#     day_of_week (int|str) – 周内第几天或者星期几 (范围0-6 或者 mon,tue,wed,thu,fri,sat,sun)
#     hours (int|str) – 时 (范围0-23)
#     minute (int|str) – 分 (范围0-59)
#     second (int|str) – 秒 (范围0-59)
#     start_date (datetime|str) – 最早开始日期(包含)
#     end_date (datetime|str) – 最晚结束时间(包含)
#     timezone (datetime.tzinfo|str) – 指定时区

