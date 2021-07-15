import pathlib
from datetime import datetime, timedelta, date
from utils import Index, determine_ends, is_trade_day
from RichDB import RichDB
from apscheduler.schedulers.blocking import BlockingScheduler
from logging.handlers import TimedRotatingFileHandler
import logging
import os
import sys


def global_init():
    DB_CLIENT.new_indexes_table()
    log_path = pathlib.Path("../log/")
    if not log_path.exists():
        log_path.mkdir()
    log_format = "%(asctime)s | %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)
    time_rotate_handler = TimedRotatingFileHandler(
        filename=log_path / 'rich.log',
        when='midnight',
        backupCount=5
    )
    formatter = logging.Formatter(log_format)
    time_rotate_handler.setFormatter(formatter)
    logger.addHandler(time_rotate_handler)
    aps_logger = logging.getLogger('apscheduler')
    aps_logger.addHandler(time_rotate_handler)


def update_stocks():
    for a, b in zip(STARTS, ENDS):
        DB_CLIENT.update_stocks(STOCK_POOL[a:b])


def daily_update_indexes():
    """
    每日更新指数成分
    :return: 
    """
    if is_trade_day():
        global STOCK_POOL, STARTS, ENDS
        STOCK_POOL = []
        for index in INDEXES:
            DB_CLIENT.update_index(index)
            STOCK_POOL.extend(DB_CLIENT.get_stocks_of_index(index))
        STARTS, ENDS = determine_ends(len(STOCK_POOL), BATCH_SIZE)
        logger.info("今日共有%d只目标股票" % len(STOCK_POOL))
        stop_date = (date.today() + timedelta(days=1)).isoformat()
        # 集合竞价
        scheduler.add_job(update_stocks,
                          'cron', hour="9", minute="15-25", second='*/%d' % QUERY_INTERVAL,
                          end_date=stop_date,
                          replace_existing=True,
                          id="call_auction",
                          name="集中竞价")
        # 上午
        scheduler.add_job(update_stocks,
                          'cron', hour="9", minute="30-59", second='*/%d' % QUERY_INTERVAL,
                          end_date=stop_date,
                          replace_existing=True,
                          id="am1",
                          name="上午1")
        scheduler.add_job(update_stocks,
                          'cron', hour="10-11", second='*/%d' % QUERY_INTERVAL,
                          end_date=stop_date,
                          replace_existing=True,
                          id="am2",
                          name="上午2")
        scheduler.add_job(update_stocks,
                          'cron', hour="11", minute="0-30", second='*/%d' % QUERY_INTERVAL,
                          end_date=stop_date,
                          replace_existing=True,
                          id="am3",
                          name="上午3")
        # 下午
        scheduler.add_job(update_stocks,
                          'cron', hour="13-14", second='*/%d' % QUERY_INTERVAL,
                          end_date=stop_date,
                          replace_existing=True,
                          id="pm1",
                          name="下午1")
        scheduler.add_job(update_stocks,
                          'cron', hour="15", minute="0", second='*/%d' % QUERY_INTERVAL,
                          end_date=stop_date,
                          replace_existing=True,
                          id="pm2",
                          name="下午2")


# 将工作路径切换到main脚本目录，方便docker部署
os.chdir(os.path.dirname(sys.argv[0]))
DB_PATH = pathlib.Path("../data/fortunes.db")
DB_CLIENT = RichDB(DB_PATH)
INDEXES = [Index.CSI300, Index.CSI500]
STOCK_POOL = []
BATCH_SIZE = 500
STARTS = []
ENDS = []
QUERY_INTERVAL = 5
scheduler = BlockingScheduler()
logger = logging.getLogger("global")
if __name__ == '__main__':
    global_init()
    now = datetime.now()
    init_delay = timedelta(seconds=5)
    scheduler.add_job(daily_update_indexes,
                      'cron',
                      day_of_week="mon-fri",
                      hour="8",
                      minute="30",
                      id="update_index",
                      name="更新指数",
                      next_run_time=now+init_delay)
    scheduler.start()
