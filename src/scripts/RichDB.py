import logging
import pathlib
import sqlite3
import json
import utils


class RichDB:
    """
    !! 线程不安全，注意不要同时在多个线程拥有同一个connection。可以考虑改成生产-消费者队列
    """
    def __init__(self, data_path: pathlib.Path):
        """
        :param data_path: 数据库文件路径
        """
        self.__path = data_path
        self.__connection = sqlite3.connect(self.__path, check_same_thread=False)
        self.__cursor = self.__connection.cursor()
        self.__csi300 = []
        self.__csi500 = []
        self.__logger = logging.getLogger("global")

    def __del__(self):
        self.__cursor.close()
        self.__connection.close()

    def new_indexes_table(self):
        """
        建立指数每日详情表
        :return:
        """
        # 建立指数成分表
        sql = "CREATE TABLE IF NOT EXISTS indexes \
               (index_date DATE, index_name CHAR(20), detail TEXT, PRIMARY KEY (index_date, index_name))"
        self.__connection.execute(sql)

    def update_index(self, index: utils.Index):
        """
        1. 更新指数成分表; 2. 确保所有成分股都有一张表; 3. 更新类的指数变量，使其包含最新的成分股
        :param index:
        :return:
        """
        content = {}
        update_date = ''
        if index == utils.Index.CSI300:
            update_date, content = utils.get_300_detail()
            self.__csi300 = list(content.keys())
        elif index == utils.Index.CSI500:
            update_date, content = utils.get_500_detail()
            self.__csi500 = list(content.keys())
        else:
            return
        index_name = utils.IndexName[index]
        if content is not None:
            self.__logger.info("正在更新指数成分: %s" % index_name)
            str_content = json.dumps(content)
            # 插入或更新指数成分表
            sql = "INSERT INTO indexes(index_date, index_name, detail) \
                   VALUES ('{0}', '{1}', '{2}') \
                   ON CONFLICT (index_date, index_name) \
                   DO UPDATE SET detail = '{3}'".format(update_date, index_name, str_content, str_content)
            self.__cursor.execute(sql)
            # 每个股票单独建一张表
            for code in content:
                sql = "CREATE TABLE IF NOT EXISTS {0} \
                       (                                 \
                            trade_time DATETIME,         \
                            opening_price DOUBLE,        \
                            closing_price DOUBLE,        \
                            current_price DOUBLE,        \
                            highest_price DOUBLE,        \
                            lowest_price DOUBLE,         \
                            highest_bid DOUBLE,          \
                            lowest_bid DOUBLE,           \
                            trading_volume INT,          \
                            business_volume DOUBLE,      \
                            buying_1_volume DOUBLE,      \
                            buying_1_price DOUBLE,       \
                            buying_2_volume DOUBLE,      \
                            buying_2_price DOUBLE,       \
                            buying_3_volume DOUBLE,      \
                            buying_3_price DOUBLE,       \
                            buying_4_volume DOUBLE,      \
                            buying_4_price DOUBLE,       \
                            buying_5_volume DOUBLE,      \
                            buying_5_price DOUBLE,       \
                            selling_1_volume DOUBLE,     \
                            selling_1_price DOUBLE,      \
                            selling_2_volume DOUBLE,     \
                            selling_2_price DOUBLE,      \
                            selling_3_volume DOUBLE,     \
                            selling_3_price DOUBLE,      \
                            selling_4_volume DOUBLE,     \
                            selling_4_price DOUBLE,      \
                            selling_5_volume DOUBLE,     \
                            selling_5_price DOUBLE,      \
                            PRIMARY KEY (trade_time)     \
                       )".format(code)
                self.__cursor.execute(sql)
            self.__connection.commit()

    def get_stocks_of_index(self, index: utils.Index):
        """
        获取指数的成分股列表
        :param index:
        :return:
        """
        if index == utils.Index.CSI300:
            return self.__csi300
        elif index == utils.Index.CSI500:
            return self.__csi500

    def update_stocks(self, stocks):
        """
        向数据库插入股票列表的最新交易数据
        :param stocks: 股票代码列表，如sh600000, sz000001, ...
        :return:
        """
        details = utils.get_stock_now(stocks)
        for k, v in details.items():
            code = k
            metrics = v.split(",")
            trade_time = "%s %s" % (metrics[30], metrics[31])
            sql = ("INSERT OR IGNORE INTO %s (trade_time, opening_price, closing_price, current_price, highest_price, \
            lowest_price, highest_bid, lowest_bid, trading_volume, business_volume, buying_1_volume, buying_1_price, \
            buying_2_volume, buying_2_price, buying_3_volume, buying_3_price, buying_4_volume, buying_4_price, \
            buying_5_volume, buying_5_price, selling_1_volume, selling_1_price, selling_2_volume, selling_2_price, \
            selling_3_volume, selling_3_price, selling_4_volume, selling_4_price, selling_5_volume, selling_5_price) \
            VALUES (" + "'%s', " * 29 + "'%s')") % ((code, trade_time) + tuple(metrics[1:30]))
            self.__connection.execute(sql)
        self.__connection.commit()

