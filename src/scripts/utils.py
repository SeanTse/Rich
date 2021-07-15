import datetime
import requests
import xlrd
import time
import pathlib
import re
from enum import Enum
import logging

pattern = re.compile(r'var\s+hq_str_(\w+)="(.*)";', re.M)
logger = logging.getLogger("global")


class Index(Enum):
    CSI300 = 1
    CSI500 = 2


IndexName = {
    Index.CSI300: "csi300",
    Index.CSI500: "csi500"
}
IndexSource = {
    Index.CSI300: "http://www.csindex.com.cn/uploads/file/autofile/cons/000300cons.xls",
    Index.CSI500: "http://www.csindex.com.cn/uploads/file/autofile/cons/000905cons.xls"
}


def is_trade_day():
    """
    今天是否交易日
    :return: True or False
    """
    closed_days = load_closed_days("../data/closed_days")
    today = datetime.date.today()
    is_weekday = today.weekday() < 5
    return today.isoformat() not in closed_days and is_weekday


def load_closed_days(path: str):
    """
    从本地文件载入休市日，每行代表一个休市日，为ISO日期格式，如1999-01-01
    :param path: 文件路径
    :return:
    """
    file = pathlib.Path(path)
    closed_days = []
    with file.open(mode="r", encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if line != '' and not line.startswith('#'):
                closed_days.append(line)
    return closed_days


def determine_ends(n: int, step: int, begin=0):
    """
    列表按指定长度分段，最后一段不足指定长度则按剩余长度计算
    :param n: 列表结束值
    :param step: 每段大小
    :param begin: 列表起始值，默认为0
    :return: start, end: 各段开始和结束值
    """
    starts = []
    ends = []
    i = begin
    while i < n:
        starts.append(i)
        j = n
        if i + step < n:
            j = i + step
        ends.append(j)
        i += step
    return starts, ends


def get_300_detail():
    """
    从中证获取沪深300指数成分股
    :return: 成分股列表
    """
    logger.info("正在获取沪深300指数")
    return get_index_detail(Index.CSI300)


def get_500_detail():
    """
    从中证获取中证500指数成分股
    :return: 成分股列表
    """
    logger.info("正在获取中证500指数")
    return get_index_detail(Index.CSI500)


def get_stock_now(stock_ids: list):
    baseurl = "http://hq.sinajs.cn/list="
    resp = requests.get(baseurl+",".join(stock_ids))
    parsed = pattern.findall(resp.text)
    res = {}
    for p in parsed:
        res[p[0]] = p[1]
    return res


def get_index_detail(index: Index):
    today = time.strftime("%Y-%m-%d", time.localtime())
    xls_path = pathlib.Path("../data/xls_indexes")
    if not xls_path.exists():
        xls_path.mkdir()
    xls = xls_path / "{0}_{1}.xls".format(IndexName[index], today)
    abbr = {"SHH": "sh", "SHZ": "sz"}
    url = IndexSource[index]
    if not xls.exists():
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 \
            Safari/537.36'
        }
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            with open(xls, "wb") as f:
                f.write(resp.content)
    with xlrd.open_workbook(xls) as book:
        sh = book.sheet_by_index(0)
        stocks = {}
        for r in range(1, sh.nrows):
            code = sh.cell_value(rowx=r, colx=4)
            name = sh.cell_value(rowx=r, colx=5)
            exchange = sh.cell_value(rowx=r, colx=7)
            stocks[abbr[exchange]+code] = name
        return sh.cell_value(rowx=1, colx=0), stocks
