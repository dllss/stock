#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据抓取核心模块
====================
这是整个系统最核心的数据抓取模块，负责从各大网站获取股票数据。

数据来源：
- 东方财富网（主要数据源）
- 新浪财经
- 其他金融网站

主要功能：
1. 股票实时数据抓取（每日股票、ETF数据）
2. 股票历史K线数据抓取（用于技术分析）
3. 资金流向数据抓取
4. 龙虎榜数据抓取
5. 大宗交易数据抓取
6. 分红配送数据抓取
7. 涨停原因数据抓取
8. 早盘/尾盘抢筹数据抓取
9. 综合选股数据抓取

数据缓存机制：
- 历史K线数据会缓存到本地
- 避免重复请求，提高效率
- 使用gzip压缩，节省磁盘空间

Python新手需要了解的概念：
- pandas DataFrame：类似Excel表格的数据结构
- 函数参数：可以有默认值，调用时可以不传
- 异常处理：try-except捕获错误，程序不会崩溃
- 文件缓存：将数据保存到文件，下次直接读取
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import os.path  # 文件路径处理
import datetime  # 日期时间处理
import numpy as np  # 数值计算库
import pandas as pd  # 数据分析库（DataFrame）
import talib as tl  # 技术分析库（TA-Lib）
import instock.core.tablestructure as tbs  # 数据表结构定义
import instock.lib.trade_time as trd  # 交易时间处理

# 导入各种数据爬虫模块
import instock.core.crawling.trade_date_hist as tdh  # 交易日历
import instock.core.crawling.fund_etf_em as fee  # ETF数据
import instock.core.crawling.stock_selection as sst  # 综合选股
import instock.core.crawling.stock_lhb_em as sle  # 龙虎榜（东方财富）
import instock.core.crawling.stock_lhb_sina as sls  # 龙虎榜（新浪）
import instock.core.crawling.stock_dzjy_em as sde  # 大宗交易
import instock.core.crawling.stock_hist_em as she  # 历史行情
import instock.core.crawling.stock_fund_em as sff  # 资金流向
import instock.core.crawling.stock_fhps_em as sfe  # 分红配送
import instock.core.crawling.stock_chip_race as scr  # 早盘/尾盘抢筹
import instock.core.crawling.stock_limitup_reason as slr  # 涨停原因

__author__ = 'myh '
__date__ = '2023/3/10 '

# ==================== 缓存目录设置 ====================
# 设置基础目录，每次加载使用
cpath_current = os.path.dirname(os.path.dirname(__file__))  # 当前模块的上级目录
stock_hist_cache_path = os.path.join(cpath_current, 'cache', 'hist')  # 历史数据缓存路径

# 如果缓存目录不存在，创建它
if not os.path.exists(stock_hist_cache_path):
    os.makedirs(stock_hist_cache_path)  # makedirs可以创建多级目录


# ==================== 股票筛选辅助函数 ====================

"""
判断股票代码是否是A股
A股市场股票代码规则：
- 600/601/603/605：上证A股（上海证券交易所）
- 000/001/002/003：深证A股（深圳证券交易所）
- 300/301：创业板（深圳证券交易所）
其他市场代码（本系统不包括）：
- 900：上证B股
- 200：深证B股
- 688：科创板
- 400：三板市场
- 430/83/87：北证A股
参数说明：
code (str): 股票代码，如"600000"
返回值：
bool: True表示是A股，False表示不是
使用示例：
if is_a_stock("600000"):
print("这是A股")
if is_a_stock("900001"):
print("这不是A股（是B股）")
"""
def is_a_stock(code):
    # str.startswith()：检查字符串是否以指定的前缀开始
    # 传入元组可以一次检查多个前缀
    return code.startswith(('600', '601', '603', '605', '000', '001', '002', '003', '300', '301'))


"""
过滤ST股票
什么是ST股票？
- ST：Special Treatment（特别处理）
- 公司连续两年亏损，被特别处理
- *ST：连续三年亏损，有退市风险
- 风险较高，本系统通常过滤掉
参数说明：
name (str): 股票名称，如"浦发银行"或"*ST海润"
返回值：
bool: True表示不是ST股，False表示是ST股
使用示例：
if is_not_st("浦发银行"):
print("普通股票")
if not is_not_st("*ST海润"):
print("这是ST股票，风险较高")
"""
def is_not_st(name):
    # not：取反运算符
    # 如果名称以*ST或ST开头，返回False
    return not name.startswith(('*ST', 'ST'))


"""
判断股票是否有有效价格（是否正常交易）
参数说明：
price (float): 股票价格
返回值：
bool: True表示有有效价格，False表示价格无效（可能退市或停牌）
NaN说明：
- NaN：Not a Number（不是一个数字）
- 表示无效的数值
- numpy.isnan()：检查是否是NaN
使用示例：
import numpy as np
if is_open(10.5):
print("价格有效")
if not is_open(np.nan):
print("价格无效，可能退市了")
"""
def is_open(price):
    # not np.isnan(price)：价格不是NaN，即有效价格
    return not np.isnan(price)


"""
判断价格是否有效（字符串版本）
某些数据源返回的价格是字符串格式
无效价格用"-"表示
参数说明：
price (str): 价格字符串
返回值：
bool: True表示有效，False表示无效
使用示例：
if is_open_with_line("10.5"):
print("有效价格")
if not is_open_with_line("-"):
print("无效价格")
"""
def is_open_with_line(price):
    return price != '-'


# ==================== 交易日历数据抓取 ====================

"""
获取股票交易日历
功能说明：
从新浪财经获取历史所有的交易日期
用于判断哪些日期是交易日
返回值：
set: 交易日期集合，每个元素是datetime.date对象
如果失败返回None
set数据结构：
- 集合：无序、不重复的元素集合
- 用{}表示，如{date1, date2, date3}
- 特点：查找速度快（用in运算符）
使用示例：
trade_dates = fetch_stocks_trade_date()
from datetime import date
if date(2024, 1, 1) in trade_dates:
print("2024-01-01是交易日")
else:
print("2024-01-01不是交易日")
数据流程：
网络请求 → DataFrame → 转换为set → 存储到单例对象
"""
def fetch_stocks_trade_date():
    try:
        # 调用爬虫函数获取交易日历数据
        data = tdh.tool_trade_date_hist_sina()
        
        # 检查数据是否有效
        if data is None or len(data.index) == 0:
            return None
        
        # 提取交易日期列，转换为set集合
        # data['trade_date'].values：获取该列的numpy数组
        # .tolist()：转换为Python列表
        # set()：转换为集合，自动去重
        data_date = set(data['trade_date'].values.tolist())
        return data_date
        
    except Exception as e:
        # 记录错误日志
        logging.error(f"stockfetch.fetch_stocks_trade_date处理异常：{e}")
    return None


# ==================== ETF数据抓取 ====================

"""
获取ETF（交易型开放式指数基金）当日数据
什么是ETF？
- Exchange Traded Fund：交易所交易基金
- 像股票一样在交易所买卖的基金
- 常见的有：沪深300ETF、创业板ETF等
参数说明：
date (datetime.date): 数据日期，None表示当前日期
返回值：
pandas.DataFrame: ETF数据表，包含以下列：
- date：日期
- code：代码
- name：名称
- new_price：最新价
- change_rate：涨跌幅
- volume：成交量
- deal_amount：成交额
- ...（更多字段见tablestructure.py）
数据处理流程：
1. 从东方财富网抓取ETF实时数据
2. 添加日期列
3. 重命名列名（匹配数据库表结构）
4. 过滤无效数据（价格为NaN的）
使用示例：
from datetime import date
# 获取今天的ETF数据
etf_data = fetch_etfs(None)
print(f"共有{len(etf_data)}只ETF")
# 获取指定日期的数据
etf_data = fetch_etfs(date(2024, 1, 1))
"""
def fetch_etfs(date):
    try:
        # 步骤1: 调用爬虫获取ETF实时数据
        data = fee.fund_etf_spot_em()
        
        # 检查数据是否有效
        if data is None or len(data.index) == 0:
            return None
        
        # 步骤2: 添加日期列
        if date is None:
            # 如果没有指定日期，使用当前日期
            # datetime.now().strftime("%Y-%m-%d")：格式化为"2024-01-01"
            data.insert(0, 'date', datetime.datetime.now().strftime("%Y-%m-%d"))
        else:
            # 使用指定日期
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        
        # 步骤3: 重命名列名
        # 从tablestructure模块获取标准列名
        data.columns = list(tbs.TABLE_CN_ETF_SPOT['columns'])
        
        # 步骤4: 过滤数据
        # data.loc[条件]：根据条件筛选行
        # apply()：对每个元素应用函数
        # 只保留价格有效的ETF
        data = data.loc[data['new_price'].apply(is_open)]
        
        return data
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_etfs处理异常：{e}")
    return None


# ==================== 股票当日数据抓取 ====================

"""
获取所有A股的当日数据（核心函数）
这是最常用的函数之一，获取所有股票的实时数据
参数说明：
date (datetime.date): 数据日期，None表示当前日期
返回值：
pandas.DataFrame: 股票数据表，包含200+个字段：
基本信息：代码、名称、最新价、涨跌幅等
成交信息：成交量、成交额、换手率等
财务指标：市盈率、市净率、每股收益等
资本结构：总股本、流通股本、总市值等
详细字段见TABLE_CN_STOCK_SPOT定义
数据处理流程：
1. 从东方财富网抓取所有A股实时数据
2. 添加日期列
3. 重命名列名
4. 过滤：只保留A股（排除B股、北证等）
5. 过滤：只保留价格有效的股票
使用示例：
# 获取今天所有股票数据
stocks = fetch_stocks(None)
print(f"共有{len(stocks)}只股票")
# 查看600000的数据
stock_600000 = stocks[stocks['code'] == '600000']
print(stock_600000[['name', 'new_price', 'change_rate']])
注意事项：
- 数据量较大（约4000-5000只股票）
- 包含大量财务指标，可能部分为空
- 开盘时数据实时更新
"""
def fetch_stocks(date):
    try:
        # 步骤1: 调用爬虫获取A股实时数据
        data = she.stock_zh_a_spot_em()
        
        # 检查数据是否有效
        if data is None or len(data.index) == 0:
            return None
        
        # 步骤2: 添加日期列
        if date is None:
            data.insert(0, 'date', datetime.datetime.now().strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        
        # 步骤3: 重命名列名（匹配数据库表结构）
        data.columns = list(tbs.TABLE_CN_STOCK_SPOT['columns'])
        
        # 步骤4: 链式过滤
        # .loc[条件1].loc[条件2]：多个条件依次过滤
        # 先过滤出A股，再过滤出价格有效的
        data = data.loc[data['code'].apply(is_a_stock)].loc[data['new_price'].apply(is_open)]
        
        return data
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_stocks处理异常：{e}")
    return None


# ==================== 综合选股数据抓取 ====================

"""
获取综合选股数据
什么是综合选股？
- 东方财富网提供的选股工具
- 可以根据200+个条件筛选股票
- 包括基本面、技术面、消息面等多维度指标
返回值：
pandas.DataFrame: 选股结果，包含所有可选股票的各项指标
数据特点：
- 字段非常多（200+个）
- 包括估值、成长、技术、人气等各类指标
- 数据量大，需要一定时间抓取
数据处理：
1. 抓取数据
2. 重命名列名
3. 去重（根据代码）
使用示例：
selection_data = fetch_stock_selection()
if selection_data is not None:
# 筛选市盈率小于20的股票
low_pe = selection_data[selection_data['pe'] < 20]
print(f"市盈率小于20的股票：{len(low_pe)}只")
"""
def fetch_stock_selection():
    try:
        # 步骤1: 调用爬虫获取选股数据
        data = sst.stock_selection()
        
        if data is None or len(data.index) == 0:
            return None
        
        # 步骤2: 重命名列名
        data.columns = list(tbs.TABLE_CN_STOCK_SELECTION['columns'])
        
        # 步骤3: 去重
        # drop_duplicates()：删除重复行
        # subset='code'：根据code列判断是否重复
        # keep='last'：保留最后一个
        # inplace=True：直接修改原DataFrame，不创建副本
        data.drop_duplicates('code', keep='last', inplace=True)
        
        return data
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_stocks_selection处理异常：{e}")
    return None


# ==================== 资金流向数据抓取 ====================

"""
获取股票资金流向数据
什么是资金流向？
- 记录主力资金（大单）的净流入/流出
- 分为超大单、大单、中单、小单
- 超大单和大单被认为是"主力资金"
- 资金流向可以判断主力的操作意图
参数说明：
index (int): 资金流向类型索引
0：今日资金流向
1：3日资金流向
2：5日资金流向
3：10日资金流向
（更多见CN_STOCK_FUND_FLOW定义）
返回值：
pandas.DataFrame: 资金流向数据，包括：
- code：股票代码
- name：股票名称
- new_price：最新价
- change_rate：涨跌幅
- fund_amount：主力净流入金额
- fund_rate：主力净流入占比
- fund_amount_super：超大单净流入
- fund_amount_large：大单净流入
- ...（更多字段）
资金流向解读：
- 正值：净流入，主力在买入
- 负值：净流出，主力在卖出
- 金额越大，主力操作越明显
使用示例：
# 获取今日资金流向
today_flow = fetch_stocks_fund_flow(0)
# 筛选主力净流入大于1亿的股票
big_inflow = today_flow[today_flow['fund_amount'] > 100000000]
print(f"今日主力大额流入股票：{len(big_inflow)}只")
# 按资金流入排序
top_inflow = today_flow.nlargest(10, 'fund_amount')
print("今日主力流入Top10:")
print(top_inflow[['code', 'name', 'fund_amount']])
"""
def fetch_stocks_fund_flow(index):
    try:
        # 步骤1: 根据索引获取配置
        cn_flow = tbs.CN_STOCK_FUND_FLOW[index]
        
        # 步骤2: 调用爬虫，传入时间范围参数
        # indicator：时间范围的中文名称，如"今日"、"3日"等
        data = sff.stock_individual_fund_flow_rank(indicator=cn_flow['cn'])
        
        if data is None or len(data.index) == 0:
            return None
        
        # 步骤3: 重命名列名
        data.columns = list(cn_flow['columns'])
        
        # 步骤4: 过滤数据
        # 只保留A股且价格有效的股票
        data = data.loc[data['code'].apply(is_a_stock)].loc[data['new_price'].apply(is_open_with_line)]
        
        return data
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_stocks_fund_flow处理异常：{e}")
    return None


# ==================== 板块资金流向数据抓取 ====================

"""
获取板块资金流向数据
什么是板块？
- 行业板块：银行、房地产、医药等
- 概念板块：5G、新能源、人工智能等
- 地域板块：江苏、浙江等
参数说明：
index_sector (int): 板块类型索引
0：行业板块
1：概念板块
index_indicator (int): 时间范围索引
0：今日
1：3日
2：5日
3：10日
返回值：
pandas.DataFrame: 板块资金流向数据
- name：板块名称
- fund_amount：主力净流入
- fund_rate：主力净流入占比
- ...
使用示例：
# 获取行业板块今日资金流向
sector_flow = fetch_stocks_sector_fund_flow(0, 0)
# 查看哪些行业资金流入最多
top_sectors = sector_flow.nlargest(5, 'fund_amount')
print("今日资金流入最多的5个行业：")
print(top_sectors[['name', 'fund_amount']])
"""
def fetch_stocks_sector_fund_flow(index_sector, index_indicator):
    try:
        # 步骤1: 获取配置
        cn_flow = tbs.CN_STOCK_SECTOR_FUND_FLOW[1][index_indicator]
        
        # 步骤2: 调用爬虫
        # sector_type：板块类型（行业/概念）
        data = sff.stock_sector_fund_flow_rank(
            indicator=cn_flow['cn'], 
            sector_type=tbs.CN_STOCK_SECTOR_FUND_FLOW[0][index_sector]
        )
        
        if data is None or len(data.index) == 0:
            return None
        
        # 步骤3: 重命名列名
        data.columns = list(cn_flow['columns'])
        
        return data
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_stocks_sector_fund_flow处理异常：{e}")
    return None


# ==================== 分红配送数据抓取 ====================

"""
获取股票分红配送数据
什么是分红配送？
- 分红：公司把利润分给股东（现金分红）
- 送股：把公积金转成股份送给股东（股票分红）
- 转增：把资本公积转成股份（增加股本）
分红配送形式：
- 10派X：每10股派现金X元
- 10送Y：每10股送Y股
- 10转Z：每10股转增Z股
- 例如："10派2.5送3转7"表示每10股派现2.5元、送3股、转增7股
参数说明：
date (datetime.date): 数据日期
返回值：
pandas.DataFrame: 分红配送数据，包括：
- date：日期
- code：股票代码
- name：股票名称
- report_date：报告期
- plan：分红配送方案
- record_date：股权登记日
- ex_date：除权除息日
- ...
重要日期说明：
- 股权登记日：当天持有股票的股东有权获得分红
- 除权除息日：股价调整日，会相应下调股价
使用示例：
bonus_data = fetch_stocks_bonus(date(2024, 6, 30))
# 筛选有分红的股票
has_bonus = bonus_data[bonus_data['plan'].str.contains('派')]
print(f"有分红的股票：{len(has_bonus)}只")
"""
def fetch_stocks_bonus(date):
    try:
        # 步骤1: 调用爬虫获取分红配送数据
        # trd.get_bonus_report_date()：获取最近的分红财报日期
        data = sfe.stock_fhps_em(date=trd.get_bonus_report_date())
        
        if data is None or len(data.index) == 0:
            return None
        
        # 步骤2: 添加日期列
        if date is None:
            data.insert(0, 'date', datetime.datetime.now().strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        
        # 步骤3: 重命名列名
        data.columns = list(tbs.TABLE_CN_STOCK_BONUS['columns'])
        
        # 步骤4: 只保留A股
        data = data.loc[data['code'].apply(is_a_stock)]
        
        return data
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_stocks_bonus处理异常：{e}")
    return None


# ==================== 龙虎榜数据抓取 ====================

"""
获取近三月有机构参与的龙虎榜股票
什么是龙虎榜？
- 每天涨跌幅、换手率异常的股票会上龙虎榜
- 公布前5名买方和卖方的席位信息
- 可以看出机构、游资的操作
什么是机构？
- 机构席位：基金、券商、社保、QFII等专业投资者
- 机构参与通常被认为是利好信号
- 机构买入次数多说明看好该股
参数说明：
date (datetime.date): 结束日期
返回值：
set: 符合条件的股票代码集合
None表示没有数据
筛选条件：
1. 近90天上过龙虎榜
2. 有买方机构席位参与
3. 机构买入次数大于1次
使用示例：
entity_codes = fetch_stock_top_entity_data(date(2024, 1, 1))
if entity_codes:
print(f"近三月有机构买入的股票：{len(entity_codes)}只")
print(f"部分代码：{list(entity_codes)[:10]}")
应用场景：
- 筛选有机构关注的股票
- 结合其他指标综合选股
"""
def fetch_stock_top_entity_data(date):
    # 计算开始日期：向前推90天
    run_date = date + datetime.timedelta(days=-90)
    start_date = run_date.strftime("%Y%m%d")  # 转换为"20240101"格式
    end_date = date.strftime("%Y%m%d")
    
    # 定义列名（数据源的列名）
    code_name = '代码'
    entity_amount_name = '买方机构数'
    
    try:
        # 步骤1: 获取龙虎榜机构买卖统计数据
        data = sle.stock_lhb_jgmmtj_em(start_date, end_date)
        
        if data is None or len(data.index) == 0:
            return None

        # 步骤2: 第一次筛选 - 有买方机构参与的记录
        # mask：布尔数组，True表示符合条件
        mask = (data[entity_amount_name] > 0)
        data = data.loc[mask]

        if len(data.index) == 0:
            return None

        # 步骤3: 按股票代码分组，统计每只股票的机构买入总次数
        # groupby()：按指定列分组
        # sum()：对每组求和
        grouped = data.groupby(by=data[code_name])
        data_series = grouped[entity_amount_name].sum()
        
        # 步骤4: 第二次筛选 - 机构买入次数大于1次
        # data_series > 1：返回布尔Series
        # .index.values：获取符合条件的股票代码
        data_code = set(data_series[data_series > 1].index.values)

        if not data_code:
            return None

        return data_code
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_top_entity_data处理异常：{e}")
    return None

# 描述: 获取东方财富-龙虎榜-个股上榜统计
def fetch_stock_lhb_data(date,count=12):
    try:
        start_date = trd.get_previous_trade_date(date,count).strftime("%Y%m%d")
        end_date = date.strftime("%Y%m%d")

        data = sle.stock_lhb_detail_em(start_date, end_date)
        if data is None or len(data.index) == 0:
            return None
        _columns = list(tbs.TABLE_CN_STOCK_lHB['columns'])
        _columns.pop(0)
        data.columns = _columns
        data = data.loc[data['code'].apply(is_a_stock)]
        data.drop_duplicates('code', keep='last', inplace=True)
        # data = data.sort_values(by='ranking_times', ascending=False)
        if date is None:
            data.insert(0, 'date', datetime.datetime.now().strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        return data
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_lhb_data处理异常：{e}")
    return None

"""
获取龙虎榜个股上榜统计数据（新浪财经）
功能说明：
获取个股的历史上榜统计信息
包括上榜次数、累计买入/卖出金额等
参数说明：
date (datetime.date): 数据日期
返回值：
pandas.DataFrame: 龙虎榜统计数据
- date：日期
- code：代码
- name：名称
- count：上榜次数
- ...
使用示例：
top_data = fetch_stock_top_data(date(2024, 1, 1))
# 查看上榜次数最多的股票
most_top = top_data.nlargest(10, 'count')
print("上榜次数Top10：")
print(most_top[['code', 'name', 'count']])
"""
def fetch_stock_top_data(date):
    try:
        # 步骤1: 调用新浪爬虫获取数据
        data = sls.stock_lhb_ggtj_sina()
        
        if data is None or len(data.index) == 0:
            return None
        
        # 步骤2: 处理列名
        _columns = list(tbs.TABLE_CN_STOCK_TOP['columns'])
        _columns.pop(0)  # 移除第一个列名（date列后面会添加）
        data.columns = _columns
        
        # 步骤3: 过滤和去重
        data = data.loc[data['code'].apply(is_a_stock)]
        data.drop_duplicates('code', keep='last', inplace=True)
        
        # 步骤4: 添加日期列
        if date is None:
            data.insert(0, 'date', datetime.datetime.now().strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        
        return data
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_top_data处理异常：{e}")
    return None


# ==================== 大宗交易数据抓取 ====================

"""
获取大宗交易数据
什么是大宗交易？
- 单笔交易数量大、金额大的交易
- 不通过正常的买卖盘撮合，单独协商
- 通常有折价（低于市价）
- 可能是机构调仓、股东减持等
大宗交易解读：
- 折价率高：可能是大股东减持，看跌信号
- 溢价买入：买方看好该股，看涨信号
- 成交量大：说明有大资金在操作
参数说明：
date (datetime.date): 数据日期
返回值：
pandas.DataFrame: 大宗交易数据
- date：日期
- code：代码
- name：名称
- price：成交价
- volume：成交量
- deal_amount：成交额
- premium_rate：溢价率（正值溢价，负值折价）
- ...
注意事项：
- 大宗交易数据延迟发布
- 通常在收盘后1-2小时才有数据
- 如果17:00前调用可能返回None
使用示例：
block_data = fetch_stock_blocktrade_data(date(2024, 1, 1))
if block_data is not None:
# 查看有折价的大宗交易
discount = block_data[block_data['premium_rate'] < 0]
print(f"折价交易：{len(discount)}笔")
"""
def fetch_stock_blocktrade_data(date):
    date_str = date.strftime("%Y%m%d")  # 转换为"20240101"格式
    
    try:
        # 步骤1: 调用爬虫获取大宗交易数据
        # start_date和end_date相同，只获取当天数据
        data = sde.stock_dzjy_mrtj(start_date=date_str, end_date=date_str)
        
        if data is None or len(data.index) == 0:
            return None

        # 步骤2: 处理列名
        columns = list(tbs.TABLE_CN_STOCK_BLOCKTRADE['columns'])
        columns.insert(0, 'index')  # 数据源有个索引列
        data.columns = columns
        
        # 步骤3: 过滤数据
        data = data.loc[data['code'].apply(is_a_stock)]
        
        # 步骤4: 删除索引列
        data.drop('index', axis=1, inplace=True)
        
        return data
        
    except TypeError:
        # TypeError：数据类型错误，通常是因为还没有数据
        logging.error("处理异常：目前还没有大宗交易数据，请17:00点后再获取！")
        return None
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_blocktrade_data处理异常：{e}")
    return None


# ==================== 早盘/尾盘抢筹数据抓取 ====================

"""
获取早盘抢筹数据
什么是早盘抢筹？
- 开盘后30分钟内快速拉升的股票
- 通常是有资金抢筹（急于买入）
- 可能有利好消息或主力拉升计划
参数说明：
date (datetime.date): 数据日期
返回值：
pandas.DataFrame: 早盘抢筹数据
- date：日期
- code：代码
- name：名称
- increase：涨幅
- ...
使用示例：
open_race = fetch_stock_chip_race_open(date(2024, 1, 1))
if open_race is not None:
print(f"早盘抢筹股票：{len(open_race)}只")
"""
def fetch_stock_chip_race_open(date):
    try:
        # 处理日期参数
        date_str = ""
        if date != datetime.datetime.now().date():
            date_str = date.strftime("%Y%m%d")
        
        # 调用爬虫获取数据
        data = scr.stock_chip_race_open(date_str)
        
        if data is None or len(data.index) == 0:
            return None
        
        # 添加日期列
        if date is None:
            data.insert(0, 'date', datetime.datetime.now().strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        
        # 重命名列名
        data.columns = list(tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['columns'])
        
        return data
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_chip_race_open处理异常：{e}")
    return None


"""
获取尾盘抢筹数据
什么是尾盘抢筹？
- 收盘前30分钟快速拉升的股票
- 可能是拉升收盘价，为第二天做准备
- 也可能是资金急于买入
参数说明：
date (datetime.date): 数据日期
返回值：
pandas.DataFrame: 尾盘抢筹数据
使用示例：
end_race = fetch_stock_chip_race_end(date(2024, 1, 1))
if end_race is not None:
print(f"尾盘抢筹股票：{len(end_race)}只")
"""
def fetch_stock_chip_race_end(date):
    try:
        date_str = ""
        if date != datetime.datetime.now().date():
            date_str = date.strftime("%Y%m%d")
        
        data = scr.stock_chip_race_end(date_str)
        
        if data is None or len(data.index) == 0:
            return None
        
        if date is None:
            data.insert(0, 'date', datetime.datetime.now().strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        
        data.columns = list(tbs.TABLE_CN_STOCK_CHIP_RACE_END['columns'])
        
        return data
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_chip_race_end处理异常：{e}")
    return None


# ==================== 涨停原因数据抓取 ====================

"""
获取涨停原因揭秘数据
什么是涨停？
- A股有涨跌停限制（通常10%）
- 涨停：当天涨到最高限制，无法继续上涨
- 涨停通常是有重大利好消息
涨停原因类型：
- 业绩大增
- 重组并购
- 新产品发布
- 政策利好
- 概念炒作
- ...
参数说明：
date (datetime.date): 数据日期
返回值：
pandas.DataFrame: 涨停原因数据
- date：日期
- code：代码
- name：名称
- reason：涨停原因
- ...
使用示例：
limitup_data = fetch_stock_limitup_reason(date(2024, 1, 1))
if limitup_data is not None:
print(f"今日涨停：{len(limitup_data)}只")
print("涨停原因分布：")
print(limitup_data['reason'].value_counts())
应用场景：
- 了解市场热点
- 挖掘潜力板块
- 跟踪概念炒作
"""
def fetch_stock_limitup_reason(date):
    try:
        # 调用爬虫获取涨停原因数据
        data = slr.stock_limitup_reason(date.strftime("%Y-%m-%d"))
        
        if data is None or len(data.index) == 0:
            return None
        
        # 重命名列名
        data.columns = list(tbs.TABLE_CN_STOCK_LIMITUP_REASON['columns'])
        
        return data
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_limitup_reason处理异常：{e}")
    return None


# ==================== ETF历史数据抓取 ====================

"""
获取ETF历史K线数据
参数说明：
data_base (tuple): (日期, 代码) 元组
date_start (str, 可选): 开始日期，格式"20240101"
date_end (str, 可选): 结束日期，格式"20240101"
adjust (str): 复权类型
- 'qfq'：前复权（默认）
- 'hfq'：后复权
- ''：不复权
什么是复权？
- 股票分红、送股后，价格会调整
- 复权：把历史价格调整到统一标准
- 前复权：以最新价为基准，调整历史价格（常用）
- 后复权：以最早价为基准，调整后续价格
- 不复权：保留原始价格，会有跳空缺口
返回值：
pandas.DataFrame: 历史K线数据
- date：日期
- open：开盘价
- high：最高价
- low：最低价
- close：收盘价
- volume：成交量（股）
- amount：成交额
- p_change：涨跌幅（%）
数据处理：
1. 抓取原始数据
2. 计算涨跌幅（使用talib的ROC函数）
3. 成交量单位从手转换为股（*100）
4. 按日期排序
"""
def fetch_etf_hist(data_base, date_start=None, date_end=None, adjust='qfq'):
    # 解析参数
    date = data_base[0]
    code = data_base[1]

    # 如果没有指定开始日期，自动计算（向前推3年）
    if date_start is None:
        date_start, is_cache = trd.get_trade_hist_interval(date)
    
    try:
        # 步骤1: 调用爬虫获取历史数据
        if date_end is not None:
            # 指定了结束日期
            data = fee.fund_etf_hist_em(
                symbol=code, 
                period="daily",  # 日线
                start_date=date_start, 
                end_date=date_end,
                adjust=adjust
            )
        else:
            # 没有指定结束日期，获取到最新
            data = fee.fund_etf_hist_em(
                symbol=code, 
                period="daily", 
                start_date=date_start, 
                adjust=adjust
            )

        if data is None or len(data.index) == 0:
            return None
        
        # 步骤2: 重命名列名
        data.columns = tuple(tbs.CN_STOCK_HIST_DATA['columns'])
        
        # 步骤3: 按日期排序
        # sort_index()：按索引（日期）排序
        data = data.sort_index()
        
        # 步骤4: 数据处理
        if data is not None:
            # 计算涨跌幅
            # tl.ROC()：Rate of Change，变化率
            # ROC(close, 1)：计算相对于1天前的涨跌幅
            # loc[:, 'p_change']：修改p_change列
            data.loc[:, 'p_change'] = tl.ROC(data['close'].values, 1)
            
            # 处理NaN值（第一天没有涨跌幅）
            data['p_change'].values[np.isnan(data['p_change'].values)] = 0.0
            
            # 成交量单位转换：手 → 股（1手=100股）
            data["volume"] = data['volume'].values.astype('double') * 100
        
        return data
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_etf_hist处理异常：{e}")
    return None


# ==================== 股票历史数据抓取（带缓存）====================

"""
获取股票历史K线数据（核心函数，带缓存机制）
这是获取历史数据的核心函数，使用了缓存机制提高效率
参数说明：
data_base (tuple): (日期, 代码, 名称) 元组
date_start (str, 可选): 开始日期，格式"20240101"
如果为None，自动计算（向前推3年）
is_cache (bool): 是否使用缓存，默认True
True：先检查缓存，没有再抓取
False：每次都重新抓取
返回值：
pandas.DataFrame: 历史K线数据，同fetch_etf_hist
缓存机制：
1. 数据保存在instock/cache/hist/目录下
2. 按月份和日期组织：cache/hist/202401/20240101/
3. 文件名：股票代码+复权类型.gzip.pickle
4. 使用gzip压缩，节省磁盘空间
5. 使用pickle格式，读写速度快
缓存策略：
- 历史数据不会变化，可以永久缓存
- 如果文件存在，直接读取
- 如果文件不存在，抓取后保存
性能提升：
- 无缓存：3000只股票需要几十分钟
- 有缓存：3000只股票只需几秒钟
使用示例：
# 获取600000的历史数据
hist_data = fetch_stock_hist(
('2024-01-01', '600000', '浦发银行'),
date_start='20210101',
is_cache=True
)
if hist_data is not None:
print(f"数据长度：{len(hist_data)}天")
print(f"最新收盘价：{hist_data['close'].iloc[-1]}")
print(f"最高价：{hist_data['high'].max()}")
print(f"最低价：{hist_data['low'].min()}")
"""
def fetch_stock_hist(data_base, date_start=None, is_cache=True):
    # 解析参数
    date = data_base[0]
    code = data_base[1]

    # 如果没有指定开始日期，自动计算
    if date_start is None:
        date_start, is_cache = trd.get_trade_hist_interval(date)
    
    try:
        # 步骤1: 调用带缓存的获取函数
        data = stock_hist_cache(code, date_start, None, is_cache, 'qfq')
        
        # 步骤2: 数据处理
        if data is not None:
            # 计算涨跌幅
            data.loc[:, 'p_change'] = tl.ROC(data['close'].values, 1)
            data['p_change'].values[np.isnan(data['p_change'].values)] = 0.0
            
            # 成交量单位转换
            data["volume"] = data['volume'].values.astype('double') * 100
        
        return data
        
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_hist处理异常：{e}")
    return None


# ==================== 缓存辅助函数 ====================

"""
股票历史数据缓存管理（内部函数）
这是内部使用的缓存管理函数，不建议直接调用
应该使用fetch_stock_hist函数
参数说明：
code (str): 股票代码
date_start (str): 开始日期，格式"20240101"
date_end (str, 可选): 结束日期
is_cache (bool): 是否使用缓存
adjust (str): 复权类型，''表示前复权
返回值：
pandas.DataFrame: 历史K线数据
缓存文件组织结构：
instock/cache/hist/
├── 202401/              # 年月文件夹
│   ├── 20240101/        # 日期文件夹
│   │   ├── 600000qfq.gzip.pickle    # 600000前复权数据
│   │   ├── 600001qfq.gzip.pickle    # 600001前复权数据
│   │   └── ...
│   └── 20240102/
│       └── ...
└── 202402/
└── ...
为什么这样组织？
- 方便管理：可以按月或按日删除旧缓存
- 提高效率：避免单个文件夹文件过多
- 清晰明了：一看就知道是哪天的数据
pickle vs CSV：
- pickle：Python专用格式，速度快，支持所有数据类型
- CSV：通用格式，速度慢，只支持文本
- 本系统使用pickle+gzip，兼顾速度和空间
"""
def stock_hist_cache(code, date_start, date_end=None, is_cache=True, adjust=''):
    # 步骤1: 构建缓存目录路径
    # date_start[0:6]：提取年月，如"202401"
    cache_dir = os.path.join(stock_hist_cache_path, date_start[0:6], date_start)
    
    # 步骤2: 创建缓存目录（如果不存在）
    try:
        if not os.path.exists(cache_dir):
            # makedirs：创建多级目录
            os.makedirs(cache_dir)
    except Exception:
        # 创建失败不影响程序运行（不使用缓存）
        pass
    
    # 步骤3: 构建缓存文件路径
    # 文件名格式：代码+复权类型.gzip.pickle
    # 例如：600000qfq.gzip.pickle
    cache_file = os.path.join(cache_dir, "%s%s.gzip.pickle" % (code, adjust))
    
    # 步骤4: 尝试读取缓存或抓取数据
    try:
        # 检查缓存文件是否存在
        if os.path.isfile(cache_file):
            # 缓存存在，直接读取
            # compression="gzip"：使用gzip解压缩
            return pd.read_pickle(cache_file, compression="gzip")
        else:
            # 缓存不存在，从网络抓取
            if date_end is not None:
                # 指定了结束日期
                stock = she.stock_zh_a_hist(
                    symbol=code, 
                    period="daily", 
                    start_date=date_start, 
                    end_date=date_end,
                    adjust=adjust
                )
            else:
                # 没有指定结束日期
                stock = she.stock_zh_a_hist(
                    symbol=code, 
                    period="daily", 
                    start_date=date_start, 
                    adjust=adjust
                )

            # 检查数据是否有效
            if stock is None or len(stock.index) == 0:
                return None
            
            # 重命名列名
            stock.columns = tuple(tbs.CN_STOCK_HIST_DATA['columns'])
            
            # 按日期排序
            stock = stock.sort_index()
            
            # 步骤5: 保存到缓存
            try:
                if is_cache:
                    # to_pickle：保存为pickle格式
                    # compression="gzip"：使用gzip压缩
                    stock.to_pickle(cache_file, compression="gzip")
            except Exception:
                # 保存失败不影响返回数据
                pass
            
            return stock
            
    except Exception as e:
        logging.error(f"stockfetch.stock_hist_cache处理异常：{code}代码{e}")
    return None


"""
===========================================
股票数据抓取模块使用总结（给Python新手）
===========================================

1. 核心功能
   - fetch_stocks()：获取所有股票当日数据【最常用】
   - fetch_stock_hist()：获取股票历史K线数据【最常用】
   - fetch_etfs()：获取ETF当日数据
   - fetch_stocks_fund_flow()：获取资金流向
   - fetch_stocks_bonus()：获取分红配送
   - fetch_stock_top_data()：获取龙虎榜数据
   - fetch_stock_blocktrade_data()：获取大宗交易

2. 数据来源
   - 东方财富网：主要数据源
   - 新浪财经：龙虎榜、交易日历
   - 所有数据免费，无需API密钥

3. 缓存机制
   - 历史数据自动缓存到本地
   - 使用gzip压缩，节省空间
   - 大大提高重复运行速度

4. 数据处理流程
   网络抓取 → DataFrame → 列名标准化 → 数据过滤 → 返回/存库
   
5. 异常处理
   - 所有函数都有try-except
   - 失败返回None，不影响程序
   - 错误记录到日志文件

6. 性能优化
   - 使用缓存避免重复请求
   - 可以使用多线程并发抓取
   - 合理的数据过滤减少处理量

7. 注意事项
   - 网络请求可能失败，要检查返回值
   - 不同数据有不同的发布时间
   - 尊重数据源，不要频繁请求
   - 大宗交易17:00后才有数据

8. 数据应用
   - 技术指标计算需要历史数据
   - K线形态识别需要历史数据
   - 策略选股需要当日数据+历史数据
   - Web展示需要当日数据
"""
