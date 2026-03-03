#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票交易时间判断模块
=====================
这个模块提供了判断股票交易日期和交易时间的各种工具函数。

股票市场的交易时间规则：
1. 工作日（周一到周五）才可能是交易日
2. 节假日、周末不是交易日
3. 每天的交易时间：
   - 上午：9:30-11:30
   - 下午：13:00-15:00
4. 集合竞价时间：9:15-9:25
5. 午间休市：11:30-13:00

主要功能：
- 判断是否是交易日
- 获取上一个/下一个交易日
- 判断是否在交易时间内
- 判断是否开盘/收盘
- 获取财报日期
"""

# ==================== 导入必需的库 ====================
import datetime  # Python标准库，用于处理日期和时间
from instock.core.singleton_trade_date import stock_trade_date  # 单例模式的交易日期数据

__author__ = 'myh '
__date__ = '2023/4/10 '


# ==================== 交易日期判断函数 ====================

"""
判断指定日期是否是交易日
什么是交易日？
- 股市开市的日期
- 一般是工作日（周一到周五）
- 排除节假日（如春节、国庆等）
参数说明：
date (datetime.date, 可选): 要判断的日期，默认None表示今天
返回值：
bool: True表示是交易日，False表示不是交易日
实现原理：
从单例对象中获取所有交易日列表，检查指定日期是否在列表中
使用示例：
from datetime import date
# 判断今天是否是交易日
if is_trade_date():
print("今天是交易日")
# 判断指定日期是否是交易日
if is_trade_date(date(2024, 1, 1)):
print("2024年1月1日是交易日")
else:
print("2024年1月1日不是交易日（元旦假期）")
"""
def is_trade_date(date=None):
    # 获取所有交易日期的列表（单例模式，数据只加载一次）
    trade_date = stock_trade_date().get_data()
    
    # 如果交易日期数据为空，返回False
    if trade_date is None:
        return False
    
    # 检查指定日期是否在交易日列表中
    # in 运算符：检查元素是否在列表/集合中
    if date in trade_date:
        return True
    else:
        return False


"""
获取指定日期的前N个交易日
参数说明：
date (datetime.date): 起始日期
count (int): 往前推几个交易日，默认1
返回值：
datetime.date: 前N个交易日
"""
def get_previous_trade_date(date, count=1):
    while True:
        date = get_one_previous_trade_date(date)
        count -= 1
        if count == 0:
            break
    return date

"""
获取指定日期的前一个交易日
参数说明：
date (datetime.date): 起始日期
返回值：
datetime.date: 前一个交易日
工作原理：
从指定日期开始，每天往前推一天
直到找到一个交易日为止
"""
def get_one_previous_trade_date(date):
    trade_date = stock_trade_date().get_data()
    if trade_date is None:
        return date  # 如果没有交易日数据，直接返回原日期
    
    tmp_date = date  # 临时变量，从指定日期开始
    
    # 无限循环，直到找到交易日
    while True:
        # timedelta：时间差对象
        # timedelta(days=-1)：表示减一天
        tmp_date += datetime.timedelta(days=-1)
        
        # 检查这一天是否是交易日
        if tmp_date in trade_date:
            break  # 找到了，跳出循环
    
    return tmp_date


"""
获取指定日期的下一个交易日
参数说明：
date (datetime.date): 起始日期
返回值：
datetime.date: 下一个交易日
工作原理：
从指定日期开始，每天往后推一天
直到找到一个交易日为止
使用示例：
from datetime import date
# 假设2024-01-1是周五
next_date = get_next_trade_date(date(2024, 1, 5))
print(next_date)  # 可能输出：2024-01-08（下周一）
应用场景：
- 计算T+1交易（今天买入，明天才能卖出）
- 预测下一个交易日的数据
"""
def get_next_trade_date(date):
    trade_date = stock_trade_date().get_data()
    if trade_date is None:
        return date
    
    tmp_date = date
    while True:
        # timedelta(days=1)：表示加一天
        tmp_date += datetime.timedelta(days=1)
        if tmp_date in trade_date:
            break
    
    return tmp_date


# ==================== 交易时间段定义 ====================
# 开盘时间段（元组的元组）
# 每个内部元组包含(开始时间, 结束时间)
OPEN_TIME = (
    (datetime.time(9, 15, 0), datetime.time(11, 30, 0)),   # 上午：9:15-11:30（包含集合竞价）
    (datetime.time(13, 0, 0), datetime.time(15, 0, 0)),   # 下午：13:00-15:00
)


"""
判断指定时间是否在交易时间段内
参数说明：
now_time (datetime.datetime): 要判断的时间
返回值：
bool: True表示在交易时间内，False表示不在
交易时间：
上午：9:15-11:30（含集合竞价9:15-9:25）
下午：13:00-15:00
使用示例：
from datetime import datetime
# 判断当前是否在交易时间
now = datetime.now()
if is_tradetime(now):
print("现在是交易时间")
else:
print("现在不是交易时间")
# 判断指定时间
test_time = datetime(2024, 1, 1, 10, 30)  # 上午10:30
if is_tradetime(test_time):
print("10:30在交易时间内")
"""
def is_tradetime(now_time):
    now = now_time.time()  # 提取时间部分（不含日期）
    
    # 遍历所有交易时间段
    for begin, end in OPEN_TIME:
        # 检查当前时间是否在这个时间段内
        # begin <= now < end：左闭右开区间
        if begin <= now < end:
            return True
    else:
        # for循环正常结束（没有break），执行else
        return False


# 午间休市时间
PAUSE_TIME = (
    (datetime.time(11, 30, 0), datetime.time(12, 59, 30)),  # 11:30-12:59:30
)


"""
判断是否在午间休市时间
参数说明：
now_time (datetime.datetime): 要判断的时间
返回值：
bool: True表示在休市时间，False表示不在
休市时间：
11:30-12:59:30（午间休息）
使用示例：
from datetime import datetime
test_time = datetime(2024, 1, 1, 12, 0)  # 中午12:00
if is_pause(test_time):
print("现在是午间休市时间")
"""
def is_pause(now_time):
    now = now_time.time()
    for b, e in PAUSE_TIME:  # b=begin, e=end（变量名简写）
        if b <= now < e:
            return True


# 即将恢复交易时间（准备开盘）
CONTINUE_TIME = (
    (datetime.time(12, 59, 30), datetime.time(13, 0, 0)),  # 12:59:30-13:00
)


"""
判断是否即将恢复交易（下午开盘前30秒）
参数说明：
now_time (datetime.datetime): 要判断的时间
返回值：
bool: True表示即将恢复交易，False表示不是
恢复交易时间：
12:59:30-13:00（下午开盘前30秒）
使用场景：
提前准备下午的交易数据
系统可以在这个时间点做一些准备工作
"""
def is_continue(now_time):
    now = now_time.time()
    for b, e in CONTINUE_TIME:
        if b <= now < e:
            return True
    return False


# 收盘时间
CLOSE_TIME = (
    datetime.time(15, 0, 0),  # 15:00（下午3点）
)


"""
判断是否即将收盘（尾盘时间）
参数说明：
now_time (datetime.datetime): 要判断的时间
start (datetime.time): 开始时间，默认14:54:30
返回值：
bool: True表示即将收盘，False表示不是
尾盘时间：
默认14:54:30-15:00（收盘前5分30秒）
使用场景：
- 尾盘抢筹数据统计
- 收盘前的交易策略
- 提醒用户即将收盘
使用示例：
from datetime import datetime
test_time = datetime(2024, 1, 1, 14, 55)
if is_closing(test_time):
print("即将收盘，请注意")
"""
def is_closing(now_time, start=datetime.time(14, 54, 30)):
    now = now_time.time()
    for close in CLOSE_TIME:
        if start <= now < close:
            return True
    return False


"""
判断是否已经收盘
参数说明：
now_time (datetime.datetime): 要判断的时间
返回值：
bool: True表示已收盘，False表示未收盘
收盘时间：
15:00之后
使用示例：
from datetime import datetime
now = datetime.now()
if is_close(now):
print("今日已收盘")
else:
print("今日未收盘")
"""
def is_close(now_time):
    now = now_time.time()
    for close in CLOSE_TIME:
        if now >= close:  # 大于等于15:00
            return True
    return False


"""
判断是否已经开盘
参数说明：
now_time (datetime.datetime): 要判断的时间
返回值：
bool: True表示已开盘，False表示未开盘
开盘时间：
9:30之后（正式开盘时间）
注意：9:15-9:30是集合竞价时间
使用示例：
from datetime import datetime
now = datetime.now()
if is_open(now):
print("今日已开盘")
"""
def is_open(now_time):
    now = now_time.time()
    if now >= datetime.time(9, 30, 0):  # 大于等于9:30
        return True
    return False


# ==================== 数据获取相关函数 ====================

"""
获取历史数据查询的时间区间
参数说明：
date (str): 结束日期，格式"YYYY-MM-DD"，如"2024-01-01"
返回值：
tuple: (开始日期字符串, 是否包含当前交易日标志)
- 开始日期：向前推3年的日期，格式"YYYYMMDD"
- 布尔值：True表示不包含当前交易日数据，False表示包含
功能说明：
计算查询历史数据的时间范围
开始日期 = 结束日期 - 3年
特殊处理：
- 如果结束日期是今天，且今天是交易日
- 且在开盘和收盘之间，则不包含今天的数据（因为今天还没收盘）
使用示例：
start, include_today = get_trade_hist_interval("2024-01-01")
print(f"查询{start}到2024-01-01的数据")
应用场景：
计算技术指标时需要一定时间范围的历史数据
如MA250（250日均线）需要至少250个交易日的数据
"""
def get_trade_hist_interval(date):
    # 解析日期字符串
    tmp_year, tmp_month, tmp_day = date.split("-")
    date_end = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day))
    
    # 计算开始日期：向前推3年（365*3天）
    # strftime：格式化日期为字符串，"%Y%m%d"表示格式为"20240101"
    date_start = (date_end + datetime.timedelta(days=-(365 * 3))).strftime("%Y%m%d")

    # 获取当前时间和日期
    now_time = datetime.datetime.now()
    now_date = now_time.date()
    
    # 标志变量：是否在交易日的开盘收盘之间
    is_trade_date_open_close_between = False
    
    # 判断结束日期是否是今天
    if date_end.date() == now_date:
        # 判断今天是否是交易日
        if is_trade_date(now_date):
            # 判断是否在开盘和收盘之间
            if is_open(now_time) and not is_close(now_time):
                is_trade_date_open_close_between = True

    # 返回：(开始日期, 是否排除今天)
    # not is_trade_date_open_close_between：
    #   如果在交易日的交易时间内，返回False（不排除今天）
    #   其他情况返回True（排除今天）
    return date_start, not is_trade_date_open_close_between


"""
获取最近的完整交易日
返回值：
tuple: (最近完整交易日, 最近完整交易日_含开盘未收盘)
返回值说明：
- 第一个日期：最近一个收盘的交易日
- 第二个日期：最近一个交易日（可能未收盘）
逻辑说明：
1. 如果今天是交易日：
- 如果已收盘：返回今天
- 如果未收盘但已开盘：返回(昨天, 今天)
- 如果未开盘：返回(昨天, 昨天)
2. 如果今天不是交易日：返回(上个交易日, 上个交易日)
使用示例：
last_date, last_date_nph = get_trade_date_last()
print(f"最近完整交易日：{last_date}")
print(f"最近交易日（可能未收盘）：{last_date_nph}")
应用场景：
- 获取最新的完整交易数据
- 判断应该抓取哪一天的数据
- nph = not open or not close（未开盘或未收盘）
"""
def get_trade_date_last():
    now_time = datetime.datetime.now()
    run_date = now_time.date()  # 今天的日期
    run_date_nph = run_date  # 默认两个日期相同
    
    # 判断今天是否是交易日
    if is_trade_date(run_date):
        # 如果今天还没收盘
        if not is_close(now_time):
            # 获取前一个交易日
            run_date = get_previous_trade_date(run_date)
            # 如果今天还没开盘
            if not is_open(now_time):
                run_date_nph = run_date
    else:
        # 今天不是交易日，获取前一个交易日
        run_date = get_previous_trade_date(run_date)
        run_date_nph = run_date
    
    return run_date, run_date_nph


# ==================== 财报日期相关函数 ====================

"""
获取最近的季度财报日期
返回值：
str: 季度财报日期，格式"YYYYMMDD"
季度财报规则：
- 1-3月：取上一年的年报（12月31日）
- 4-6月：取当年一季报（3月31日）
- 7-9月：取当年中报（6月30日）
- 10-12月：取当年三季报（9月30日）
财报发布时间：
- 一季报：4月发布
- 中报：8月发布
- 三季报：10月发布
- 年报：次年4月发布
使用示例：
report_date = get_quarterly_report_date()
print(f"最近财报日期：{report_date}")
应用场景：
查询上市公司的财务数据时需要指定报告期
"""
def get_quarterly_report_date():
    now_time = datetime.datetime.now()
    year = now_time.year
    month = now_time.month
    
    # 根据当前月份确定最近的财报日期
    if 1 <= month <= 3:
        month_day = '1231'  # 上一年年报
    elif 4 <= month <= 6:
        month_day = '0331'  # 一季报
    elif 7 <= month <= 9:
        month_day = '0630'  # 中报
    else:  # 10-12月
        month_day = '0930'  # 三季报
    
    return f"{year}{month_day}"


"""
获取最近的分红配送财报日期
返回值：
str: 分红配送财报日期，格式"YYYYMMDD"
分红配送规则：
分红配送通常基于年报（12-31）或中报（06-30）
逻辑：
- 2-6月：取上一年年报
- 8-12月：取当年中报
- 1月：根据日期判断
- 7月：根据日期判断（25日为分界点）
使用示例：
bonus_date = get_bonus_report_date()
print(f"最近分红财报日期：{bonus_date}")
应用场景：
查询股票分红配送信息
"""
def get_bonus_report_date():
    now_time = datetime.datetime.now()
    year = now_time.year
    month = now_time.month
    
    # 根据月份和日期确定分红财报日期
    if 2 <= month <= 6:
        # 2-6月：取上一年年报
        year -= 1
        month_day = '1231'
    elif 8 <= month <= 12:
        # 8-12月：取当年中报
        month_day = '0630'
    elif month == 7:
        # 7月：根据日期判断
        if now_time.day > 25:
            month_day = '0630'  # 25日后取中报
        else:
            year -= 1
            month_day = '1231'  # 25日前取上一年年报
    else:  # month == 1
        # 1月：根据日期判断
        year -= 1
        if now_time.day > 25:
            month_day = '1231'  # 25日后取上一年年报
        else:
            month_day = '0630'  # 25日前取上一年中报
    
    return f"{year}{month_day}"


"""
===========================================
交易时间模块使用总结（给Python新手）
===========================================

1. 核心概念
   - 交易日：股市开市的日期（排除周末和节假日）
   - 交易时间：每天9:30-11:30和13:00-15:00
   - 集合竞价：9:15-9:25
   
2. 常用函数
   - is_trade_date(): 判断是否是交易日【常用】
   - is_tradetime(): 判断是否在交易时间【常用】
   - get_previous_trade_date(): 获取上一个交易日【常用】
   - get_trade_date_last(): 获取最近完整交易日【常用】
   
3. 时间判断函数
   - is_open(): 是否已开盘（>=9:30）
   - is_close(): 是否已收盘（>=15:00）
   - is_closing(): 是否即将收盘（尾盘）
   - is_pause(): 是否午间休市
   
4. 财报相关
   - get_quarterly_report_date(): 季度财报日期
   - get_bonus_report_date(): 分红财报日期
   
5. 使用场景
   - 数据抓取：判断什么时候抓取数据
   - 实时交易：判断是否可以交易
   - 数据分析：获取正确的交易日期范围
   - 策略执行：根据时间触发不同的操作

6. 注意事项
   - 所有时间判断都基于中国A股市场规则
   - 交易日数据来自单例对象（只加载一次）
   - 时间比较使用datetime.time对象
   - 日期计算使用datetime.timedelta
"""
