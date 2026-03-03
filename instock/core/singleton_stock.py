#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据缓存模块（单例模式）
===========================
这个模块提供股票数据的单例缓存，避免重复获取相同的数据。

单例模式的好处：
1. 内存优化：数据只加载一次，所有地方共享使用
2. 性能提升：避免重复的网络请求和数据处理
3. 数据一致性：确保整个系统使用的是同一份数据

模块包含两个单例类：
1. stock_data：当天的股票数据（快照数据）
2. stock_hist_data：股票的历史K线数据（用于技术分析）

数据流程：
网络请求 → 数据抓取 → 单例缓存 → 多个模块共享使用
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import concurrent.futures  # 并发处理库，用于多线程
import instock.core.stockfetch as stf  # 股票数据抓取模块
import instock.core.tablestructure as tbs  # 数据表结构定义
import instock.lib.trade_time as trd  # 交易时间处理模块
from instock.lib.singleton_type import singleton_type  # 单例模式元类

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 当天股票数据单例类 ====================
class stock_data(metaclass=singleton_type):
    """
    当天股票数据的单例类
    
    功能说明：
        - 获取指定日期的所有股票的快照数据
        - 包括：代码、名称、价格、涨跌幅、成交量、市值等
        - 使用单例模式，同一个日期的数据只加载一次
        
    使用示例：
        from datetime import date
        
        # 第一次调用：从网络抓取数据
        stock_obj = stock_data(date(2024, 1, 1))
        df = stock_obj.get_data()
        
        # 第二次调用：直接返回缓存的数据（不会重新抓取）
        stock_obj2 = stock_data(date(2024, 1, 1))
        df2 = stock_obj2.get_data()  # df2和df是同一个对象
        
    单例模式实现：
        通过metaclass=singleton_type实现
        相同参数的多次实例化会返回同一个对象
    """
    
    def __init__(self, date):
        """
        初始化股票数据单例
        
        参数说明：
            date (datetime.date): 要获取数据的日期
            
        执行流程：
            1. 调用stockfetch模块的fetch_stocks函数
            2. 从网络抓取指定日期的所有股票数据
            3. 返回pandas DataFrame格式的数据
            4. 存储到self.data中
            
        异常处理：
            如果抓取失败，记录错误日志，但不影响程序运行
        """
        try:
            # 调用抓取函数获取股票数据
            # 返回DataFrame：包含所有股票的当日数据
            self.data = stf.fetch_stocks(date)
        except Exception as e:
            # 记录错误日志
            logging.error(f"singleton.stock_data处理异常：{e}")

    def get_data(self):
        """
        获取股票数据
        
        返回值：
            pandas.DataFrame: 股票数据表
                - 行：每只股票
                - 列：股票的各项指标（代码、名称、价格等）
                
        DataFrame结构示例：
            code    name    new_price  change_rate  volume  ...
            600000  浦发银行   10.5      1.5          1000000  ...
            600001  邯郸钢铁   5.2       -0.8         500000   ...
            ...
        """
        return self.data


# ==================== 股票历史数据单例类 ====================
class stock_hist_data(metaclass=singleton_type):
    """
    股票历史K线数据的单例类
    
    功能说明：
        - 获取多只股票的历史K线数据
        - 使用多线程并发获取，提高效率
        - 数据用于计算技术指标、K线形态识别、策略选股等
        
    K线数据包含：
        - 日期
        - 开盘价、最高价、最低价、收盘价（OHLC）
        - 成交量、成交额
        - 涨跌幅等
        
    多线程说明：
        假设要获取3000只股票的历史数据
        - 单线程：需要3000次网络请求，耗时很长
        - 多线程：同时发起16个请求，大大缩短时间
        
    使用示例：
        # 获取所有股票的历史数据
        hist_obj = stock_hist_data(date='2024-01-01')
        hist_dict = hist_obj.get_data()
        
        # hist_dict是字典，键是(日期,代码)，值是DataFrame
        for (date, code), df in hist_dict.items():
            print(f"{code}的历史数据：")
            print(df.head())  # 显示前5行
    """
    
    def __init__(self, date=None, stocks=None, workers=16):
        """
        初始化股票历史数据单例
        
        参数说明：
            date (str, 可选): 结束日期，格式"YYYY-MM-DD"
                              历史数据会获取[结束日期-3年, 结束日期]的数据
            stocks (list, 可选): 股票列表，格式[(date, code, name), ...]
                                如果为None，则获取当天所有股票的历史数据
            workers (int): 线程池大小，默认16个线程
                          - 线程越多，速度越快，但占用资源也越多
                          - 建议根据CPU核心数和网络情况调整
                          
        执行流程：
            1. 如果没有指定stocks，从当天数据中获取所有股票列表
            2. 计算历史数据的时间范围（向前推3年）
            3. 创建线程池，并发获取每只股票的历史数据
            4. 将结果存储到字典中
        """
        # 步骤1: 如果没有指定股票列表，获取当天所有股票
        if stocks is None:
            # 从当天股票数据中提取需要的列（日期、代码、名称）
            _subset = stock_data(date).get_data()[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
            # 将DataFrame转换为元组列表：[(date, code, name), ...]
            # values：获取DataFrame的值（numpy数组）
            # [tuple(x) for x in ...]：将每一行转换为元组
            stocks = [tuple(x) for x in _subset.values]
        
        # 检查股票列表是否为空
        if stocks is None:
            self.data = None
            return
        
        # 步骤2: 计算历史数据的时间范围
        # 只计算一次，提高效率（所有股票使用相同的时间范围）
        # stocks[0][0]：第一只股票的日期
        date_start, is_cache = trd.get_trade_hist_interval(stocks[0][0])
        
        # 步骤3: 准备存储结果的字典
        _data = {}  # 键：(date, code)，值：历史数据DataFrame
        
        try:
            # 步骤4: 创建线程池并发获取数据
            # ThreadPoolExecutor：线程池管理器
            # max_workers：线程池大小，如果为None则默认为CPU核心数*5
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                # 提交所有任务到线程池
                # executor.submit()：提交一个任务
                # 返回Future对象，代表异步执行的结果
                # 字典推导式：{Future对象: 股票信息}
                future_to_stock = {
                    executor.submit(stf.fetch_stock_hist, stock, date_start, is_cache): stock 
                    for stock in stocks
                }
                
                # 步骤5: 等待所有任务完成并收集结果
                # as_completed()：按完成顺序返回Future对象
                # 不是按提交顺序，而是谁先完成谁先返回
                for future in concurrent.futures.as_completed(future_to_stock):
                    stock = future_to_stock[future]  # 获取对应的股票信息
                    try:
                        # future.result()：获取任务的返回值
                        # 会阻塞直到任务完成
                        __data = future.result()
                        if __data is not None:
                            # 将结果存储到字典中
                            # stock是元组(date, code, name)
                            _data[stock] = __data
                    except Exception as e:
                        # 单只股票出错不影响其他股票
                        # stock[1]是代码
                        logging.error(f"singleton.stock_hist_data处理异常：{stock[1]}代码{e}")
        except Exception as e:
            # 线程池整体出错
            logging.error(f"singleton.stock_hist_data处理异常：{e}")
        
        # 步骤6: 检查结果并存储
        if not _data:
            # 如果没有任何数据，设置为None
            self.data = None
        else:
            # 存储获取的数据
            self.data = _data

    def get_data(self):
        """
        获取股票历史数据字典
        
        返回值：
            dict: 历史数据字典
                - 键：(date, code, name) 元组
                - 值：pandas.DataFrame，该股票的历史K线数据
                
        DataFrame结构示例：
            date        open   high   low    close  volume    ...
            2021-01-01  10.0   10.5   9.8    10.2   1000000   ...
            2021-01-02  10.2   10.8   10.1   10.5   1200000   ...
            ...
            
        使用示例：
            hist_data = stock_hist_data(date='2024-01-01').get_data()
            
            # 遍历所有股票
            for (date, code, name), df in hist_data.items():
                print(f"{name}({code})的历史数据：")
                print(f"数据长度：{len(df)}天")
                print(f"最新收盘价：{df['close'].iloc[-1]}")
        """
        return self.data


"""
===========================================
股票数据单例模块使用总结（给Python新手）
===========================================

1. 单例模式的概念
   - 确保一个类只有一个实例
   - 多次创建返回同一个对象
   - 数据共享，节省内存

2. 两个单例类
   - stock_data：当天股票快照数据
   - stock_hist_data：股票历史K线数据

3. 使用场景
   - 计算技术指标：需要历史K线数据
   - K线形态识别：需要历史K线数据
   - 策略选股：需要当天数据和历史数据
   - Web展示：需要当天数据

4. 多线程概念
   - 并发执行：同时做多件事
   - 提高效率：特别是I/O密集型任务（网络请求）
   - ThreadPoolExecutor：Python的线程池管理器

5. 性能优化
   - 单例模式：避免重复加载数据
   - 多线程：并发获取数据，提高速度
   - 缓存机制：历史数据可以缓存到本地

6. 注意事项
   - 数据量较大，注意内存使用
   - 网络请求可能失败，有异常处理
   - 线程数不是越多越好，要根据实际情况调整
"""
