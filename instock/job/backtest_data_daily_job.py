#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
回测数据任务模块（第七层）
==========================
这个模块负责对选股策略进行回测验证，计算收益率。

什么是回测？
- 回测就是用历史数据验证策略的有效性
- 假设我们在某天按策略买入股票
- 计算后续N天的收益率
- 验证策略是否真的能赚钱

为什么要回测？
- 验证策略：不是所有策略都有效
- 优化参数：找到最佳买入时机
- 评估风险：了解最大回撤
- 避免亏损：淘汰无效策略

回测指标：
- N日收益率：1日、3日、5日、10日、20日...100日
- 最大收益率：期间的最高收益
- 最小收益率：期间的最低收益（最大回撤）

回测对象：
- 技术指标买入信号（超买区域）
- 技术指标卖出信号（超卖区域）  
- 各种策略选股结果

数据流程：
策略选股结果 → 提取股票列表 → 计算历史收益率 → 更新到数据库

使用场景：
- 验证策略有效性
- 对比不同策略表现
- 优化策略参数
- 选择最佳策略
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import concurrent.futures  # 多线程并发
import pandas as pd  # 数据处理
import os.path  # 路径操作
import sys  # 系统操作
import datetime  # 日期时间

# ==================== 路径配置 ====================
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

# ==================== 导入项目模块 ====================
import instock.core.tablestructure as tbs  # 表结构定义
import instock.lib.database as mdb  # 数据库操作
import instock.core.backtest.rate_stats as rate  # 收益率计算
from instock.core.singleton_stock import stock_hist_data  # 历史数据单例

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 准备回测数据 ====================

"""
准备并执行回测任务
功能说明：
1. 获取所有股票的历史K线数据
2. 遍历所有需要回测的表
3. 对每个表进行并行回测
需要回测的表：
- TABLE_CN_STOCK_INDICATORS_BUY：技术指标买入信号
- TABLE_CN_STOCK_INDICATORS_SELL：技术指标卖出信号
- TABLE_CN_STOCK_STRATEGIES：各种策略选股结果（10个策略）
并行执行：
使用线程池同时处理多个表
提高回测速度
执行流程：
1. 获取历史K线数据（单例缓存）
2. 构建回测列定义
3. 遍历每个表，提交到线程池
4. 等待所有回测完成
"""
def prepare():
    # 步骤1: 构建需要回测的表列表
    # 技术指标买入和卖出信号表
    tables = [tbs.TABLE_CN_STOCK_INDICATORS_BUY, tbs.TABLE_CN_STOCK_INDICATORS_SELL]
    
    # 扩展策略表列表（10个策略）
    # TABLE_CN_STOCK_STRATEGIES是一个列表，包含所有策略表的定义
    tables.extend(tbs.TABLE_CN_STOCK_STRATEGIES)
    
    # 步骤2: 构建回测数据列定义
    # 回测列包括：date, code, rate_1, rate_3, rate_5, ..., rate_100
    backtest_columns = list(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
    backtest_columns.insert(0, 'code')  # 在开头插入code列
    backtest_columns.insert(0, 'date')  # 在开头插入date列
    backtest_column = backtest_columns
    
    # 步骤3: 获取所有股票的历史数据（单例模式，只加载一次）
    stocks_data = stock_hist_data().get_data()
    if stocks_data is None:
        # 没有历史数据，无法回测
        return
    
    # 步骤4: 从历史数据中提取日期
    # stocks_data是字典，键是(date, code, name)
    for k in stocks_data:
        date = k[0]  # 获取第一个股票的日期
        break  # 只需要一个日期即可（所有股票日期相同）
    
    # 步骤5: 使用线程池并行处理所有表
    # with语句确保线程池正确关闭
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for table in tables:
            # 为每个表提交一个回测任务
            # submit()返回Future对象，代表异步执行的结果
            executor.submit(process, table, stocks_data, date, backtest_column)
    
    # with语句结束时，会等待所有任务完成


# ==================== 处理单个表的回测 ====================

"""
处理单个表的回测任务
参数说明：
table (dict): 表结构定义，包含name和columns
data_all (dict): 所有股票的历史K线数据
- 键：(date, code, name)
- 值：DataFrame（历史K线）
date (str): 数据日期
backtest_column (list): 回测列名列表
功能说明：
1. 检查表是否存在
2. 查询需要回测的股票（回测数据为空的）
3. 并行计算每只股票的收益率
4. 更新回测结果到数据库
什么是需要回测的股票？
- 已经被策略选出
- 但回测数据还是NULL（空值）
- 需要补充计算收益率
SQL查询逻辑：
SELECT * FROM 表名 
WHERE date < 今天 
AND 最后一列 IS NULL
解释：
- date < 今天：只回测历史数据（今天的无法计算未来收益）
- 最后一列 IS NULL：最后一列是rate_100，如果为空说明没回测过
执行流程：
1. 检查表存在性
2. 查询待回测股票
3. 提取股票列表
4. 并行计算收益率
5. 更新数据库
"""
def process(table, data_all, date, backtest_column):
    # 步骤1: 获取表名
    table_name = table['name']
    
    # 步骤2: 检查表是否存在
    if not mdb.checkTableIsExist(table_name):
        # 表不存在，跳过
        return

    # 步骤3: 获取最后一列的列名（通常是rate_100）
    # tuple(table['columns'])：将列名字典转换为元组
    # [-1]：取最后一个元素
    column_tail = tuple(table['columns'])[-1]
    
    # 步骤4: 获取当前日期
    now_date = datetime.datetime.now().date()
    
    # 步骤5: 构建SQL查询语句
    # 查询条件：
    # 1. date < 今天（只回测历史）
    # 2. 最后一列 IS NULL（未回测过）
    sql = f"SELECT * FROM `{table_name}` WHERE `date` < '{now_date}' AND `{column_tail}` is NULL"
    
    try:
        # 步骤6: 执行查询，获取待回测的股票
        # read_sql()：执行SQL并返回DataFrame
        data = pd.read_sql(sql=sql, con=mdb.engine())
        
        # 检查是否有数据
        if data is None or len(data.index) == 0:
            # 没有需要回测的股票
            return

        # 步骤7: 提取股票的(date, code, name)信息
        # TABLE_CN_STOCK_FOREIGN_KEY：外键列定义，包含date, code, name
        subset = data[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
        
        # 将date列转换为字符串类型
        # astype()：类型转换
        subset = subset.astype({'date': 'string'})
        
        # 将DataFrame转换为元组列表
        # 格式：[(date, code, name), (date, code, name), ...]
        stocks = [tuple(x) for x in subset.values]

        # 步骤8: 并行计算收益率
        results = run_check(stocks, data_all, date, backtest_column)
        if results is None:
            # 计算失败，跳过
            return

        # 步骤9: 将计算结果转换为DataFrame
        # results是字典：{(date, code, name): Series数据}
        # values()取出所有Series
        # DataFrame()将多个Series合并为DataFrame
        data_new = pd.DataFrame(results.values())
        
        # 步骤10: 更新数据库
        # update_db_from_df()：根据date和code更新对应记录
        # ('date', 'code')：WHERE条件字段
        mdb.update_db_from_df(data_new, table_name, ('date', 'code'))

    except Exception as e:
        # 记录错误日志
        logging.error(f"backtest_data_daily_job.process处理异常：{table}表{e}")


# ==================== 并行计算收益率 ====================

"""
并行计算多只股票的收益率
参数说明：
stocks (list): 股票列表，格式[(date, code, name), ...]
data_all (dict): 所有股票的历史K线数据
date (str): 数据日期
backtest_column (list): 回测列名列表
workers (int): 线程池大小，默认40个线程
返回值：
dict: 计算结果字典
- 键：(date, code, name)
- 值：Series，包含收益率数据
并行计算原理：
假设有100只股票需要回测
- 单线程：需要串行计算100次
- 多线程（40线程）：同时计算40只，只需3轮
- 大大提高效率
执行流程：
1. 创建线程池（40个线程）
2. 为每只股票提交计算任务
3. 等待所有任务完成
4. 收集计算结果
5. 返回结果字典
为什么是40个线程？
- 太少：速度慢
- 太多：竞争激烈，反而慢
- 40是经验值，可以根据CPU调整
"""
def run_check(stocks, data_all, date, backtest_column, workers=40):
    # 步骤1: 准备存储结果的字典
    data = {}
    
    try:
        # 步骤2: 创建线程池
        # max_workers=40：最多同时运行40个线程
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            
            # 步骤3: 为每只股票提交计算任务
            # 字典推导式：{Future对象: 股票信息}
            future_to_data = {
                # executor.submit()：提交任务到线程池
                # 任务：调用rate.get_rates()计算收益率
                # 参数：
                #   stock: (date, code, name)
                #   data_all.get(...): 获取该股票的历史K线
                #   backtest_column: 列名列表
                #   len(backtest_column) - 1: 需要计算的天数
                executor.submit(
                    rate.get_rates,  # 要执行的函数
                    stock,  # 股票信息
                    data_all.get((date, stock[1], stock[2])),  # 历史K线数据
                    backtest_column,  # 列名
                    len(backtest_column) - 1  # 计算天数（列数-2，减去date和code）
                ): stock 
                for stock in stocks  # 遍历所有股票
            }
            
            # 步骤4: 等待任务完成并收集结果
            # as_completed()：按完成顺序返回Future对象
            for future in concurrent.futures.as_completed(future_to_data):
                # 获取对应的股票信息
                stock = future_to_data[future]
                
                try:
                    # future.result()：获取任务的返回值
                    # 会阻塞直到任务完成
                    _data_ = future.result()
                    
                    if _data_ is not None:
                        # 将结果存储到字典
                        data[stock] = _data_
                        
                except Exception as e:
                    # 单只股票计算失败，记录日志但不影响其他股票
                    logging.error(f"backtest_data_daily_job.run_check处理异常：{stock[1]}代码{e}")
                    
    except Exception as e:
        # 整体执行异常
        logging.error(f"backtest_data_daily_job.run_check处理异常：{e}")
    
    # 步骤5: 检查结果并返回
    if not data:
        # 没有任何结果
        return None
    else:
        # 返回结果字典
        return data


# ==================== 主函数 ====================

"""
回测任务主函数
功能说明：
调用prepare()执行回测任务
执行时机：
- 每天收盘后执行
- 由execute_daily_job.py调度
回测对象：
1. 技术指标买入信号
2. 技术指标卖出信号
3. 各种策略选股结果
回测结果：
更新到各个表的回测字段：
- rate_1: 1日收益率
- rate_3: 3日收益率
- rate_5: 5日收益率
- ...
- rate_100: 100日收益率
使用场景：
1. 策略验证：查看策略的历史表现
2. 策略优化：对比不同参数的效果
3. 策略选择：选出表现最好的策略
"""
def main():
    prepare()


# ==================== 程序入口 ====================
# main函数入口
if __name__ == '__main__':
    """
    直接运行此脚本时的入口
    
    运行方式：
        python backtest_data_daily_job.py
        
    注意：
        - 需要先有历史数据（singleton_stock.stock_hist_data）
        - 需要先有策略结果（strategy_data_daily_job）
        - 一般由execute_daily_job统一调度
    """
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()


"""
===========================================
回测数据任务模块使用总结（给Python新手）
===========================================

1. 核心概念
   - 回测：用历史数据验证策略
   - 收益率：买入后的盈亏百分比
   - 多线程：同时计算多只股票

2. 回测流程
   策略选股 → 提取股票列表 → 获取历史数据 → 计算收益率 → 更新数据库

3. 回测指标
   - 1日收益率：买入后1天的收益
   - 5日收益率：买入后5天的收益
   - 10日收益率：买入后10天的收益
   - ...最多计算100日

4. 并行计算
   - 单线程：100只股票需要100次计算
   - 多线程：40个线程同时计算，只需3轮
   - 效率提升：约40倍

5. SQL查询
   SELECT * FROM 表名 
   WHERE date < 今天 
   AND rate_100 IS NULL
   
   解释：
   - date < 今天：只回测历史
   - rate_100 IS NULL：未回测过

6. 数据结构
   输入：
   - stocks: [(date, code, name), ...]
   - data_all: {(date, code, name): DataFrame}
   
   输出：
   - {(date, code, name): Series(收益率数据)}

7. 使用场景
   - 验证策略：这个策略真的赚钱吗？
   - 对比策略：哪个策略更好？
   - 优化参数：什么时候买入最好？
   - 评估风险：最大回撤是多少？

8. 回测表
   - cn_stock_indicators_buy：技术指标买入信号
   - cn_stock_indicators_sell：技术指标卖出信号
   - cn_stock_strategy_*：各种策略结果

9. Python知识点
   - 多线程：concurrent.futures
   - DataFrame：pandas数据处理
   - 字典推导式：{expr: value for item in list}
   - as_completed()：按完成顺序获取结果

10. 注意事项
    - 回测不等于未来表现
    - 历史数据可能有幸存者偏差
    - 需要考虑交易成本
    - 回测周期不宜过短
"""
