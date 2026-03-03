#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
策略选股任务模块（第六层核心）
==============================
这个模块负责运行所有选股策略，筛选出符合条件的股票。

什么是选股策略？
- 根据特定条件筛选股票的方法
- 量化：用代码定义筛选规则
- 系统化：可重复、可验证
- 自动化：程序自动执行

系统内置的10种策略：
1. 放量上涨（enter.py）
2. 海龟交易法则（turtle_trade.py）
3. 放量跌停（climax_limitdown.py）
4. 低ATR成长（low_atr.py）
5. 回踩年线（backtrace_ma250.py）
6. 突破平台（breakthrough_platform.py）
7. 停机坪（parking_apron.py）
8. 无大幅回撤（low_backtrace_increase.py）
9. 均线多头（keep_increasing.py）
10. 高而窄的旗形（high_tight_flag.py）

策略分类：
- 趋势类：海龟交易、突破平台、均线多头
- 形态类：停机坪、高而窄的旗形
- 量价类：放量上涨、放量跌停
- 回撤类：回踩年线、无大幅回撤
- 成长类：低ATR成长

数据流程：
基础数据 → 历史K线 → 
遍历所有股票 → 应用策略函数 → 
筛选符合的股票 → 保存结果 → 
回测验证 → Web展示

并行执行：
- 使用多线程同时运行所有策略
- 每个策略独立执行
- 大大缩短总时间

运行时机：
- 收盘后运行
- 建议17:30后
- 确保基础数据完整
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import concurrent.futures  # 多线程并发
import pandas as pd  # 数据处理
import os.path  # 路径操作
import sys  # 系统操作

# ==================== 路径配置 ====================
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

# ==================== 导入项目模块 ====================
import instock.lib.run_template as runt  # 任务运行模板
import instock.core.tablestructure as tbs  # 表结构定义
import instock.lib.database as mdb  # 数据库操作
from instock.core.singleton_stock import stock_hist_data  # 历史数据单例
from instock.core.stockfetch import fetch_stock_top_entity_data  # 龙虎榜机构数据

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 准备并执行单个策略 ====================

"""
准备并执行单个选股策略
参数说明：
date (datetime.date): 执行日期
strategy (dict): 策略定义字典
- name：策略表名
- func：策略函数
- cn：策略中文名
功能说明：
1. 获取所有股票的历史数据
2. 并行执行策略函数，筛选股票
3. 保存符合条件的股票到数据库
执行流程：
1. 从单例获取历史数据
2. 调用策略函数筛选
3. 构建结果DataFrame
4. 删除旧数据
5. 插入新数据
6. 添加回测列
为什么要添加回测列？
- 策略选出的股票需要回测
- 验证策略是否真的有效
- 计算未来N日的收益率
- 评估策略表现
使用场景：
main函数中调用
为每个策略执行一次
"""
def prepare(date, strategy):
    try:
        # ==================== 步骤1: 获取历史数据 ====================
        # 从单例获取所有股票的历史K线
        stocks_data = stock_hist_data(date=date).get_data()
        if stocks_data is None:
            # 没有历史数据，无法执行策略
            logging.warning(f"没有历史数据，无法执行策略：{strategy['cn']}")
            return
        
        # ==================== 步骤2: 获取策略信息 ====================
        table_name = strategy['name']  # 策略表名，如'cn_stock_strategy_turtle'
        strategy_func = strategy['func']  # 策略函数，如check_enter
        
        # ==================== 步骤3: 并行执行策略 ====================
        # run_check()：使用多线程遍历所有股票，应用策略函数
        results = run_check(strategy_func, table_name, stocks_data, date)
        
        if results is None:
            # 没有符合条件的股票
            logging.info(f"策略{strategy['cn']}未筛选到股票：{date}")
            return

        # ==================== 步骤4: 删除旧数据 ====================
        # 删除该日期的旧数据（避免重复）
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            # 表不存在，获取字段类型
            # 所有策略表结构相同，都用TABLE_CN_STOCK_STRATEGIES[0]
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_STRATEGIES[0]['columns'])

        # ==================== 步骤5: 构建DataFrame ====================
        # results是列表：[(date, code, name), ...]
        data = pd.DataFrame(results)
        
        # 设置列名
        columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])  # date, code, name
        data.columns = columns
        
        # ==================== 步骤6: 添加回测列 ====================
        # 添加空的回测数据列（rate_1, rate_3, ...）
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        
        # ==================== 步骤7: 日期处理 ====================
        # 确保日期正确（单例模式可能有问题）
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            data['date'] = date_str
        
        # ==================== 步骤8: 插入数据库 ====================
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"策略{strategy['cn']}执行完成：{date}，选出{len(data)}只股票")

    except Exception as e:
        logging.error(f"strategy_data_daily_job.prepare处理异常：{strategy}策略{e}")


# ==================== 并行执行策略筛选 ====================

"""
并行执行策略函数，筛选所有股票
参数说明：
strategy_fun (function): 策略函数
- 如：check_enter, check_volume等
- 接收(code_name, data, date)
- 返回True或False
table_name (str): 策略表名
- 用于日志记录
stocks (dict): 所有股票的历史数据
- 键：(date, code, name)
- 值：DataFrame（历史K线）
date (datetime.date): 执行日期
workers (int): 线程池大小，默认40
返回值：
list: 符合策略的股票列表
- 格式：[(date, code, name), ...]
- None表示没有符合的股票
功能说明：
1. 检查是否是特殊策略（高而窄的旗形）
2. 创建线程池
3. 为每只股票提交策略检查任务
4. 收集返回True的股票
5. 返回符合条件的股票列表
特殊处理：高而窄的旗形策略
- 需要额外的龙虎榜机构数据
- 筛选近期有机构买入的股票
- 提高策略准确性
并行执行原理：
- 4000只股票
- 40个线程同时检查
- 每个线程处理100只
- 大大提高速度
"""
def run_check(strategy_fun, table_name, stocks, date, workers=40):
    # ==================== 步骤1: 特殊策略检查 ====================
    # 检查是否是"高而窄的旗形"策略
    is_check_high_tight = False
    if strategy_fun.__name__ == 'check_high_tight':
        # 获取近期有机构买入的股票代码集合
        stock_tops = fetch_stock_top_entity_data(date)
        if stock_tops is not None:
            is_check_high_tight = True
    
    # ==================== 步骤2: 准备结果列表 ====================
    data = []  # 存储符合条件的股票
    
    try:
        # ==================== 步骤3: 创建线程池 ====================
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            
            # ==================== 步骤4: 提交所有检查任务 ====================
            if is_check_high_tight:
                # 高而窄的旗形策略：需要额外参数istop
                # istop：是否近期有机构买入
                future_to_data = {
                    executor.submit(
                        strategy_fun,  # 策略函数
                        k,  # 股票键(date, code, name)
                        stocks[k],  # 历史K线
                        date=date,  # 日期
                        istop=(k[1] in stock_tops)  # 是否有机构买入
                    ): k 
                    for k in stocks
                }
            else:
                # 普通策略：不需要额外参数
                future_to_data = {
                    executor.submit(
                        strategy_fun,  # 策略函数
                        k,  # 股票键
                        stocks[k],  # 历史K线
                        date=date  # 日期
                    ): k 
                    for k in stocks
                }
            
            # ==================== 步骤5: 收集符合条件的股票 ====================
            # 等待所有任务完成
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]  # 获取对应的股票
                try:
                    # future.result()：获取策略函数的返回值
                    if future.result():
                        # 返回True，说明符合策略
                        data.append(stock)  # 添加到结果列表
                        
                except Exception as e:
                    # 单只股票检查失败，记录日志
                    logging.error(f"strategy_data_daily_job.run_check处理异常：{stock[1]}代码{e}策略{table_name}")
                    
    except Exception as e:
        # 整体执行异常
        logging.error(f"strategy_data_daily_job.run_check处理异常：{e}策略{table_name}")
    
    # ==================== 步骤6: 检查结果并返回 ====================
    if not data:
        # 没有符合条件的股票
        return None
    else:
        # 返回股票列表
        return data


# ==================== 主函数 ====================

"""
策略选股任务主函数
功能说明：
并行执行所有10种选股策略
执行流程：
1. 创建线程池
2. 为每个策略提交执行任务
3. 等待所有策略完成
并行执行：
- 10个策略同时执行
- 每个策略使用40个线程处理股票
- 两级并行：策略级+股票级
为什么并行？
串行执行：
- 10个策略 × 每个10分钟 = 100分钟
并行执行：
- 最慢的策略决定总时间
- 约10-15分钟
效率提升：约7-10倍
运行方式：
# 执行今天的策略
python strategy_data_daily_job.py
# 执行指定日期
python strategy_data_daily_job.py 2024-01-01
# 批量执行
python strategy_data_daily_job.py 2024-01-01 2024-01-31
产生的数据表：
- cn_stock_strategy_enter：放量上涨
- cn_stock_strategy_turtle：海龟交易
- cn_stock_strategy_climax：放量跌停
- cn_stock_strategy_low_atr：低ATR成长
- cn_stock_strategy_backtrace_ma250：回踩年线
- cn_stock_strategy_breakthrough：突破平台
- cn_stock_strategy_parking：停机坪
- cn_stock_strategy_low_backtrace：无大幅回撤
- cn_stock_strategy_keep_increasing：均线多头
- cn_stock_strategy_high_tight：高而窄的旗形
后续任务：
- backtest_data_daily_job：回测验证
- Web展示：查看策略结果
"""
def main():
    # 使用线程池并行执行所有策略
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # 遍历所有策略定义
        # TABLE_CN_STOCK_STRATEGIES：策略列表，包含10个策略
        for strategy in tbs.TABLE_CN_STOCK_STRATEGIES:
            # 为每个策略提交执行任务
            # runt.run_with_args()：处理命令行参数
            # prepare：策略执行函数
            # strategy：策略定义参数
            executor.submit(runt.run_with_args, prepare, strategy)
    
    # with语句结束时，等待所有策略完成
    logging.info("所有策略执行完成")


# ==================== 程序入口 ====================
# main函数入口
if __name__ == '__main__':
    """
    直接运行此脚本时的入口
    
    前置条件：
        1. 已有基础数据（basic_data_daily_job）
        2. 已有历史K线（stock_hist_data）
        
    产生数据：
        10个策略表，每个表包含符合该策略的股票
        
    后续使用：
        1. 回测验证（backtest_data_daily_job）
        2. Web查看（策略选股页面）
        3. 对比分析（哪个策略更好）
    """
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()


"""
===========================================
策略选股任务模块使用总结（给Python新手）
===========================================

1. 模块定位
   - 第六层：选股策略层
   - 核心功能：应用策略筛选股票
   - 10种内置策略

2. 策略执行流程
   获取历史数据 → 遍历所有股票 → 
   应用策略函数 → 筛选符合的 → 
   保存到数据库 → 等待回测

3. 并行执行
   两级并行：
   
   第一级（策略级）：
   - 10个策略同时执行
   - 互不影响
   - ThreadPoolExecutor
   
   第二级（股票级）：
   - 每个策略内部
   - 40个线程处理股票
   - 提高单个策略速度
   
   总效率：
   - 单线程：100+分钟
   - 两级并行：10-15分钟
   - 提升：约10倍

4. 策略函数接口
   标准接口：
   ```python
   def check_xxx(code_name, data, date=None, threshold=60):
       # code_name: (date, code, name)
       # data: DataFrame（历史K线）
       # date: 计算日期
       # threshold: 最少数据天数
       
       # 返回：
       # True：符合策略
       # False：不符合策略
   ```
   
   特殊接口（高而窄的旗形）：
   ```python
   def check_high_tight(code_name, data, date=None, istop=False):
       # 额外参数istop：是否有机构买入
   ```

5. 策略表结构
   所有策略表包含相同的列：
   - date：日期
   - code：代码
   - name：名称
   - rate_1, rate_3, ..., rate_100：回测收益率

6. 数据流向
   策略筛选结果 → 策略表 → 
   回测任务读取 → 计算收益率 → 
   更新回测列 → Web展示

7. run_check函数
   功能：
   - 并行应用策略到所有股票
   - 收集符合条件的股票
   
   返回：
   - list：[(date, code, name), ...]
   - None：没有符合的股票

8. 特殊策略处理
   高而窄的旗形：
   - 需要龙虎榜数据
   - 筛选有机构关注的股票
   - 提高准确率
   
   实现：
   ```python
   if strategy_fun.__name__ == 'check_high_tight':
       stock_tops = fetch_stock_top_entity_data(date)
       istop = (code in stock_tops)
   ```

9. 使用场景
   每日选股：
   - 收盘后运行
   - 筛选符合策略的股票
   - 供第二天参考
   
   历史回测：
   - 批量执行历史日期
   - 验证策略有效性
   - 优化策略参数
   
   实盘交易：
   - 查看策略结果
   - 结合其他分析
   - 制定交易计划

10. 策略对比
    查看结果：
    - Web界面：策略选股菜单
    - 数据库：各策略表
    
    对比方法：
    - 选出股票数量
    - 回测收益率
    - 胜率统计
    - 最大回撤
    
    选择策略：
    - 综合表现最好的
    - 适合当前市场的
    - 自己理解的

11. Python知识点
    - 两级并行：嵌套ThreadPoolExecutor
    - 函数对象：strategy['func']
    - 函数名：func.__name__
    - 列表推导：[x for x in list if cond]
    - 动态调用：submit(func, args)

12. 常见问题
    Q: 为什么有的策略选不出股票？
    A: 当天可能没有符合条件的股票
    
    Q: 策略结果如何查看？
    A: Web界面或数据库表
    
    Q: 如何添加新策略？
    A: 参考enter.py编写，在tablestructure.py注册
    
    Q: 运行很慢怎么办？
    A: 增加workers数量，或减少策略

13. 优化建议
    - 缓存历史数据：避免重复加载
    - 筛选股票范围：只检查活跃股票
    - 策略预筛选：先用简单条件过滤
    - 调整线程数：根据CPU调整

14. 策略组合
    - 多策略组合：提高胜率
    - 取交集：都符合的股票
    - 取并集：任一符合的股票
    - 加权评分：不同策略不同权重
"""
