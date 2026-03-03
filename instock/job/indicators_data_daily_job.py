#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
技术指标计算任务模块（第三层核心）
===================================
这个模块负责计算所有股票的技术指标，并筛选出买入/卖出信号。

什么是技术指标？
- 通过数学公式计算的技术分析工具
- 基于价格、成交量等历史数据
- 帮助判断买入卖出时机
- 如：MACD、KDJ、RSI、BOLL等

本模块计算的指标（32种）：
1. MACD（指数平滑异同移动平均线）
2. KDJ（随机指标）
3. BOLL（布林带）
4. RSI（相对强弱指标）
5. CR（能量指标）
6. VR（成交量比率）
7. ATR（真实波幅）
8. DMI（趋向指标）
9. W&R（威廉指标）
10. CCI（顺势指标）
... 共32种

技术指标的作用：
- 判断超买超卖：KDJ、RSI
- 判断趋势方向：MACD、DMI
- 判断支撑压力：BOLL
- 判断波动：ATR
- 判断量价关系：VR、OBV

买入卖出信号：
买入信号（超卖区域）：
- KDJ < 20：超卖
- RSI < 20：超卖
- CCI < -100：超卖
- CR < 40：超卖
- W&R < -80：超卖
- VR < 40：超卖

卖出信号（超买区域）：
- KDJ > 80：超买
- RSI > 80：超买
- CCI > 100：超买
- CR > 300：超买
- W&R > -20：超买
- VR > 160：超买

数据流程：
基础数据 → 历史K线 → 计算指标 → 筛选信号 → 保存结果

运行时机：
- 收盘后运行（需要完整的历史数据）
- 建议：17:30后
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
import instock.core.indicator.calculate_indicator as idr  # 指标计算模块
from instock.core.singleton_stock import stock_hist_data  # 历史数据单例

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 准备并计算指标 ====================

"""
准备并计算所有股票的技术指标
参数说明：
date (datetime.date): 计算日期
功能说明：
1. 获取所有股票的历史K线数据
2. 并行计算每只股票的32种技术指标
3. 合并结果并保存到数据库
执行流程：
1. 从单例获取历史数据（已缓存）
2. 并行计算指标（多线程）
3. 构建结果DataFrame
4. 删除旧数据
5. 插入新数据
数据量：
- 股票数量：约4000只
- 指标数量：32种
- 计算时间：约5-10分钟（多线程）
为什么需要历史数据？
- 技术指标需要一定周期的数据
- 如MA20需要20天数据
- 如MA250需要250天数据
- 默认获取3年历史数据
"""
def prepare(date):
    try:
        # 步骤1: 获取所有股票的历史K线数据
        # stock_hist_data()：单例模式，只加载一次
        # get_data()：返回字典 {(date, code, name): DataFrame}
        stocks_data = stock_hist_data(date=date).get_data()
        
        # 检查数据是否有效
        if stocks_data is None:
            # 没有历史数据，无法计算指标
            logging.warning(f"没有历史数据，无法计算指标：{date}")
            return
        
        # 步骤2: 并行计算所有股票的指标
        # run_check()：使用多线程计算
        # 返回字典：{(date, code, name): Series(指标数据)}
        results = run_check(stocks_data, date=date)
        
        if results is None:
            # 计算失败
            logging.warning(f"指标计算失败：{date}")
            return

        # 步骤3: 获取表名
        table_name = tbs.TABLE_CN_STOCK_INDICATORS['name']  # 'cn_stock_indicators'
        
        # 步骤4: 删除旧数据（如果表存在）
        if mdb.checkTableIsExist(table_name):
            # 表存在，删除该日期的旧数据
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            # 表不存在，获取字段类型
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_INDICATORS['columns'])

        # 步骤5: 构建DataFrame
        # results.keys()：所有股票的(date, code, name)
        dataKey = pd.DataFrame(results.keys())
        _columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        dataKey.columns = _columns  # 设置列名：date, code, name
        
        # results.values()：所有股票的指标数据（Series）
        dataVal = pd.DataFrame(results.values())
        
        # 删除date列（因为dataKey已经有了）
        # axis=1：按列删除
        # inplace=True：直接修改原DataFrame
        dataVal.drop('date', axis=1, inplace=True)

        # 步骤6: 合并两个DataFrame
        # merge()：类似SQL的JOIN
        # on=['code']：按code列合并
        # how='left'：左连接，保留左边所有行
        data = pd.merge(dataKey, dataVal, on=['code'], how='left')
        
        # 步骤7: 日期处理
        # 确保date列是正确的日期（单例模式下可能有问题）
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            # 如果日期不匹配，更新为正确的日期
            data['date'] = date_str
        
        # 步骤8: 插入数据到数据库
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"指标计算完成：{date}，共{len(data)}只股票")

    except Exception as e:
        logging.error(f"indicators_data_daily_job.prepare处理异常：{e}")


# ==================== 并行计算指标 ====================

"""
并行计算多只股票的技术指标
参数说明：
stocks (dict): 股票历史数据字典
- 键：(date, code, name)
- 值：DataFrame（历史K线）
date (datetime.date): 计算日期
workers (int): 线程池大小，默认40
返回值：
dict: 计算结果字典
- 键：(date, code, name)
- 值：Series（指标数据）
并行计算原理：
- 单线程：4000只股票串行计算，很慢
- 多线程：40个线程同时计算，快很多
- CPU密集型：主要是数学计算
执行流程：
1. 构建列名列表
2. 创建线程池
3. 提交所有计算任务
4. 等待完成并收集结果
5. 返回结果字典
"""
def run_check(stocks, date=None, workers=40):
    # 步骤1: 准备存储结果的字典
    data = {}
    
    # 步骤2: 构建列名列表
    # STOCK_STATS_DATA：指标数据结构定义
    columns = list(tbs.STOCK_STATS_DATA['columns'])
    columns.insert(0, 'code')  # 在开头插入code
    columns.insert(0, 'date')  # 在开头插入date
    data_column = columns  # 列名列表
    
    try:
        # 步骤3: 创建线程池
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            
            # 步骤4: 为每只股票提交计算任务
            # 字典推导式：{Future: 股票键}
            future_to_data = {
                # executor.submit()：提交任务
                # idr.get_indicator：指标计算函数
                # k：股票键(date, code, name)
                # stocks[k]：该股票的历史K线DataFrame
                # data_column：列名列表
                # date：计算日期
                executor.submit(idr.get_indicator, k, stocks[k], data_column, date=date): k 
                for k in stocks  # 遍历所有股票
            }
            
            # 步骤5: 等待任务完成并收集结果
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]  # 获取对应的股票键
                try:
                    _data_ = future.result()  # 获取计算结果
                    if _data_ is not None:
                        data[stock] = _data_  # 存储结果
                except Exception as e:
                    # 单只股票计算失败，记录日志
                    logging.error(f"indicators_data_daily_job.run_check处理异常：{stock[1]}代码{e}")
                    
    except Exception as e:
        # 整体执行异常
        logging.error(f"indicators_data_daily_job.run_check处理异常：{e}")
    
    # 步骤6: 检查结果并返回
    if not data:
        return None
    else:
        return data


# ==================== 筛选买入信号 ====================

"""
根据技术指标筛选买入信号（超卖区域）
参数说明：
date (datetime.date): 筛选日期
功能说明：
从已计算的指标中筛选符合买入条件的股票
买入条件（所有条件同时满足）：
1. KDJ_K >= 80：K值进入超买区
2. KDJ_D >= 70：D值进入超买区
3. KDJ_J >= 100：J值超过100
4. RSI_6 >= 80：6日RSI超买
5. CCI >= 100：CCI进入超买区
6. CR >= 300：CR能量指标高位
7. WR_6 >= -20：威廉指标超买
8. VR >= 160：成交量比率高位
为什么这样设置？
- 多个指标共振：提高准确率
- 超买区域：股价可能回调
- 短线交易：寻找高位卖出机会
注意：
- 这是简单筛选策略
- 实际应用需结合其他因素
- 回测验证策略有效性
数据流程：
指标数据 → SQL筛选 → 买入信号表 → 回测验证
"""
def guess_buy(date):
    try:
        # 步骤1: 获取指标表名
        _table_name = tbs.TABLE_CN_STOCK_INDICATORS['name']
        
        # 步骤2: 检查表是否存在
        if not mdb.checkTableIsExist(_table_name):
            # 表不存在，说明还没计算指标
            logging.warning(f"指标表不存在，无法筛选买入信号：{date}")
            return

        # 步骤3: 构建查询的列名
        _columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])  # date, code, name
        _selcol = '`,`'.join(_columns)  # 用反引号和逗号连接：`date`,`code`,`name`
        
        # 步骤4: 构建SQL查询语句
        # 筛选条件：所有指标都达到超买阈值
        sql = f'''SELECT `{_selcol}` FROM `{_table_name}` WHERE `date` = '{date}' and 
                `kdjk` >= 80 and `kdjd` >= 70 and `kdjj` >= 100 and `rsi_6` >= 80 and 
                `cci` >= 100 and `cr` >= 300 and `wr_6` >= -20 and `vr` >= 160'''
        
        # 步骤5: 执行查询
        data = pd.read_sql(sql=sql, con=mdb.engine())
        
        # 步骤6: 去重（按code）
        # subset="code"：根据code列判断重复
        # keep="last"：保留最后一个
        data = data.drop_duplicates(subset="code", keep="last")
        
        # 检查是否有结果
        if len(data.index) == 0:
            # 没有符合条件的股票
            logging.info(f"没有买入信号：{date}")
            return

        # 步骤7: 准备保存到买入信号表
        table_name = tbs.TABLE_CN_STOCK_INDICATORS_BUY['name']  # 'cn_stock_indicators_buy'
        
        # 删除旧数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_INDICATORS_BUY['columns'])

        # 步骤8: 添加回测数据列（空值，待回测填充）
        # TABLE_CN_STOCK_BACKTEST_DATA：回测数据列定义
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        # concat()：连接DataFrame
        # pd.DataFrame(columns=...)：创建空DataFrame（只有列名）
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        
        # 步骤9: 插入数据到买入信号表
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"买入信号筛选完成：{date}，共{len(data)}只股票")
        
    except Exception as e:
        logging.error(f"indicators_data_daily_job.guess_buy处理异常：{e}")


# ==================== 筛选卖出信号 ====================

"""
根据技术指标筛选卖出信号（超卖区域）
参数说明：
date (datetime.date): 筛选日期
功能说明：
从已计算的指标中筛选符合卖出条件的股票
卖出条件（所有条件同时满足）：
1. KDJ_K < 20：K值进入超卖区
2. KDJ_D < 30：D值进入超卖区
3. KDJ_J < 10：J值低于10
4. RSI_6 < 20：6日RSI超卖
5. CCI < -100：CCI进入超卖区
6. CR < 40：CR能量指标低位
7. WR_6 < -80：威廉指标超卖
8. VR < 40：成交量比率低位
为什么这样设置？
- 超卖区域：股价可能反弹
- 多指标共振：提高准确率
- 短线交易：寻找低位买入机会
与guess_buy的区别：
- 买入信号：超买区域（高位）
- 卖出信号：超卖区域（低位）
- 策略相反
数据流程：
指标数据 → SQL筛选 → 卖出信号表 → 回测验证
"""
def guess_sell(date):
    try:
        # 步骤1: 获取指标表名
        _table_name = tbs.TABLE_CN_STOCK_INDICATORS['name']
        
        # 步骤2: 检查表是否存在
        if not mdb.checkTableIsExist(_table_name):
            logging.warning(f"指标表不存在，无法筛选卖出信号：{date}")
            return

        # 步骤3: 构建查询列名
        _columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        _selcol = '`,`'.join(_columns)
        
        # 步骤4: 构建SQL查询语句（超卖条件）
        sql = f'''SELECT `{_selcol}` FROM `{_table_name}` WHERE `date` = '{date}' and 
                `kdjk` < 20 and `kdjd` < 30 and `kdjj` < 10 and `rsi_6` < 20 and 
                `cci` < -100 and `cr` < 40 and `wr_6` < -80 and `vr` < 40'''
        
        # 步骤5: 执行查询
        data = pd.read_sql(sql=sql, con=mdb.engine())
        
        # 步骤6: 去重
        data = data.drop_duplicates(subset="code", keep="last")
        
        # 检查结果
        if len(data.index) == 0:
            logging.info(f"没有卖出信号：{date}")
            return

        # 步骤7: 准备保存到卖出信号表
        table_name = tbs.TABLE_CN_STOCK_INDICATORS_SELL['name']  # 'cn_stock_indicators_sell'
        
        # 删除旧数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_INDICATORS_SELL['columns'])

        # 步骤8: 添加回测数据列
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        
        # 步骤9: 插入数据
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"卖出信号筛选完成：{date}，共{len(data)}只股票")
        
    except Exception as e:
        logging.error(f"indicators_data_daily_job.guess_sell处理异常：{e}")


# ==================== 主函数 ====================

"""
技术指标任务主函数
功能说明：
按顺序执行三个任务：
1. 计算所有股票的技术指标
2. 筛选买入信号（超买）
3. 筛选卖出信号（超卖）
执行顺序：
prepare() → guess_buy() → guess_sell()
必须按顺序，因为后面依赖前面的结果
运行方式：
# 计算今天的指标
python indicators_data_daily_job.py
# 计算指定日期
python indicators_data_daily_job.py 2024-01-01
# 批量计算
python indicators_data_daily_job.py 2024-01-01 2024-01-31
运行时机：
- 收盘后：15:00后
- 建议：17:30后（确保基础数据完整）
数据用途：
- 技术分析：查看指标值
- 选股：根据指标筛选
- 回测：验证指标策略
- Web展示：显示指标图表
"""
def main():
    # 任务1: 计算技术指标
    # run_with_args()：处理命令行参数，调用prepare()
    runt.run_with_args(prepare)
    
    # 任务2: 筛选买入信号
    runt.run_with_args(guess_buy)
    
    # 任务3: 筛选卖出信号
    runt.run_with_args(guess_sell)
    
    logging.info("技术指标任务执行完成")


# ==================== 程序入口 ====================
# main函数入口
if __name__ == '__main__':
    """
    直接运行此脚本时的入口
    
    前置条件：
        1. 已有基础数据（basic_data_daily_job）
        2. 已有历史K线（stock_hist_data）
        
    产生数据：
        1. cn_stock_indicators：所有股票的32种指标
        2. cn_stock_indicators_buy：买入信号股票
        3. cn_stock_indicators_sell：卖出信号股票
        
    后续任务：
        - backtest_data_daily_job：回测验证
        - Web展示：显示指标和信号
    """
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()


"""
===========================================
技术指标任务模块使用总结（给Python新手）
===========================================

1. 模块定位
   - 第三层：技术指标层
   - 依赖：基础数据、历史K线
   - 产出：32种技术指标、买卖信号

2. 核心概念
   技术指标：
   - 数学公式计算的分析工具
   - 基于价格和成交量
   - 帮助判断买卖时机
   
   超买超卖：
   - 超买：价格上涨过度，可能回调
   - 超卖：价格下跌过度，可能反弹

3. 32种指标
   趋势类：MACD、DMI、DMA
   摆动类：KDJ、RSI、CCI、WR
   通道类：BOLL、ENE
   能量类：CR、VR、OBV
   波动类：ATR、STOCHRSI
   ... 等

4. 买入卖出信号
   买入信号（超买）：
   - 多个指标同时超买
   - 适合高位止盈
   
   卖出信号（超卖）：
   - 多个指标同时超卖
   - 适合低位建仓

5. 并行计算
   - 4000只股票
   - 32种指标
   - 40个线程同时计算
   - 约5-10分钟完成

6. 数据流程
   基础数据 → 历史K线 → 
   计算指标 → 筛选信号 → 
   回测验证 → Web展示

7. SQL筛选
   SELECT date, code, name 
   FROM cn_stock_indicators 
   WHERE date = '2024-01-01' 
   AND kdjk >= 80 
   AND kdjd >= 70 
   ...
   
   解释：
   - 所有条件用AND连接
   - 必须同时满足
   - 提高准确率

8. 回测数据列
   - rate_1, rate_3, rate_5, ...
   - 初始为NULL
   - 待回测任务填充
   - 验证策略有效性

9. 使用场景
   - 技术分析：查看指标图表
   - 量化选股：根据指标筛选
   - 策略回测：验证指标策略
   - 实盘交易：发现交易机会

10. Python知识点
    - 多线程：concurrent.futures
    - DataFrame操作：pd.merge, pd.concat
    - SQL查询：pd.read_sql
    - 去重：drop_duplicates
    - 数据库操作：增删改查

11. 常见问题
    Q: 指标计算很慢？
    A: 正常，需要处理大量数据
    
    Q: 没有买卖信号？
    A: 可能当天没有符合条件的股票
    
    Q: 指标值为0？
    A: 可能数据不足或计算异常
    
    Q: 如何调整阈值？
    A: 修改guess_buy/guess_sell中的条件

12. 优化建议
    - 增加线程数：根据CPU核心数
    - 缓存历史数据：避免重复加载
    - 只计算必要指标：提高速度
    - 定期清理旧数据：节省空间
"""
