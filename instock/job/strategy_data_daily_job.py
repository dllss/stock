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
    """
    执行单个选股策略 - 核心数据处理函数
    
    参数说明：
    ---------
    date (datetime.date): 执行日期
        - 用于获取历史数据范围
        - 用于数据库时间戳
        
    strategy (dict): 策略定义字典，包含：
        - 'name'：策略表名，如'cn_stock_strategy_enter'
        - 'func'：策略检查函数，如check_enter
        - 'cn'：策略中文名，如'放量上涨'
    
    执行流程：
    --------
    1. 获取所有股票的历史K线数据
    2. 调用run_check()并行执行策略
    3. 筛选出符合策略的股票
    4. 构建DataFrame
    5. 添加回测计算列
    6. 删除旧数据，插入新数据
    7. 记录执行日志
    
    产生的数据：
    ----------
    策略表中包含：
    - date：执行日期
    - code：股票代码
    - name：股票名称
    - rate_1, rate_3, rate_5...rate_100：N日收益率（回测数据）
    
    工作原理（以海龟交易为例）：
    --------------------------
    输入：
    - 4000只股票的历史K线
    - 海龟交易策略函数
    - 2024-01-01
    
    处理：
    1. 并行检查4000只股票
    2. 每只股票：
       - 计算20日高点和10日低点
       - 判断是否突破
       - 返回True或False
    3. 收集返回True的股票（假设200只）
    
    输出：
    - 保存到cn_stock_strategy_turtle
    - 200只符合海龟交易条件的股票
    
    性能指标：
    ---------
    处理时间：
    - 历史数据获取：2-3秒
    - 策略检查（40线程）：5-10秒
    - 数据库操作：2-3秒
    - 总计：约10-15秒/策略
    - 10个策略并行：约15-20秒（总）
    
    内存使用：
    - 历史数据：~1GB（4000只股票）
    - 结果数据：~10MB（200-500只符合的）
    """
    try:
        # ==================== 步骤1: 获取历史数据 ====================
        # stock_hist_data()：单例模式，只加载一次
        # 返回所有股票的历史K线数据字典
        # 键：(date, code, name)
        # 值：DataFrame包含OHLCV数据
        stocks_data = stock_hist_data(date=date).get_data()
        if stocks_data is None:
            # 没有历史数据，无法执行策略
            logging.warning(f"没有历史数据，无法执行策略：{strategy['cn']} - {date}")
            return
        
        # ==================== 步骤2: 获取策略信息 ====================
        table_name = strategy['name']          # 策略表名，如'cn_stock_strategy_turtle'
        strategy_func = strategy['func']       # 策略检查函数
        strategy_cn = strategy['cn']           # 策略中文名
        
        # ==================== 步骤3: 并行执行策略 ====================
        # run_check()：并行检查所有股票
        # 返回值：[(date, code, name), ...] 或 None
        results = run_check(strategy_func, table_name, stocks_data, date)
        
        if results is None:
            # 没有符合条件的股票
            logging.info(f"策略{strategy_cn}未筛选到符合条件的股票：{date}")
            return

        # ==================== 步骤4: 删除旧数据 ====================
        # 同一日期的旧数据需要删除（避免重复和误导）
        if mdb.checkTableIsExist(table_name):
            # 表存在，删除该日期的旧数据
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            # 表已存在，无需重新创建
            cols_type = None
        else:
            # 表不存在（第一次运行），获取字段类型
            # 所有策略表结构相同，都参考TABLE_CN_STOCK_STRATEGIES[0]
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_STRATEGIES[0]['columns'])

        # ==================== 步骤5: 构建DataFrame ====================
        # results：[(date, code, name), ...]
        # 转换为DataFrame便于处理
        data = pd.DataFrame(results)
        
        # 设置列名
        columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])  # ('date', 'code', 'name')
        data.columns = columns
        
        # ==================== 步骤6: 添加回测数据列 ====================
        # 策略选出的股票需要回测
        # 添加空的回测收益率列：rate_1, rate_3, rate_5, ..., rate_100
        # 这些列后续由backtest_data_daily_job填充
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        
        # ==================== 步骤7: 日期处理 ====================
        # 确保所有行的date列都是正确的日期
        # 单例模式可能导致date不统一
        date_str = date.strftime("%Y-%m-%d")
        if len(data) > 0 and data.iloc[0]['date'] != date_str:
            # 批量更新date列（确保一致性）
            data['date'] = date_str
        
        # ==================== 步骤8: 插入数据库 ====================
        # insert_db_from_df()：从DataFrame插入数据库
        # 参数说明：
        #   - data：要插入的数据
        #   - table_name：目标表名
        #   - cols_type：列类型（首次创建需要）
        #   - False：是否删除源数据（这里不删除）
        #   - "`date`,`code`"：联合主键（防止重复）
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        # ==================== 步骤9: 记录执行日志 ====================
        num_selected = len(data)
        logging.info(f"策略{strategy_cn}执行完成：{date}，选出{num_selected}只股票")

    except Exception as e:
        # 异常处理：记录异常但不中断
        strategy_name = strategy.get('cn', '未知策略')
        logging.error(f"strategy_data_daily_job.prepare异常：{strategy_name}策略 - {e}")


# ==================== 并行执行策略筛选 ====================

def run_check(strategy_fun, table_name, stocks, date, workers=40):
    """
    并行执行策略函数，筛选所有符合条件的股票
    
    参数说明：
    ---------
    strategy_fun (function): 策略检查函数
        - 如：check_enter, check_volume等（来自core/strategy/下的各个模块）
        - 函数签名：func(code_name, data, date, **kwargs) -> bool
        - 接收股票键、历史数据、日期等参数
        - 返回True表示符合策略，False表示不符合
        
    table_name (str): 策略表名称
        - 如：'cn_stock_strategy_turtle'（海龟交易）
        - 用于日志记录和异常追踪
        
    stocks (dict): 所有股票的历史K线数据
        - 键格式：(date, code, name)
        - 值：DataFrame（包含Open、High、Low、Close、Volume等）
        - 数量：约4000只股票
        
    date (datetime.date): 执行日期
        - 用于数据库查询和日志
        
    workers (int): 线程池大小，默认40
        - 并发执行的线程数
        - 4000只股票 ÷ 40线程 = 每线程100只
        - 可根据CPU核数调整
    
    返回值：
    -------
    list 或 None: 符合策略的股票列表
        - 成功：返回列表，格式 [(date, code, name), ...]
        - 失败/无结果：返回None
    
    核心功能：
    ---------
    1. 特殊策略检查：
       - "高而窄的旗形"需要龙虎榜机构数据
       - 其他策略只需历史K线
       
    2. 创建线程池：
       - concurrent.futures.ThreadPoolExecutor
       - max_workers=40个线程
       
    3. 并发提交任务：
       - 为每只股票提交一个检查任务
       - 任务返回True/False
       
    4. 收集结果：
       - 使用as_completed()等待完成
       - 只保存返回True的股票
       
    5. 异常处理：
       - 单只股票失败不影响整体
       - 记录失败股票到日志
    
    并行执行原理：
    -----------
    时间对比：
    - 串行：4000只股票 × 2ms/只 = 8秒
    - 但还要加上其他操作（磁盘IO、网络等）
    - 实际串行：约2-3分钟
    - 并行（40线程）：约3-5秒 (excluding IO)
    - 实际并行：约10-15秒（得益于IO不阻塞）
    - 总体提升：约10-20倍
    
    异常处理说明：
    -----------
    - 单只股票检查异常：记录异常，继续处理下一只
    - 线程池创建异常：返回None，记录异常
    - 策略函数异常：由策略函数内部处理
    
    特殊案例："高而窄的旗形"策略
    ---------------------------
    - 普通策略：只基于K线形态
    - 此策略：需要额外的龙虎榜数据
    - fetch_stock_top_entity_data()：获取近期有机构买入的股票
    - 提高了策略的准确性（机构认可的股票）
    """
    """
    为指定的策略函数添加详细注释和异常处理
    """
    # ==================== 步骤1: 特殊策略检查 ====================
    # 检查是否是"高而窄的旗形"策略
    is_check_high_tight = False
    if strategy_fun.__name__ == 'check_high_tight':
        # "高而窄的旗形"策略需要龙虎榜机构数据
        # 用于筛选近期有机构买入的股票
        # fetch_stock_top_entity_data()：爬取龙虎榜数据
        # 返回值：集合，包含近期有大额机构买入的股票代码
        stock_tops = fetch_stock_top_entity_data(date)
        if stock_tops is not None:
            # 成功获取机构数据
            is_check_high_tight = True
            logging.info(f"获取机构龙虎榜数据成功：{len(stock_tops)}只股票")
    
    # ==================== 步骤2: 准备结果列表 ====================
    data = []  # 存储符合条件的股票：[(date, code, name), ...]
    
    try:
        # ==================== 步骤3: 创建线程池 ====================
        # ThreadPoolExecutor：Tornado线程池
        # max_workers：最大并发线程数
        # with语句自动管理线程池生命周期
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            
            # ==================== 步骤4: 为所有股票提交检查任务 ====================
            if is_check_high_tight:
                # 高而窄的旗形策略：需要额外的istop参数
                # istop：布尔值，表示该股票是否近期有机构买入
                future_to_data = {
                    executor.submit(
                        strategy_fun,       # 策略检查函数
                        k,                  # 股票键：(date, code, name)
                        stocks[k],          # 该股票的历史K线DataFrame
                        date=date,          # 日期参数
                        istop=(k[1] in stock_tops)  # 是否有机构买入
                    ): k 
                    for k in stocks  # 遍历所有4000只股票
                }
            else:
                # 普通策略：只需基本参数
                # 返回值：dict，键为Future对象，值为股票键
                # 使用字典理解式创建任务映射
                future_to_data = {
                    executor.submit(
                        strategy_fun,       # 策略检查函数
                        k,                  # 股票键
                        stocks[k],          # 历史K线
                        date=date           # 日期
                    ): k 
                    for k in stocks  # 遍历所有股票
                }
            
            # ==================== 步骤5: 收集符合条件的股票 ====================
            # as_completed()：返回完成的Future对象迭代器
            # 任务完成（无论成功或失败）时立即返回，不必等待全部完成
            for future in concurrent.futures.as_completed(future_to_data):
                stock_key = future_to_data[future]  # 获取对应的股票键
                try:
                    # future.result()：获取该Future任务的返回值
                    # 阻塞直到该任务完成
                    result = future.result()
                    
                    if result:  # 返回True，说明符合策略
                        # 将股票键添加到结果列表
                        data.append(stock_key)
                        
                except Exception as e:
                    # 单只股票检查过程中出异常
                    # 记录异常但不中断整体任务
                    # 异常原因可能：
                    # - 数据缺失
                    # - 策略函数bug
                    # - 数据格式错误
                    code = stock_key[1]
                    logging.error(f"strategy_data_daily_job.run_check异常：{code}代码{e}策略{table_name}")
                    # 继续处理下一只股票
                    
    except Exception as e:
        # 整体执行异常（如线程池创建失败）
        logging.error(f"strategy_data_daily_job.run_check处理异常：{e}策略{table_name}")
    
    # ==================== 步骤6: 检查结果并返回 ====================
    if not data:
        # 没有符合条件的股票
        # 返回None给调用者
        return None
    else:
        # 返回符合条件的股票列表
        # 后续prepare()函数会保存这些股票到数据库
        return data


# ==================== 主函数 ====================

def main():
    """
    策略选股任务主函数 - 并行执行所有10种策略
    
    功能说明：
    ---------
    按顺序执行系统内置的10种选股策略
    使用线程池实现策略级别的并行执行
    
    执行的10种策略：
    ---------------
    1. 放量上涨（enter.py）- 量价类
    2. 海龟交易法则（turtle_trade.py）- 趋势类
    3. 放量跌停（climax_limitdown.py）- 反转类
    4. 低ATR成长（low_atr.py）- 稳健类
    5. 回踩年线（backtrace_ma250.py）- 趋势类
    6. 突破平台（breakthrough_platform.py）- 突破类
    7. 停机坪（parking_apron.py）- 形态类
    8. 无大幅回撤（low_backtrace_increase.py）- 稳健类
    9. 均线多头（keep_increasing.py）- 趋势类
    10. 高而窄的旗形（high_tight_flag.py）- 爆发类
    
    并行架构：
    ---------
    第一层（策略级）：
    - 使用ThreadPoolExecutor创建线程池
    - 每个策略一个线程
    - 10个策略可同时执行
    
    第二层（股票级）：
    - 每个策略内部使用40个线程
    - 4000只股票并行检查
    - 嵌套并行（2层）
    
    性能对比：
    
    方案1：完全串行
    ┌─────────────────────────────────┐
    │ 策略1 (15min)                    │
    │ 策略2 (15min)                    │
    │ ... 10个策略 ...                │
    │ 总时间：150分钟（2.5小时）      │
    └─────────────────────────────────┘
    
    方案2：策略级并行（推荐）
    ┌─────────────────────────────────┐
    │ 策略1 策略2 策略3... 策略10       │ (全部并行)
    │ 15分钟（最慢的策略决定）        │
    └─────────────────────────────────┘
    
    效率提升：150 ÷ 15 = 10倍！
    
    运行场景：
    --------
    # 执行最近一天的策略（自动识别）
    python strategy_data_daily_job.py
    
    # 执行指定日期
    python strategy_data_daily_job.py 2024-01-01
    
    # 批量执行多天（需要run_template支持）
    python strategy_data_daily_job.py 2024-01-01 2024-01-31
    
    产生的输出表：
    -----------
    数据库自动创建10个表：
    - cn_stock_strategy_enter: 放量上涨信号
    - cn_stock_strategy_turtle: 海龟交易信号
    - cn_stock_strategy_climax: 放量跌停信号
    - cn_stock_strategy_low_atr: 低ATR成长
    - cn_stock_strategy_backtrace_ma250: 回踩年线
    - cn_stock_strategy_breakthrough: 突破平台
    - cn_stock_strategy_parking: 停机坪形态
    - cn_stock_strategy_low_backtrace: 无大幅回撤
    - cn_stock_strategy_keep_increasing: 均线多头
    - cn_stock_strategy_high_tight: 高而窄的旗形
    
    每个表包含：
    - date：选出日期
    - code：股票代码
    - name：股票名称
    - rate_1, rate_3, ...rate_100：N日收益率
    
    执行前置条件：
    -----------
    1. 基础数据已准备（basic_data_daily_job）
       - 股票代码列表
       - 股票名称等信息
    
    2. 历史K线已缓存（stock_hist_data）
       - 所有股票的历史数据
       - 通常需要3年历史
    
    3. 技术指标已计算（indicators_data_daily_job）
       - MACD、KDJ等32种指标
       - 某些策略依赖指标
    
    4. K线形态已识别（klinepattern_data_daily_job）
       - 61种形态数据
       - 形态类策略需要
    
    后续处理：
    --------
    1. 回测验证（backtest_data_daily_job）
       - 计算N日收益率
       - 评估策略效果
    
    2. Web展示（web_service）
       - 查看每个策略的选股结果
       - 对比不同策略
       - 分析股票特征
    
    3. 人工审查
       - 验证选股质量
       - 调整策略参数
       - 开发新策略
    
    错误处理：
    --------
    - 单个策略失败不影响其他
    - 所有异常都被记录
    - 系统继续运行
    - 可检查日志了解失败原因
    """
    # 创建策略级线程池
    # 任务：为每个策略提交一个执行任务
    with concurrent.futures.ThreadPoolExecutor() as executor:
        
        # ==================== 遍历所有策略 ====================
        # TABLE_CN_STOCK_STRATEGIES：定义了10个策略
        # 每个元素：{'name': 表名, 'func': 策略函数, 'cn': 中文名}
        for strategy in tbs.TABLE_CN_STOCK_STRATEGIES:
            
            # ==================== 为每个策略提交任务 ====================
            # runt.run_with_args()：处理命令行参数和日期
            # prepare：要执行的函数
            # strategy：要传递的参数（策略定义）
            # 
            # 执行流程：
            # 1. 获取要处理的日期（从命令行或自动识别）
            # 2. 调用prepare(date, strategy)
            # 3. 返回结果
            executor.submit(runt.run_with_args, prepare, strategy)
    
    # ==================== 等待所有策略完成 ====================
    # with语句退出时自动等待线程池所有任务完成
    # 阻塞直到所有10个策略都执行完毕
    
    logging.info("所有10个选股策略执行完成！")
    logging.info("产生的表：")
    for strategy in tbs.TABLE_CN_STOCK_STRATEGIES:
        logging.info(f"  - {strategy['name']}: {strategy['cn']}")
    logging.info("后续步骤：运行backtest_data_daily_job进行回测验证")


# ==================== 程序入口 ====================

# main函数入口
if __name__ == '__main__':
    """
    脚本直接运行时的入口点
    
    执行步骤：
    --------
    1. 设置日志配置
    2. 调用main()函数
    3. 执行10个策略
    4. 产生10个输出表
    
    使用方式：
    --------
    # 直接运行（处理最近一个交易日）
    python strategy_data_daily_job.py
    
    # 处理指定日期
    python strategy_data_daily_job.py 2024-01-01
    
    # 输出示例：
    # INFO: 策略放量上涨执行完成：2024-01-01，选出523只股票
    # INFO: 策略海龟交易执行完成：2024-01-01，选出187只股票
    # ... 更多策略 ...
    # INFO: 所有10个选股策略执行完成！
    
    运行环境：
    --------
    - Python 3.7+
    - 已安装所有依赖库
    - 数据库连接正常
    - 历史数据已准备
    """
    # 设置job任务的日志配置
    # 日志会输出到：
    # - 控制台（INFO及以上）
    # - 日志文件（DEBUG及以上）
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    
    # 调用主函数
    main()


# ==================== 知识点总结和常见问题 ====================
"""
核心知识点（量化策略选股）
==========================

1. 量化投资基础
   - 策略：基于数据和规则的投资方法
   - 选股：根据条件筛选股票
   - 回测：用历史数据验证策略
   - 实盘：在真实市场上执行

2. 10种策略的分类
   
   趋势类（抓住主要趋势）：
   - 海龟交易：突破高点买入
   - 均线多头：所有均线向上排列
   - 突破平台：突破整理平台区间
   
   形态类（基于K线组合）：
   - 停机坪：小幅回落形态
   - 高而窄的旗形：持续上涨的旗形
   
   量价类（成交量+价格）：
   - 放量上涨：价格创新高，量能突增
   - 放量跌停：大跌伴随大量成交
   
   回撤类（控制风险）：
   - 回踩年线：大跌后回踩年线
   - 无大幅回撤：一直保持上升
   
   成长类（稳定成长）：
   - 低ATR成长：低波动率中持续上升

3. 并发编程模式
   - 多线程：CPU密集和IO密集混合
   - ThreadPoolExecutor：线程池管理
   - 两级并行：策略级+股票级
   - future.result()：阻塞式获取结果

4. 策略函数的设计模式
   - 输入：股票键、历史数据、日期
   - 处理：计算指标、判断条件
   - 输出：True或False
   - 好处：统一接口，易于扩展

5. 数据流程和存储
   - 源数据：基础信息、历史K线
   - 计算：技术指标、K线形态
   - 筛选：10种策略分别筛选
   - 存储：10个策略表
   - 回测：计算N日收益率
   - 展示：Web展示结果

6. 数据库操作模式
   - 删除旧数据：避免重复
   - 构建DataFrame：便于批量操作
   - 批量插入：提高效率
   - 联合主键：防止重复

7. 异常处理最佳实践
   - 单点失败不影响全局
   - 详细的日志记录
   - 链式调用的异常处理
   - 记录失败股票便于调试

常见问题Q&A
===========

Q1: 为什么使用两级并行？
A: - 提高效率：10倍的性能提升
   - 充分利用CPU和IO
   - 策略不相关可并行执行
   - 股票检查天然适合并行

Q2: 为什么"高而窄的旗形"需要特殊处理？
A: - 需要龙虎榜机构数据
   - 机构认可的股票更可靠
   - 提高策略准确性
   - 避免中小散户陷阱

Q3: 为什么要删除旧数据？
A: - 同一日期可能多次运行
   - 避免数据重复
   - 避免重复计算回测数据
   - 保证数据一致性

Q4: DataFrame为什么要concat而不是直接赋值？
A: # 错误方式：直接赋值
   data['rate_1'] = None  # 创建新列
   
   # 正确方式：concat
   data = pd.concat([data, pd.DataFrame(columns=['rate_1'])])
   
   # 原因：
   - concat更安全
   - 保证列顺序正确
   - 处理类型转换
   - 避免SettingWithCopyWarning

Q5: 如何调试单个策略？
A: # 临时修改main()函数
   def main():
       strategy = tbs.TABLE_CN_STOCK_STRATEGIES[0]  # 只测试第一个
       prepare(datetime.date.today(), strategy)

Q6: 为什么要记录策略选出的股票数？
A: - 监控策略有效性
   - 检查是否选出过多/过少
   - 识别异常情况
   - 调整参数的参考

Q7: 如何添加新策略？
A: 1. 在core/strategy/下创建新文件
   2. 实现check_xxx(code_name, data, date, **kwargs)函数
   3. 在tablestructure.py的TABLE_CN_STOCK_STRATEGIES中注册
   4. 自动被main()函数执行

Q8: 为什么选出的股票数差异很大？
A: - 不同策略严格度不同
   - 市场行情影响选股数量
   - 某些策略条件较严格（如海龟交易）
   - 某些策略条件较宽松（如放量上涨）
   - 这很正常，反映了市场状态

优化建议
=======
1. 策略优化
   - 调整参数提高准确性
   - 结合多个策略
   - 添加风险控制条件
   - 开发因子组合

2. 性能优化
   - 增加ThreadPoolExecutor线程数
   - 使用AsyncIO异步编程
   - 缓存历史数据
   - 批量数据库操作

3. 功能优化
   - 添加策略权重
   - 支持策略组合投票
   - 添加风险评估
   - 实时监控策略表现

4. 可维护性
   - 添加单元测试
   - 添加集成测试
   - 完善文档
   - 提取通用模块

实战应用场景
===========
1. 日常监控（每日收盘后运行）
   - 找出当日的选股结果
   - 评估不同策略的表现
   - 选择最佳股票下单

2. 历史回测（周期批量运行）
   - 验证过去一个月的表现
   - 对比不同策略的效果
   - 优化策略参数

3. 策略开发（测试新思路）
   - 添加新策略
   - 测试新参数
   - 评估新组合

4. 风险管理（实时监控）
   - 监控持仓股票的策略信号
   - 当出现卖出信号时及时止盈/止损
   - 动态调整投资组合

系统调用链
==========
日常数据更新流程：
1. basic_data_daily_job
   → 更新基础数据
2. indicators_data_daily_job
   → 计算技术指标（32种）
3. klinepattern_data_daily_job
   → 识别K线形态（61种）
4. strategy_data_daily_job  [当前]
   → 执行选股策略（10种）
5. backtest_data_daily_job
   → 回测验证（计算收益率）
6. Web展示
   → 查看结果

总耗时：约60-90分钟（5层处理）
"""


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
