#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
K线形态识别任务模块（第四层核心）
==================================
这个模块负责识别所有股票的K线形态。

什么是K线形态识别？
- 分析K线组合图案
- 识别经典的买卖信号
- 61种经典形态
- 基于技术分析理论

形态的意义：
- 反映市场心理
- 买卖力量对比
- 趋势转折信号
- 持续或反转判断

61种形态包括：
看涨形态：
- 锤头、晨星、三个白兵、看涨吞没等

看跌形态：
- 上吊线、暮星、三只乌鸦、看跌吞没等

中性形态：
- 十字星、纺锤线等

数据流程：
历史K线 → TA-Lib识别 → 
筛选有形态的 → 保存数据库 → 
Web展示

为什么只保存有形态的？
- 大部分时间没有形态
- 节省数据库空间
- 方便查询和筛选

运行时机：
- 收盘后运行
- 建议17:30后
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
import instock.core.pattern.pattern_recognitions as kpr  # 形态识别模块

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 准备并识别形态 ====================

"""
准备并识别所有股票的K线形态
参数说明：
date (datetime.date): 识别日期
功能说明：
1. 获取所有股票的历史K线
2. 并行识别每只股票的61种形态
3. 筛选出有形态的股票
4. 保存到数据库
执行流程：
1. 从单例获取历史数据
2. 并行识别形态
3. 构建结果DataFrame
4. 删除旧数据
5. 插入新数据
数据量：
- 输入：4000只股票
- 识别：61种形态
- 输出：约100-300只（有形态的）
为什么输出少？
- 大部分股票大部分时间没有经典形态
- 只保存识别到形态的股票
- 节省空间，方便查询
"""
def prepare(date):
    try:
        # ==================== 步骤1: 获取历史数据 ====================
        stocks_data = stock_hist_data(date=date).get_data()
        if stocks_data is None:
            logging.warning(f"没有历史数据，无法识别形态：{date}")
            return
        
        # ==================== 步骤2: 并行识别形态 ====================
        # run_check()：使用多线程识别所有股票
        results = run_check(stocks_data, date=date)
        
        if results is None:
            # 没有识别到任何形态
            logging.info(f"未识别到K线形态：{date}")
            return

        # ==================== 步骤3: 获取表名 ====================
        table_name = tbs.TABLE_CN_STOCK_KLINE_PATTERN['name']  # 'cn_stock_kline_pattern'
        
        # ==================== 步骤4: 删除旧数据 ====================
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_KLINE_PATTERN['columns'])

        # ==================== 步骤5: 构建DataFrame ====================
        # results是字典：{(date, code, name): Series(形态数据)}
        
        # 构建键DataFrame（股票信息）
        dataKey = pd.DataFrame(results.keys())
        _columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])  # date, code, name
        dataKey.columns = _columns
        
        # 构建值DataFrame（形态识别结果）
        dataVal = pd.DataFrame(results.values())

        # 合并两个DataFrame
        # on=['code']：按代码列合并
        # how='left'：左连接
        data = pd.merge(dataKey, dataVal, on=['code'], how='left')
        
        # ==================== 步骤6: 日期处理 ====================
        # 确保日期正确
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            data['date'] = date_str
        
        # ==================== 步骤7: 插入数据库 ====================
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"K线形态识别完成：{date}，共{len(data)}只股票有形态")

    except Exception as e:
        logging.error(f"klinepattern_data_daily_job.prepare处理异常：{e}")


# ==================== 并行识别形态 ====================

"""
并行识别多只股票的K线形态
参数说明：
stocks (dict): 股票历史数据字典
- 键：(date, code, name)
- 值：DataFrame（历史K线）
date (datetime.date): 计算日期
workers (int): 线程池大小，默认40
返回值：
dict: 识别结果字典
- 键：(date, code, name)
- 值：Series（形态数据）
- 只包含识别到形态的股票
功能说明：
1. 获取形态定义
2. 创建线程池
3. 为每只股票提交识别任务
4. 收集有形态的股票
5. 返回结果字典
执行原理：
- 40个线程同时识别
- 每个线程处理一只股票
- 识别61种形态
- 只保存有形态的
为什么是40个线程？
- CPU密集型任务
- 线程数适中
- 不会过度竞争
- 可以根据CPU调整
"""
def run_check(stocks, date=None, workers=40):
    # ==================== 步骤1: 准备结果字典 ====================
    data = {}
    
    # ==================== 步骤2: 获取形态定义 ====================
    # STOCK_KLINE_PATTERN_DATA：61种形态的定义
    # columns：字典，键是形态名，值包含TA-Lib函数
    columns = tbs.STOCK_KLINE_PATTERN_DATA['columns']
    data_column = columns
    
    try:
        # ==================== 步骤3: 创建线程池 ====================
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            
            # ==================== 步骤4: 提交所有识别任务 ====================
            # 字典推导式：{Future: 股票键}
            future_to_data = {
                # executor.submit()：提交任务
                # kpr.get_pattern_recognition：形态识别函数
                # k：股票键
                # stocks[k]：历史K线
                # data_column：形态定义
                # date：日期
                executor.submit(kpr.get_pattern_recognition, k, stocks[k], data_column, date=date): k 
                for k in stocks  # 遍历所有股票
            }
            
            # ==================== 步骤5: 收集识别结果 ====================
            # 等待任务完成并收集结果
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]  # 获取对应的股票
                try:
                    _data_ = future.result()  # 获取识别结果
                    if _data_ is not None:
                        # 识别到形态，保存结果
                        data[stock] = _data_
                        
                except Exception as e:
                    # 单只股票识别失败，记录日志
                    logging.error(f"klinepattern_data_daily_job.run_check处理异常：{stock[1]}代码{e}")
                    
    except Exception as e:
        # 整体执行异常
        logging.error(f"klinepattern_data_daily_job.run_check处理异常：{e}")
    
    # ==================== 步骤6: 检查结果并返回 ====================
    if not data:
        # 没有识别到任何形态
        return None
    else:
        # 返回结果字典
        return data


# ==================== 主函数 ====================

"""
K线形态识别任务主函数
功能说明：
调用prepare()执行形态识别
运行方式：
# 识别今天的形态
python klinepattern_data_daily_job.py
# 识别指定日期
python klinepattern_data_daily_job.py 2024-01-01
# 批量识别
python klinepattern_data_daily_job.py 2024-01-01 2024-01-31
产生数据：
- cn_stock_kline_pattern表
- 包含识别到形态的股票
- 61个形态列
数据用途：
- 技术分析：查看形态
- 选股：筛选特定形态
- Web展示：形态图表
"""
def main():
    # 使用运行模板执行任务
    # run_with_args()：处理命令行参数，调用prepare()
    runt.run_with_args(prepare)
    
    logging.info("K线形态识别任务执行完成")


# ==================== 程序入口 ====================
# main函数入口
if __name__ == '__main__':
    """
    直接运行此脚本时的入口
    
    前置条件：
        1. 已有基础数据
        2. 已有历史K线
        
    产生数据：
        cn_stock_kline_pattern表
        
    后续使用：
        - Web查看形态
        - 筛选特定形态
        - 技术分析参考
    """
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()


"""
===========================================
K线形态任务模块使用总结（给Python新手）
===========================================

1. 模块定位
   - 第四层：K线形态层
   - 依赖：历史K线数据
   - 产出：形态识别结果

2. 核心功能
   - 识别61种K线形态
   - 并行处理提高速度
   - 只保存有形态的股票

3. 数据流程
   历史K线 → 形态识别 → 
   筛选有形态的 → 保存数据库 → 
   Web展示

4. 识别结果
   - +100：看涨形态
   - -100：看跌形态
   - 0：没有形态

5. 并行执行
   - 4000只股票
   - 40个线程
   - 每只股票识别61种形态
   - 约3-5分钟完成

6. 输出特点
   - 输入：4000只股票
   - 输出：100-300只（有形态的）
   - 节省：数据库空间
   - 方便：查询和展示

7. 使用场景
   - 技术分析：查看K线形态
   - 选股：筛选特定形态
   - 辅助决策：买卖参考

8. Python知识点
   - DataFrame操作：merge合并
   - 多线程：ThreadPoolExecutor
   - 字典操作：keys(), values()
   - 异常处理：try-except

9. 常见形态
   必须掌握：
   - 锤头：底部看涨
   - 上吊线：顶部看跌
   - 十字星：转折信号
   - 晨星：底部反转
   - 暮星：顶部反转

10. 注意事项
    - 形态是辅助工具
    - 需要趋势确认
    - 需要成交量配合
    - 不是绝对信号
"""
