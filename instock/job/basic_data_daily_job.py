#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
基础数据每日任务模块（第二层核心）
===================================
这是数据抓取的核心模块，负责每天抓取股票和ETF的实时数据。

什么是基础数据？
- 股票的当日数据：价格、成交量、市值、财务指标等
- ETF的当日数据：基金的当日行情
- 这些是所有后续分析的基础

数据来源：
- 东方财富网
- 新浪财经
- 免费、实时、全面

数据内容：
股票数据（200+个字段）：
- 基本信息：代码、名称、价格、涨跌幅
- 成交信息：成交量、成交额、换手率
- 估值指标：市盈率、市净率
- 财务指标：每股收益、净资产收益率
- 资本结构：总股本、流通股本、市值

ETF数据：
- 基金代码、名称
- 最新价、涨跌幅
- 成交量、成交额
- 市值信息

运行时机：
- 开盘期间：可以实时运行（数据会更新）
- 收盘后：运行一次获取当天最终数据
- 建议：17:30运行（确保数据完整）

数据处理：
1. 从网络抓取最新数据
2. 转换为标准格式
3. 删除旧数据（避免重复）
4. 插入新数据到数据库

为什么要删除旧数据？
- 同一天多次运行会插入重复数据
- 删除旧的，插入新的，保证数据唯一性
- 主键是(date, code)，保证每天每只股票只有一条记录
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
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
import instock.core.stockfetch as stf  # 股票数据抓取
from instock.core.singleton_stock import stock_data  # 股票数据单例

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 保存股票数据 ====================

"""
保存股票实时行情数据到数据库
参数说明：
date (datetime.date): 数据日期
before (bool): 时间标志
- True: 开盘前（不执行）
- False: 开盘后或收盘后（执行）
功能说明：
1. 从单例获取股票数据（已经抓取好的）
2. 删除数据库中该日期的旧数据
3. 插入新数据到数据库
为什么有before参数？
- 开盘前：数据还未更新，不需要保存
- 开盘后：数据实时更新，可以保存
- 这个机制支持灵活的运行时间
数据流程：
网络抓取 → 单例缓存 → 数据库保存
nph是什么意思？
nph = not open or not close
表示：未开盘或未收盘时也可以运行
执行流程：
1. 检查是否开盘
2. 从单例获取数据
3. 删除旧数据
4. 插入新数据
5. 创建主键和索引
"""
def save_nph_stock_spot_data(date, before=True):
    # 步骤1: 检查是否开盘
    if before:
        # 开盘前，不执行
        return
    
    try:
        # 步骤2: 从单例获取股票数据
        # stock_data()：调用单例，如果没有则抓取，如果有则直接返回
        # get_data()：获取DataFrame数据
        data = stock_data(date).get_data()
        
        # 检查数据是否有效
        if data is None or len(data.index) == 0:
            # 没有数据，可能网络问题或非交易日
            return

        # 步骤3: 获取表名
        table_name = tbs.TABLE_CN_STOCK_SPOT['name']  # 'cn_stock_spot'
        
        # 步骤4: 删除该日期的旧数据（如果表存在）
        if mdb.checkTableIsExist(table_name):
            # 表已存在，删除旧数据
            # 构建DELETE SQL语句
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            # 执行删除
            mdb.executeSql(del_sql)
            # 表已存在，不需要指定字段类型（已有表结构）
            cols_type = None
        else:
            # 表不存在，第一次运行
            # 需要指定字段类型，创建表时使用
            # get_field_types()：从表结构定义中提取字段类型
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SPOT['columns'])

        # 步骤5: 插入数据到数据库
        # insert_db_from_df()：从DataFrame插入数据
        # 参数：
        #   data: DataFrame数据
        #   table_name: 表名
        #   cols_type: 字段类型（None或dict）
        #   False: 不写入索引
        #   "`date`,`code`": 主键定义（日期+代码）
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        # 日志记录（可选）
        logging.info(f"保存股票数据成功：{date}，共{len(data)}条")

    except Exception as e:
        # 捕获并记录异常
        # 不会中断程序，保证后续任务继续执行
        logging.error(f"basic_data_daily_job.save_stock_spot_data处理异常：{e}")


# ==================== 保存ETF数据 ====================

"""
保存ETF实时行情数据到数据库
参数说明：
date (datetime.date): 数据日期
before (bool): 时间标志，同上
功能说明：
与save_nph_stock_spot_data类似
区别：
1. 抓取的是ETF数据而不是股票数据
2. 使用不同的表名和表结构
3. 数据来源也是东方财富网
ETF数据特点：
- 字段比股票少（没有财务指标）
- 以基金代码标识（51、159等开头）
- 交易机制与股票相同
执行流程：
1. 检查是否开盘
2. 抓取ETF数据
3. 删除旧数据
4. 插入新数据
"""
def save_nph_etf_spot_data(date, before=True):
    # 步骤1: 检查是否开盘
    if before:
        return
    
    try:
        # 步骤2: 抓取ETF数据
        # stf.fetch_etfs()：从网络抓取ETF数据
        # 返回DataFrame格式
        data = stf.fetch_etfs(date)
        
        # 检查数据是否有效
        if data is None or len(data.index) == 0:
            return

        # 步骤3: 获取ETF表名
        table_name = tbs.TABLE_CN_ETF_SPOT['name']  # 'cn_etf_spot'
        
        # 步骤4: 删除旧数据
        if mdb.checkTableIsExist(table_name):
            # 表存在，删除旧数据
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            # 表不存在，获取字段类型
            cols_type = tbs.get_field_types(tbs.TABLE_CN_ETF_SPOT['columns'])

        # 步骤5: 插入ETF数据
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"保存ETF数据成功：{date}，共{len(data)}条")
        
    except Exception as e:
        logging.error(f"basic_data_daily_job.save_nph_etf_spot_data处理异常：{e}")


# ==================== 主函数 ====================

"""
基础数据任务主函数
功能说明：
按顺序执行股票和ETF数据的保存任务
run_with_args是什么？
- 任务运行模板函数
- 处理命令行参数（日期）
- 自动判断交易日
- 支持批量执行
执行顺序：
1. 先保存股票数据（主要）
2. 再保存ETF数据（次要）
运行方式：
# 当前交易日
python basic_data_daily_job.py
# 指定日期
python basic_data_daily_job.py 2024-01-01
# 多个日期
python basic_data_daily_job.py 2024-01-01,2024-01-02
# 日期区间
python basic_data_daily_job.py 2024-01-01 2024-01-10
执行时机：
1. 开盘期间：获取实时数据
2. 收盘后：获取最终数据
3. 建议：17:30后运行
数据用途：
- 策略选股：根据当日数据筛选
- 技术分析：计算指标需要最新价格
- 回测：需要历史每日数据
- Web展示：显示最新行情
"""
def main():
    # 执行股票数据保存任务
    # run_with_args()会处理：
    # 1. 解析命令行参数
    # 2. 判断是否交易日
    # 3. 循环处理多个日期
    # 4. 调用传入的函数（save_nph_stock_spot_data）
    runt.run_with_args(save_nph_stock_spot_data)
    
    # 执行ETF数据保存任务
    runt.run_with_args(save_nph_etf_spot_data)
    
    # 任务完成
    logging.info("基础数据任务执行完成")


# ==================== 程序入口 ====================
# main函数入口
if __name__ == '__main__':
    """
    直接运行此脚本时的入口
    
    这是数据处理流程的第一步
    必须先有基础数据，才能进行后续分析
    
    运行示例：
        # 获取今天的数据
        python basic_data_daily_job.py
        
        # 获取指定日期的数据
        python basic_data_daily_job.py 2024-01-01
        
        # 回测历史数据
        python basic_data_daily_job.py 2024-01-01 2024-01-31
    """
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()


"""
===========================================
基础数据任务模块使用总结（给Python新手）
===========================================

1. 模块定位
   - 第二层：数据抓取层
   - 核心模块：所有数据的源头
   - 依赖：网络抓取模块
   - 被依赖：所有后续分析模块

2. 核心功能
   - 抓取股票当日数据（4000+只）
   - 抓取ETF当日数据（500+只）
   - 保存到数据库

3. 数据内容
   股票数据：
   - 价格：开盘价、最高价、最低价、收盘价
   - 成交：成交量、成交额、换手率
   - 估值：市盈率、市净率
   - 财务：每股收益、净资产收益率
   - 市值：总市值、流通市值
   
   ETF数据：
   - 价格、成交量、成交额
   - 涨跌幅、市值

4. 运行时机
   开盘期间：
   - 9:30-15:00
   - 数据实时更新
   - 可多次运行
   
   收盘后：
   - 15:00后
   - 数据最终确定
   - 建议17:30运行

5. 数据处理流程
   网络请求 → 数据解析 → 格式转换 → 删除旧数据 → 插入新数据

6. 删除旧数据的原因
   - 避免重复：同一天多次运行
   - 保证唯一：主键(date, code)
   - 获取最新：实时数据会变化

7. before参数
   - True: 开盘前，不执行
   - False: 开盘后，执行
   - 灵活运行：支持不同时间

8. 单例模式的作用
   - stock_data()：获取股票数据单例
   - 第一次：从网络抓取
   - 后续：直接返回缓存
   - 避免重复抓取

9. 错误处理
   - try-except：捕获所有异常
   - logging.error：记录错误日志
   - 不中断程序：保证后续任务执行

10. 使用场景
    - 每日更新：定时任务执行
    - 实时监控：开盘期间多次运行
    - 历史回测：批量获取历史数据
    - 数据修复：重新获取指定日期数据

11. 依赖关系
    需要：
    - 网络连接
    - MySQL数据库
    - tablestructure（表结构）
    - stockfetch（抓取模块）
    
    被需要：
    - indicators_data_daily_job（指标计算）
    - strategy_data_daily_job（策略选股）
    - klinepattern_data_daily_job（形态识别）
    - Web展示模块

12. Python知识点
    - 函数定义：def 函数名(参数)
    - 参数默认值：before=True
    - 条件判断：if before: return
    - 异常处理：try-except
    - 模块导入：import ... as ...
    - 字符串格式化：f-string
    - DataFrame操作：pandas
    - 数据库操作：SQL

13. 调试技巧
    - 查看日志：stock_execute_job.log
    - 检查数据：Navicat查看数据库
    - 打印信息：print(data.head())
    - 单步执行：IDE调试模式

14. 常见问题
    Q: 为什么没有数据？
    A: 检查网络、检查是否交易日
    
    Q: 数据重复怎么办？
    A: 删除旧数据再运行
    
    Q: 运行很慢怎么办？
    A: 正常现象，4000+只股票需要时间
    
    Q: 如何只获取部分股票？
    A: 修改stockfetch.py中的过滤条件
"""
