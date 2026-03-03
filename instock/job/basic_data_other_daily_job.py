#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
其他基础数据任务模块（第二层）
==============================
这个模块负责抓取除了股票/ETF行情之外的其他基础数据。

主要数据类型：
1. 龙虎榜数据
2. 资金流向数据（个股、行业、概念）
3. 分红配送数据
4. 早盘抢筹数据
5. 涨停原因数据
6. 基本面选股数据

为什么叫"其他"基础数据？
- basic_data_daily_job：核心行情数据（股票、ETF）
- basic_data_other_daily_job：其他重要数据（本文件）
- basic_data_after_close_daily_job：收盘后数据（大宗交易等）

这些数据的特点：
- 开盘即有数据（不需要等收盘）
- 对选股有重要参考价值
- 可以实时更新
- 数据量适中

运行时机：
- 开盘后运行：获取最新数据
- 建议：10:00后（数据更完整）
- 或收盘后：17:30

数据用途：
- 资金流向：寻找主力资金流向
- 龙虎榜：发现机构和游资动向
- 分红配送：股息收益机会
- 早盘抢筹：强势股发现
- 涨停原因：热点题材挖掘
- 基本面选股：价值投资

任务列表：
1. save_nph_stock_top_data - 龙虎榜
2. save_nph_stock_bonus - 分红配送  
3. save_nph_stock_fund_flow_data - 个股资金流向
4. save_nph_stock_sector_fund_flow_data - 行业/概念资金流向
5. stock_chip_race_open_data - 早盘抢筹
6. stock_imitup_reason_data - 涨停原因
7. stock_spot_buy - 基本面选股
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import concurrent.futures  # 多线程并发
import os.path  # 路径操作
import sys  # 系统操作
import pandas as pd  # 数据处理

# ==================== 路径配置 ====================
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

# ==================== 导入项目模块 ====================
import instock.lib.run_template as runt  # 任务运行模板
import instock.core.tablestructure as tbs  # 表结构定义
import instock.lib.database as mdb  # 数据库操作
import instock.core.stockfetch as stf  # 数据抓取模块

__author__ = 'myh '
__date__ = '2023/3/10 '

# ==================== 1. 保存龙虎榜数据 ====================

"""
保存龙虎榜数据
什么是龙虎榜？
- 异常波动股票的买卖席位公布
- 显示前5名买方和卖方
- 可以看出机构和游资的操作
数据内容：
- 上榜次数
- 累计买入/卖出金额
- 机构参与情况
用途：
- 发现主力动向
- 跟踪游资操作
- 寻找热点股票
"""
# 每日股票龙虎榜
def save_nph_stock_lhb_data(date, before=True):
    if before:
        return

    try:
        data = stf.fetch_stock_lhb_data(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_lHB['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_lHB['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_stock_lhb_data处理异常：{e}")
    stock_spot_buy(date)
def save_nph_stock_top_data(date, before=True):
    if before:
        return

    try:
        # 抓取龙虎榜数据
        data = stf.fetch_stock_top_data(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_TOP['name']
        # 删除老数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_TOP['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"保存龙虎榜数据成功：{len(data)}条")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_stock_top_data处理异常：{e}")
    
    # 执行基本面选股
    stock_spot_buy(date)


# ==================== 2. 保存个股资金流向数据 ====================

"""
保存个股资金流向数据
什么是资金流向？
- 主力资金（大单）的净流入/流出
- 分为超大单、大单、中单、小单
- 主力资金 = 超大单 + 大单
数据包含：
- 今日资金流向（index=0）
- 3日资金流向（index=1）
- 5日资金流向（index=2）
- 10日资金流向（index=3）
数据处理：
- 分别抓取4个时间周期的数据
- 按股票代码合并
- 形成一张完整的资金流向表
用途：
- 寻找主力流入的股票
- 避免主力流出的股票
- 判断资金趋势
"""
def save_nph_stock_fund_flow_data(date, before=True):
    if before:
        return

    try:
        # 定义时间周期：0=今日，1=3日，2=5日，3=10日
        times = tuple(range(4))
        
        # 抓取4个周期的资金流向数据
        results = run_check_stock_fund_flow(times)
        if results is None:
            return

        # 合并4个周期的数据
        for t in times:
            if t == 0:
                # 第一个周期（今日），作为基础数据
                data = results.get(t)
            else:
                # 其他周期，合并到基础数据
                r = results.get(t)
                if r is not None:
                    # 删除重复列（name和new_price在第一个周期已有）
                    r.drop(columns=['name', 'new_price'], inplace=True)
                    # 按代码合并
                    data = pd.merge(data, r, on=['code'], how='left')

        if data is None or len(data.index) == 0:
            return

        # 添加日期列
        data.insert(0, 'date', date.strftime("%Y-%m-%d"))

        table_name = tbs.TABLE_CN_STOCK_FUND_FLOW['name']
        # 删除老数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_FUND_FLOW['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"保存个股资金流向数据成功：{len(data)}条")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_nph_stock_fund_flow_data处理异常：{e}")


"""
抓取多个周期的资金流向数据
参数说明：
times (tuple): 时间周期索引
- 0：今日
- 1：3日
- 2：5日
- 3：10日
返回值：
dict: {周期索引: DataFrame}
注释掉的代码：
多线程版本（注释掉了）
改为顺序执行（更稳定）
"""
def run_check_stock_fund_flow(times):
    data = {}
    try:
        # 顺序抓取各周期数据
        for k in times:
            _data = stf.fetch_stocks_fund_flow(k)
            if _data is not None:
                data[k] = _data
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.run_check_stock_fund_flow处理异常：{e}")
    
    if not data:
        return None
    else:
        return data


# ==================== 3. 保存板块资金流向数据 ====================

"""
保存行业和概念板块资金流向
什么是板块资金流向？
- 行业板块：银行、房地产、医药等
- 概念板块：5G、新能源、人工智能等
- 显示板块整体的资金流向
用途：
- 发现热门行业
- 寻找热点概念
- 行业轮动分析
"""
def save_nph_stock_sector_fund_flow_data(date, before=True):
    if before:
        return

    # 分别处理行业（0）和概念（1）
    stock_sector_fund_flow_data(date, 0)  # 行业
    stock_sector_fund_flow_data(date, 1)  # 概念


"""
保存单个类型的板块资金流向
参数说明：
index_sector (int):
- 0：行业板块
- 1：概念板块
"""
def stock_sector_fund_flow_data(date, index_sector):
    try:
        # 抓取3个时间周期：今日、3日、5日
        times = tuple(range(3))
        results = run_check_stock_sector_fund_flow(index_sector, times)
        if results is None:
            return

        # 合并3个周期的数据
        for t in times:
            if t == 0:
                data = results.get(t)
            else:
                r = results.get(t)
                if r is not None:
                    # 按板块名称合并
                    data = pd.merge(data, r, on=['name'], how='left')

        if data is None or len(data.index) == 0:
            return

        # 添加日期列
        data.insert(0, 'date', date.strftime("%Y-%m-%d"))

        # 根据类型选择表
        if index_sector == 0:
            tbs_table = tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY  # 行业资金流向表
        else:
            tbs_table = tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT  # 概念资金流向表
        
        table_name = tbs_table['name']
        # 删除老数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs_table['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`name`")
        
        sector_type = "行业" if index_sector == 0 else "概念"
        logging.info(f"保存{sector_type}资金流向数据成功：{len(data)}条")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_sector_fund_flow_data处理异常：{e}")


"""
并行抓取板块资金流向数据
使用多线程同时抓取3个周期的数据
"""
def run_check_stock_sector_fund_flow(index_sector, times):
    data = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(times)) as executor:
            future_to_data = {
                executor.submit(stf.fetch_stocks_sector_fund_flow, index_sector, k): k 
                for k in times
            }
            for future in concurrent.futures.as_completed(future_to_data):
                _time = future_to_data[future]
                try:
                    _data_ = future.result()
                    if _data_ is not None:
                        data[_time] = _data_
                except Exception as e:
                    logging.error(f"basic_data_other_daily_job.run_check_stock_sector_fund_flow处理异常：代码{e}")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.run_check_stock_sector_fund_flow处理异常：{e}")
    
    if not data:
        return None
    else:
        return data


# ==================== 4. 保存分红配送数据 ====================

"""
保存分红配送数据
什么是分红配送？
- 分红：现金分红
- 送股：股票分红
- 转增：资本公积转增股本
数据用途：
- 寻找高分红股票
- 计算股息率
- 除权除息日提醒
"""
def save_nph_stock_bonus(date, before=True):
    if before:
        return

    try:
        # 抓取分红配送数据
        data = stf.fetch_stocks_bonus(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_BONUS['name']
        # 删除老数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_BONUS['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"保存分红配送数据成功：{len(data)}条")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_nph_stock_bonus处理异常：{e}")


# ==================== 5. 基本面选股 ====================

"""
基本面选股（价值投资策略）
选股条件：
1. 市盈率TTM（pe9）> 0 且 <= 20
- 估值合理，不太贵
2. 市净率（pbnewmrq）<= 10
- 不高估
3. 净资产收益率（roe_weight）>= 15
- 盈利能力强
策略类型：
- 价值投资
- 选择好公司、好价格
用途：
- 长期投资选股
- 寻找低估值成长股
"""
def stock_spot_buy(date):
    try:
        _table_name = tbs.TABLE_CN_STOCK_SPOT['name']
        if not mdb.checkTableIsExist(_table_name):
            return

        # SQL查询：基本面条件筛选
        sql = f'''SELECT * FROM `{_table_name}` WHERE `date` = '{date}' and 
                `pe9` > 0 and `pe9` <= 20 and `pbnewmrq` <= 10 and `roe_weight` >= 15'''
        
        # 读取符合条件的股票
        data = pd.read_sql(sql=sql, con=mdb.engine())
        # 去重（按代码）
        data = data.drop_duplicates(subset="code", keep="last")
        
        if len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_SPOT_BUY['name']
        # 删除老数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SPOT_BUY['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"基本面选股成功：{len(data)}只股票")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_spot_buy处理异常：{e}")


# ==================== 6. 保存早盘抢筹数据 ====================

"""
保存早盘抢筹数据
什么是早盘抢筹？
- 开盘后30分钟快速拉升的股票
- 说明有资金急于买入
- 可能有重大利好或主力拉升计划
"""
def stock_chip_race_open_data(date):
    try:
        data = stf.fetch_stock_chip_race_open(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['name']
        # 删除老数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"保存早盘抢筹数据成功：{len(data)}条")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_chip_race_open_data：{e}")


# ==================== 7. 保存涨停原因数据 ====================

"""
保存涨停原因数据
什么是涨停原因？
- 涨停股票的原因分析
- 如：业绩大增、重组、新产品等
用途：
- 了解市场热点
- 挖掘投资机会
- 跟踪题材炒作
"""
def stock_imitup_reason_data(date):
    try:
        data = stf.fetch_stock_limitup_reason(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_LIMITUP_REASON['name']
        # 删除老数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_LIMITUP_REASON['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"保存涨停原因数据成功：{len(data)}条")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_imitup_reason_data：{e}")


# ==================== 主函数 ====================

"""
其他基础数据任务主函数
执行顺序：
1. 龙虎榜数据
2. 分红配送数据
3. 个股资金流向
4. 板块资金流向
5. 早盘抢筹
6. 涨停原因
每个任务独立执行
一个任务失败不影响其他任务
"""
def main():
    # 1. 龙虎榜（包含基本面选股）
    runt.run_with_args(save_nph_stock_lhb_data)
    
    # 2. 分红配送
    runt.run_with_args(save_nph_stock_bonus)
    
    # 3. 个股资金流向（今日、3日、5日、10日）
    runt.run_with_args(save_nph_stock_fund_flow_data)
    
    # 4. 板块资金流向（行业、概念）
    runt.run_with_args(save_nph_stock_sector_fund_flow_data)
    
    # 5. 早盘抢筹
    runt.run_with_args(stock_chip_race_open_data)
    
    # 6. 涨停原因
    runt.run_with_args(stock_imitup_reason_data)
    
    logging.info("其他基础数据任务执行完成")


# ==================== 程序入口 ====================
if __name__ == '__main__':
    """
    直接运行此脚本
    
    运行方式：
        python basic_data_other_daily_job.py
    """
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()


"""
===========================================
其他基础数据任务使用总结（给Python新手）
===========================================

1. 模块定位
   - 第二层：数据抓取层
   - 补充数据：除了行情之外的重要数据
   - 7个子任务

2. 数据类型
   龙虎榜：
   - 异常波动股票
   - 机构游资动向
   
   资金流向：
   - 个股：单只股票资金
   - 板块：行业概念资金
   
   分红配送：
   - 现金分红
   - 股票分红
   
   早盘抢筹：
   - 开盘拉升股票
   - 资金抢筹信号
   
   涨停原因：
   - 涨停股票
   - 原因分析

3. 数据合并
   资金流向合并：
   - 今日 + 3日 + 5日 + 10日
   - 按code合并
   - pd.merge()
   
   板块流向合并：
   - 行业或概念
   - 按name合并

4. 执行顺序
   - 顺序执行7个任务
   - 每个独立
   - 失败不影响其他

5. 使用场景
   - 主力资金追踪
   - 热点板块发现
   - 价值投资选股
   - 短线机会发现
"""
