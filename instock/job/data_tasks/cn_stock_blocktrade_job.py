#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
大宗交易数据任务 - 独立执行脚本
==================================
功能：抓取并保存大宗交易数据
目标表：cn_stock_blocktrade
API请求数：约1次
执行时间：约5-10秒

运行方式：
    python instock/job/data_tasks/cn_stock_blocktrade_job.py
    
依赖关系：
    无（独立任务）
    
数据用途：
    - 发现大资金动向
    - 股东减持预警
    - 机构调仓分析
    - 折价/溢价分析

注意事项：
    - 大宗交易数据通常在交易日17:00后发布
    - 如果17:00前运行，可能返回空数据
    - 建议在收盘后2小时(17:00-19:00)运行
"""

import os
import sys
import logging
from datetime import datetime

# 添加项目路径
cpath_current = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(cpath_current)

# 导入必需的库
from instock.core import stockfetch as stf
from instock.core import tablestructure as tbs
from instock.lib import database as mdb
from instock.lib import run_template as runt
from instock.job.task_utils import check_and_skip_if_exists


# ==================== 保存大宗交易数据 ====================

def save_block_trade_data(date, before=False):
    """
    保存大宗交易数据到数据库
    
    参数：
        date: 日期对象 (datetime.date)
        before: 是否为盘前数据(大宗交易都是盘后数据,此参数保留以兼容接口)
    
    返回：
        None
    
    异常处理：
        - API请求失败：记录错误日志
        - 数据库插入失败：记录错误日志
        - 数据为空：记录警告日志
    """
    try:
        # 格式化日期字符串用于日志
        if isinstance(date, str):
            log_date = date
        else:
            log_date = date.strftime('%Y-%m-%d')
        
        table_name = tbs.TABLE_CN_STOCK_BLOCKTRADE['name']
        
        # 步骤1: 检查当天是否已有数据
        if check_and_skip_if_exists(table_name, log_date):
            return
        
        # 步骤2: 抓取大宗交易数据
        logging.info("=" * 20)
        logging.info(f"[{log_date}] 开始获取大宗交易数据...")
        logging.info("=" * 20)
        
        data = stf.fetch_stock_blocktrade_data(date)
        
        # 检查数据是否为空
        if data is None or len(data.index) == 0:
            logging.warning(f"⚠️  大宗交易数据暂无：{log_date}（可能17:00后才有）")
            return
        
        # 准备插入数据
        
        # 检查表是否存在
        if mdb.checkTableIsExist(table_name):
            cols_type = None  # 表已存在，不需要字段类型
        else:
            # 表不存在，需要创建
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_BLOCKTRADE['columns'])
            logging.info(f"📋 表 {table_name} 不存在，将自动创建")
        
        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (大宗交易)")
        logging.info(f"   目标日期: {log_date}")
        logging.info(f"   数据量: {len(data)}条记录")
        logging.info(f"   开始插入数据...")
        
        # 插入新数据
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        # 记录成功日志
        logging.info(f"✅ 保存大宗交易数据成功：{len(data)}条")
        logging.info(f"   表名: {table_name}")
        logging.info(f"   主键: date, code")
        logging.info(f"   字段数: {len(data.columns)}")
        logging.info("=" * 20)
        
    except Exception as e:
        logging.error(f"❌ 大宗交易任务处理异常：{e}", exc_info=True)


# ==================== 主函数 ====================

def main():
    """
    大宗交易数据任务主函数
    
    执行流程：
    1. 获取当前日期或指定日期
    2. 调用save_block_trade_data保存数据
    3. 记录任务完成日志
    
    运行方式：
        python cn_stock_blocktrade_job.py
    """
    logging.info("")
    logging.info("=" * 20)
    logging.info("开始执行大宗交易数据任务")
    logging.info("=" * 20)
    
    # 使用run_template执行任务（会自动传递日期参数）
    runt.run_with_args(save_block_trade_data, False)
    
    logging.info("")
    logging.info("=" * 20)
    logging.info("大宗交易数据任务执行完成")
    logging.info("=" * 20)


# ==================== 程序入口 ====================

if __name__ == '__main__':
    """
    直接运行此脚本
    
    运行方式：
        python instock/job/data_tasks/cn_stock_blocktrade_job.py
        
    最佳运行时间：
        - 17:00后（数据已发布）
        - 或第二天运行前一天的数据
    
    示例：
        # 使用当前日期
        python cn_stock_blocktrade_job.py
        
        # 指定日期（需要在代码中修改）
        # 修改 main() 函数中的日期参数
    """
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()


"""
===========================================
大宗交易数据任务使用总结
===========================================

1. 模块定位
   - 独立任务文件
   - 位于 instock/job/data_tasks/
   - 可单独执行，也可被其他任务调用

2. 数据特点
   - 发布时间：交易日17:00后
   - 数据类型：大宗成交记录
   - 重要字段：成交价、成交量、溢价率、买卖席位

3. 分析方法
   - 折价率高：可能看跌（股东减持）
   - 溢价买入：可能看涨（机构看好）
   - 频繁交易：需要重点关注
   - 大额成交：重要信号

4. 运行建议
   - 定时任务：17:30后自动运行
   - 手动运行：第二天补充前一天数据
   - 检查日志：确认数据是否成功保存

5. 与其他任务的关系
   - 独立任务，无依赖
   - 可在 execute_daily_job.py 中调用
   - 也可通过批处理文件单独运行
"""
