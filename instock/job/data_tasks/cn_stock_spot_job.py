#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票基础数据任务 - cn_stock_spot
=================================

功能说明：
抓取A股每日基础行情数据并保存到数据库

数据来源：
- 东方财富网（实时行情API）

数据内容（200+字段）：
- 基本信息：股票代码、名称、交易所
- 价格信息：开盘价、最高价、最低价、收盘价、涨跌幅
- 成交信息：成交量、成交额、换手率、量比
- 估值指标：市盈率(TTM/静/动)、市净率、市销率
- 财务指标：每股收益、每股净资产、ROE
- 市值信息：总市值、流通市值、总股本、流通股本

执行时机：
- 开盘期间：可实时运行（数据会更新）
- 收盘后：建议17:30运行（获取最终数据）
- 非交易日：跳过

数据处理流程：
1. 从单例获取股票数据（已缓存或重新抓取）
2. 删除数据库中该日期的旧数据
3. 插入新数据到 cn_stock_spot 表
4. 主键：(date, code) 保证唯一性

运行方式：
# 当前交易日
python stock_spot_job.py

# 指定日期
python stock_spot_job.py 2026-05-08

# 多个日期
python stock_spot_job.py 2026-05-08,2026-05-09

# 日期区间
python stock_spot_job.py 2026-05-01 2026-05-31
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import date
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
from instock.core.singleton_stock import stock_data
from instock.job.task_utils import check_and_skip_if_exists

__author__ = 'myh'
__date__ = '2026/05/09'


def save_stock_spot_data(date_obj, before=True):
    """
    保存股票基础数据到数据库
    
    Args:
        date_obj: 日期对象 (datetime.date)
        before: 时间标志
            - True: 开盘前，不执行
            - False: 开盘后或收盘后，执行
    
    Returns:
        None
    """
    # ==================== 步骤1: 检查是否开盘 ====================
    if before:
        logging.info(f"⚠️  开盘前不执行股票基础数据任务")
        return
    
    try:
        # ==================== 步骤1.5: 检查是否已有当天数据 ====================
        table_name = tbs.TABLE_CN_STOCK_SPOT['name']
        if check_and_skip_if_exists(table_name, date_obj):
            return
        
        # ==================== 步骤2: 获取股票数据 ====================
        logging.info("=" * 20)
        logging.info(f"开始获取股票基础数据: {date_obj} | 📋 目标表: cn_stock_spot (股票实时行情)")
        logging.info("=" * 20)
        
        # 从单例获取数据（如果未缓存则自动抓取）
        data = stock_data(date_obj).get_data()
        
        # 检查数据有效性
        if data is None or len(data.index) == 0:
            logging.warning(f"⚠️  股票数据为空，可能网络问题或非交易日")
            return
        
        logging.info(f"✅ 成功获取股票数据: {len(data)} 条记录")
        
        # ==================== 步骤3: 准备数据库操作 ====================
        table_name = tbs.TABLE_CN_STOCK_SPOT['name']  # 'cn_stock_spot'
        
        # 检查表是否存在
        if mdb.checkTableIsExist(table_name):
            cols_type = None  # 表已存在，不需要字段类型
        else:
            # 首次运行，需要创建表
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SPOT['columns'])
            logging.info(f"📋 表 {table_name} 不存在，将创建新表")
        
        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (股票基础数据)")
        logging.info(f"   目标日期: {date_obj}")
        logging.info(f"   数据量: {len(data)}条记录")
        logging.info(f"   开始插入数据...")
        
        # ==================== 步骤4: 插入数据 ====================
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"✅ 股票基础数据保存成功: {date_obj}")
        logging.info(f"   表名: {table_name}")
        logging.info(f"   记录数: {len(data)}")
        logging.info(f"   字段数: {len(data.columns)}")
        logging.info("=" * 20)
        
    except Exception as e:
        logging.error(f"❌ 股票基础数据任务失败: {e}", exc_info=True)
        raise


def main():
    """主入口函数"""
    logging.info("=" * 20)
    logging.info("开始执行股票基础数据任务")
    logging.info("=" * 20)
    
    # 使用 run_with_args 处理命令行参数和交易日判断
    # 默认 before=False（盘后执行）
    runt.run_with_args(save_stock_spot_data, False)
    
    logging.info("")
    logging.info("=" * 20)
    logging.info("股票基础数据任务执行完成")
    logging.info("=" * 20)


if __name__ == '__main__':
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()
