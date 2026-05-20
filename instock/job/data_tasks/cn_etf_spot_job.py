#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF基础数据任务 - cn_etf_spot
==============================

功能说明：
抓取ETF基金每日基础行情数据并保存到数据库

数据来源：
- 东方财富网（ETF实时行情API）

数据内容：
- 基本信息：基金代码、名称
- 价格信息：最新价、涨跌幅、涨跌额
- 成交信息：成交量、成交额
- 市值信息：总市值、流通市值
- 其他：开盘价、最高价、最低价、昨收价、换手率

执行时机：
- 开盘期间：可实时运行（数据会更新）
- 收盘后：建议17:30运行（获取最终数据）
- 非交易日：跳过

数据处理流程：
1. 从网络抓取ETF数据
2. 数据清洗（处理无效值 "-"、"--"、"N/A" 等）
3. 删除数据库中该日期的旧数据
4. 插入新数据到 cn_etf_spot 表
5. 主键：(date, code) 保证唯一性

运行方式：
# 当前交易日
python etf_spot_job.py

# 指定日期
python etf_spot_job.py 2026-05-08

# 多个日期
python etf_spot_job.py 2026-05-08,2026-05-09

# 日期区间
python etf_spot_job.py 2026-05-01 2026-05-31
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import pandas as pd
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.stockfetch as stf
from instock.job.task_utils import check_and_skip_if_exists

__author__ = 'myh'
__date__ = '2026/05/09'


def save_etf_spot_data(date_obj, before=True):
    """
    保存ETF基础数据到数据库
    
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
        logging.info(f"⚠️  开盘前不执行ETF基础数据任务")
        return
    
    try:
        # ==================== 步骤2: 获取ETF数据 ====================
        logging.info("=" * 20)
        logging.info(f"开始获取ETF基础数据: {date_obj}")
        logging.info("=" * 20)
        
        # 从网络抓取ETF数据
        data = stf.fetch_etfs(date_obj)
        
        # 检查数据有效性
        if data is None or len(data.index) == 0:
            logging.warning(f"⚠️  ETF数据为空，可能网络问题或非交易日")
            return
        
        logging.info(f"✅ 成功获取ETF数据: {len(data)} 条记录")
        
        # ==================== 步骤3: 数据清洗 ====================
        # 处理无效值（"-"、"--"、"N/A" 等替换为 0）
        numeric_columns = [
            'new_price', 'change_rate', 'ups_downs',
            'volume', 'deal_amount', 'open_price', 
            'high_price', 'low_price', 'pre_close_price',
            'turnoverrate', 'total_market_cap', 'free_cap'
        ]
        
        for col in numeric_columns:
            if col in data.columns:
                # 将非数字值转换为 NaN，然后填充为 0
                data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0)
                
                # 确保整数类型字段是整数
                if col in ['volume', 'deal_amount', 'total_market_cap', 'free_cap']:
                    data[col] = data[col].astype(int)
        
        logging.info(f"✅ 数据清洗完成")
        
        # ==================== 步骤4: 检查是否已有当天数据 ====================
        table_name = tbs.TABLE_CN_ETF_SPOT['name']  # 'cn_etf_spot'
        if check_and_skip_if_exists(table_name, date_obj):
            return
        
        # ==================== 步骤5: 准备数据库操作 ====================
        table_name = tbs.TABLE_CN_ETF_SPOT['name']  # 'cn_etf_spot'
        
        # 检查表是否存在
        if mdb.checkTableIsExist(table_name):
            cols_type = None  # 表已存在，不需要字段类型
        else:
            # 首次运行，需要创建表
            cols_type = tbs.get_field_types(tbs.TABLE_CN_ETF_SPOT['columns'])
            logging.info(f"📋 表 {table_name} 不存在，将创建新表")
        
        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (ETF基础数据)")
        logging.info(f"   目标日期: {date_obj}")
        logging.info(f"   数据量: {len(data)}条记录")
        logging.info(f"   开始插入数据...")
        
        # ==================== 步骤5: 插入数据 ====================
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"✅ ETF基础数据保存成功: {date_obj}")
        logging.info(f"   表名: {table_name}")
        logging.info(f"   记录数: {len(data)}")
        logging.info(f"   字段数: {len(data.columns)}")
        logging.info("=" * 20)
        
    except Exception as e:
        logging.error(f"❌ ETF基础数据任务失败: {e}", exc_info=True)
        raise


def main():
    """主入口函数"""
    logging.info("=" * 20)
    logging.info("开始执行ETF基础数据任务")
    logging.info("=" * 20)
    
    # 使用 run_with_args 处理命令行参数和交易日判断
    # 默认 before=False（盘后执行）
    runt.run_with_args(save_etf_spot_data, False)
    
    logging.info("")
    logging.info("=" * 20)
    logging.info("ETF基础数据任务执行完成")
    logging.info("=" * 20)


if __name__ == '__main__':
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()
