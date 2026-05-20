#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
综合选股数据任务 - cn_stock_selection
=====================================

功能说明：
抓取东方财富网综合选股数据并保存到数据库

数据来源：
- 东方财富网（综合选股API）

数据内容（200+字段）：
1. 股票范围：市场、行业、地区、概念、风格
2. 基本面指标：估值、盈利、成长、偿债能力
3. 技术面指标：均线、MACD、KDJ、RSI、BOLL
4. 消息面指标：公告、机构关注、持股情况
5. 人气指标：股吧排名、粉丝数量、浏览量
6. 行情数据：股价、成交、资金流向、沪深股通

执行时机：
- 开盘期间：可实时运行
- 收盘后：建议17:30运行
- 非交易日：跳过

运行方式：
python instock/job/data_tasks/cn_stock_selection_job.py
"""

import sys
import os
# 获取项目根目录 (d:\WorkProject\stock)
# __file__ 是当前文件路径,需要向上4级:
# data_tasks -> job -> instock -> stock(项目根目录)
cpath_current = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(cpath_current)

import logging
from datetime import date
import pandas as pd
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.stockfetch as stf
from instock.job.task_utils import check_and_skip_if_exists

__author__ = 'myh'
__date__ = '2026/05/10'


def save_selection_data(date_obj, before=True):
    """保存综合选股数据到数据库"""
    if before:
        logging.info(f"⚠️  开盘前不执行综合选股数据任务")
        return
    
    try:
        logging.info("")
        logging.info("=" * 20)
        logging.info(f"[{date_obj}] 开始获取综合选股数据...")
        logging.info("=" * 20)
        
        # 从东方财富抓取数据
        data = stf.fetch_stock_selection()
        
        if data is None or len(data.index) == 0:
            logging.warning(f"⚠️  综合选股数据为空，可能网络问题或非交易日")
            return
        
        logging.info(f"✅ 成功获取综合选股数据: {len(data)} 条记录")
        
        table_name = tbs.TABLE_CN_STOCK_SELECTION['name']
        _date = data.iloc[0]['date']
        
        # 检查是否已有当天数据
        if check_and_skip_if_exists(table_name, pd.to_datetime(_date)):
            return
        
        # 检查表是否存在
        if mdb.checkTableIsExist(table_name):
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SELECTION['columns'])
            logging.info(f"📋 表 {table_name} 不存在，将创建新表")
        
        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (综合选股)")
        logging.info(f"   目标日期: {_date}")
        logging.info(f"   数据量: {len(data)}条记录")
        logging.info(f"   开始插入数据...")
        
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"✅ 综合选股数据保存成功: {_date}")
        logging.info(f"   表名: {table_name}")
        logging.info(f"   记录数: {len(data)}")
        logging.info(f"   字段数: {len(data.columns)}")
        logging.info("=" * 20)
        
    except Exception as e:
        logging.error(f"❌ 综合选股数据任务失败: {e}", exc_info=True)
        raise


def main():
    """综合选股数据任务主函数"""
    logging.info("")
    logging.info("=" * 20)
    logging.info("开始执行综合选股数据任务")
    logging.info("=" * 20)
    
    runt.run_with_args(save_selection_data, False)
    
    logging.info("")
    logging.info("=" * 20)
    logging.info("综合选股数据任务执行完成")
    logging.info("=" * 20)


if __name__ == '__main__':
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()
