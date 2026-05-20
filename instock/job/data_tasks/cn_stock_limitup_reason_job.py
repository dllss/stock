#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
涨停原因数据任务 - 独立执行脚本
================================
功能：抓取并保存涨停原因数据
目标表：cn_stock_limitup_reason
API请求数：约1次
执行时间：约几秒钟

运行方式：
    python instock/job/chip_race/limitup_reason_job.py
    
依赖关系：
    无（独立任务）
    
数据用途：
    - 了解市场热点
    - 挖掘投资机会
    - 跟踪题材炒作
"""

import os
import sys
import logging
from datetime import datetime

# 添加项目路径
cpath_current = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(cpath_current)

import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.stockfetch as stf

__author__ = 'AI Assistant'
__date__ = '2026/05/09'


def save_limitup_reason_data(date, before=True):
    """
    主函数：抓取并保存涨停原因数据
    
    参数:
        date: 日期对象
        before: 是否在开盘前运行（默认True，跳过）
    """
    if before:
        logging.info("⚠️ 开盘前不执行涨停原因任务")
        return

    try:
        logging.info("=" * 20)
        logging.info(f"[{date}] 开始获取涨停原因数据...")
        
        data = stf.fetch_stock_limitup_reason(date)
        if data is None or len(data.index) == 0:
            logging.info("涨停原因数据为空，跳过")
            return

        table_name = tbs.TABLE_CN_STOCK_LIMITUP_REASON['name']
        
        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (涨停原因)")
        logging.info(f"   目标日期: {date.strftime('%Y-%m-%d')}")
        logging.info(f"   数据量: {len(data)}条记录")
        
        # 删除老数据（涨停原因可能需要多次更新以获取完整数据）
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            logging.info(f"   执行SQL: {del_sql}")
            mdb.executeSql(del_sql)
            logging.info(f"   ✅ 已删除{date}的旧数据")
            cols_type = None
        else:
            logging.info(f"   ⚠️ 表不存在，将创建新表")
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_LIMITUP_REASON['columns'])

        logging.info(f"   开始插入数据...")
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"✅ 保存涨停原因数据成功：{len(data)}条")
        logging.info(f"   表名: {table_name}")
        logging.info(f"   主键: date, code")
        logging.info(f"   字段数: {len(data.columns)}")
        
    except Exception as e:
        logging.error(f"❌ 涨停原因任务处理异常：{e}", exc_info=True)


def main():
    """主入口函数"""
    logging.info("=" * 20)
    logging.info("开始执行涨停原因数据任务")
    logging.info("=" * 20)
    
    # 设置 before=False（盘后执行）
    runt.run_with_args(save_limitup_reason_data, False)
    
    logging.info("")
    logging.info("=" * 20)
    logging.info("涨停原因数据任务执行完成")
    logging.info("=" * 20)


if __name__ == '__main__':
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()
