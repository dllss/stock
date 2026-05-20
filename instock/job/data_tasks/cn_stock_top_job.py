#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
龙虎榜汇总数据任务 - 独立执行脚本
==================================
功能：抓取并保存龙虎榜汇总数据（新浪源）
目标表：cn_stock_top_list
API请求数：约5-10次（分页获取）
执行时间：约1分钟

运行方式：
    python instock/job/lhb/lhb_summary_job.py
    
依赖关系：
    无（独立任务）
    
数据用途：
    - 查看龙虎榜整体情况
    - 统计上榜次数和金额
    - 发现活跃席位
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
from instock.job.task_utils import check_and_skip_if_exists

__author__ = 'AI Assistant'
__date__ = '2026/05/09'


def save_lhb_summary_data(date, before=True):
    """
    主函数：抓取并保存龙虎榜汇总数据
    
    参数:
        date: 日期对象
        before: 是否在开盘前运行（默认True，跳过）
    """
    if before:
        logging.info("⚠️ 开盘前不执行龙虎榜汇总任务")
        return

    try:
        table_name = tbs.TABLE_CN_STOCK_TOP['name']
        
        # 步骤1: 检查当天是否已有数据
        if check_and_skip_if_exists(table_name, date):
            return
        
        # 步骤2: 抓取数据
        logging.info("=" * 20)
        logging.info(f"[{date}] 开始获取龙虎榜汇总数据（新浪）...")
        
        data = stf.fetch_stock_top_data(date)
        if data is None or len(data.index) == 0:
            logging.info("龙虎榜汇总数据为空，跳过")
            return

        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (龙虎榜汇总)")
        logging.info(f"   目标日期: {date.strftime('%Y-%m-%d')}")
        logging.info(f"   数据量: {len(data)}条记录")
        
        # 检查表是否存在
        if mdb.checkTableIsExist(table_name):
            cols_type = None  # 表已存在，不需要字段类型
        else:
            logging.info(f"   ⚠️ 表不存在，将创建新表")
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_TOP['columns'])

        logging.info(f"   开始插入数据...")
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"✅ 保存龙虎榜汇总数据成功：{len(data)}条")
        logging.info(f"   表名: {table_name}")
        logging.info(f"   主键: date, code")
        logging.info(f"   字段数: {len(data.columns)}")
        
    except Exception as e:
        logging.error(f"❌ 龙虎榜汇总任务处理异常：{e}", exc_info=True)


def main():
    """主入口函数"""
    logging.info("=" * 20)
    logging.info("开始执行龙虎榜汇总数据任务")
    logging.info("=" * 20)
    
    # 设置 before=False（盘后执行）
    runt.run_with_args(save_lhb_summary_data, False)
    
    logging.info("")
    logging.info("=" * 20)
    logging.info("龙虎榜汇总数据任务执行完成")
    logging.info("=" * 20)


if __name__ == '__main__':
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()
