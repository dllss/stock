#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
早盘抢筹数据任务 - 独立执行脚本
================================
功能：抓取并保存早盘抢筹数据
目标表：cn_stock_chip_race_open
API请求数：约1次
执行时间：约几秒钟

运行方式：
    python instock/job/chip_race/chip_race_open_job.py
    
依赖关系：
    无（独立任务）
    
数据用途：
    - 发现开盘后快速拉升的股票
    - 识别资金抢筹信号
    - 短线机会发现
"""

import os
import sys

# 添加项目路径
cpath_current = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(cpath_current)

# 先初始化日志（在其他导入之前）
from instock.lib.logger_config import setup_job_logging
_log_file = setup_job_logging()
print(f'日志文件: {_log_file}')  # 立即打印，确认日志已配置

import logging
from datetime import datetime

import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.stockfetch as stf
from instock.job.task_utils import check_and_skip_if_exists

__author__ = 'AI Assistant'
__date__ = '2026/05/09'


def save_chip_race_open_data(date, before=True):
    """
    主函数：抓取并保存早盘抢筹数据
    
    参数:
        date: 日期对象
        before: 是否在开盘前运行（默认True，跳过）
    """
    if before:
        logging.info("⚠️ 开盘前不执行早盘抢筹任务")
        return

    try:
        # 检查是否为交易日（周末无数据）
        import instock.lib.trade_time as trd
        if not trd.is_trade_date(date):
            logging.info(f"⚠️ {date} 不是交易日，跳过")
            return
        
        table_name = tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['name']
        
        # 步骤1: 检查当天是否已有数据
        if check_and_skip_if_exists(table_name, date):
            return
        
        # 步骤2: 抓取数据
        logging.info("=" * 20)
        logging.info(f"[{date}] 开始获取早盘抢筹数据...")
        
        data = stf.fetch_stock_chip_race_open(date)
        if data is None or len(data.index) == 0:
            logging.info("早盘抢筹数据为空，跳过")
            return

        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (早盘抢筹)")
        logging.info(f"   目标日期: {date.strftime('%Y-%m-%d')}")
        logging.info(f"   数据量: {len(data)}条记录")
        
        # 检查表是否存在
        if mdb.checkTableIsExist(table_name):
            cols_type = None  # 表已存在，不需要字段类型
        else:
            logging.info(f"   ⚠️ 表不存在，将创建新表")
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['columns'])

        logging.info(f"   开始插入数据...")
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"✅ 保存早盘抢筹数据成功：{len(data)}条")
        logging.info(f"   表名: {table_name}")
        logging.info(f"   主键: date, code")
        logging.info(f"   字段数: {len(data.columns)}")
        
    except Exception as e:
        logging.error(f"❌ 早盘抢筹任务处理异常：{e}", exc_info=True)


def main():
    """主入口函数"""
    logging.info("=" * 20)
    logging.info("开始执行早盘抢筹数据任务")
    logging.info("=" * 20)
    
    # 设置 before=False（盘后执行）
    runt.run_with_args(save_chip_race_open_data, False)
    
    logging.info("")
    logging.info("=" * 20)
    logging.info("早盘抢筹数据任务执行完成")
    logging.info("=" * 20)


if __name__ == '__main__':
    main()
