#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
尾盘抢筹数据任务 - 独立执行脚本
==================================
功能：抓取并保存尾盘抢筹数据
目标表：cn_stock_chip_race_end
API请求数：约1次
执行时间：约10-30秒

运行方式：
    python instock/job/data_tasks/chip_race_end_job.py
    
依赖关系：
    无（独立任务）
    
数据用途：
    - 发现强势股
    - 短线交易机会
    - 尾盘竞价参考
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


def save_chip_race_end_data(date, before=False):
    """
    主函数：抓取并保存尾盘抢筹数据
    
    参数:
        date: 日期对象
        before: 是否在开盘前运行（默认False，执行）
    """
    if before:
        logging.info("⚠️ 开盘前不执行尾盘抢筹任务")
        return

    try:
        # 检查是否为交易日（周末无数据）
        import instock.lib.trade_time as trd
        if not trd.is_trade_date(date):
            logging.info(f"⚠️ {date} 不是交易日，跳过")
            return
        
        table_name = tbs.TABLE_CN_STOCK_CHIP_RACE_END['name']
        
        # 步骤1: 检查当天是否已有数据
        if check_and_skip_if_exists(table_name, date):
            return
        
        # 步骤2: 抓取数据
        logging.info("=" * 20)
        logging.info(f"[{date}] 开始获取尾盘抢筹数据...")
        
        data = stf.fetch_stock_chip_race_end(date)
        logging.info(f"   fetch_stock_chip_race_end 返回类型: {type(data)}")
        if data is not None:
            logging.info(f"   数据形状: {data.shape if hasattr(data, 'shape') else 'N/A'}")
        
        if data is None or len(data.index) == 0:
            logging.info("尾盘抢筹数据为空，跳过")
            return

        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (尾盘抢筹)")
        logging.info(f"   目标日期: {date.strftime('%Y-%m-%d')}")
        logging.info(f"   数据量: {len(data)}条记录")
        
        # 检查表是否存在
        if mdb.checkTableIsExist(table_name):
            cols_type = None  # 表已存在，不需要字段类型
        else:
            logging.info(f"   ⚠️ 表不存在，将创建新表")
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_CHIP_RACE_END['columns'])

        logging.info(f"   开始插入数据...")
        try:
            insert_result = mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
            logging.info(f"   insert_db_from_df 返回值: {insert_result}")
        except Exception as insert_err:
            logging.error(f"   ❌ 插入数据时发生异常: {insert_err}", exc_info=True)
            raise
        
        if insert_result:
            logging.info(f"✅ 保存尾盘抢筹数据成功：{len(data)}条")
            logging.info(f"   表名: {table_name}")
            logging.info(f"   主键: date, code")
            logging.info(f"   字段数: {len(data.columns)}")
        else:
            logging.error(f"❌ 保存尾盘抢筹数据失败！请检查上方错误信息")
            logging.error(f"   可能原因:")
            logging.error(f"   1. 数据库连接失败")
            logging.error(f"   2. 表结构不匹配")
            logging.error(f"   3. 数据格式错误")
            logging.error(f"   4. 主键冲突")
            
    except Exception as e:
        logging.error(f"❌ 尾盘抢筹任务处理异常：{e}", exc_info=True)
        import traceback
        traceback.print_exc()


def main():
    """主入口函数"""
    logging.info("=" * 20)
    logging.info("开始执行尾盘抢筹数据任务")
    logging.info("=" * 20)
    
    # 设置 before=False（盘后执行）
    runt.run_with_args(save_chip_race_end_data, False)
    
    logging.info("")
    logging.info("=" * 20)
    logging.info("尾盘抢筹数据任务执行完成")
    logging.info("=" * 20)


if __name__ == '__main__':
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()
