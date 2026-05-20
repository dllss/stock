#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务执行工具函数
提供通用的数据检查和跳过逻辑
"""

import logging
import inspect
from instock.lib import database as mdb


def log_task_start(task_name, description=""):
    """
    打印任务开始的日志，与上一个任务有明显的视觉分隔
    
    Args:
        task_name: 任务名称（英文）
        description: 任务描述（中文）
    
    Example:
        log_task_start("basic_data_daily", "基础数据抓取")
    """
    # 获取调用栈帧
    frame = inspect.currentframe().f_back
    caller_filename = frame.f_code.co_filename.split('/')[-1].split('\\')[-1]  # 获取文件名
    caller_lineno = frame.f_lineno  # 获取行号
    
    separator = "=" * 80
    logging.info("")
    logging.info(separator)
    logging.info(f"🚀 开始执行任务: {task_name} (来自 {caller_filename}:{caller_lineno})")
    if description:
        logging.info(f"📝 任务描述: {description}")
    logging.info(separator)
    logging.info("")


def check_and_skip_if_exists(table_name, date):
    """
    检查指定表在指定日期是否已有数据，如果有则跳过
    
    Args:
        table_name: 表名
        date: 日期对象或日期字符串
        
    Returns:
        bool: True表示已有数据应跳过，False表示无数据需要抓取
    """
    # 如果date是日期对象，转换为字符串
    if hasattr(date, 'strftime'):
        date_str = date.strftime('%Y-%m-%d')
    else:
        date_str = str(date)
    
    # 检查表是否存在
    if not mdb.checkTableIsExist(table_name):
        logging.info(f"📊 {table_name} 表不存在，开始抓取...")
        return False
    
    # 检查当天是否已有数据
    check_sql = f"SELECT COUNT(*) FROM `{table_name}` WHERE `date` = '{date_str}'"
    count = mdb.executeSqlCount(check_sql)
    
    if count > 0:
        logging.info(f"✅ {table_name} 表在 {date_str} 已有 {count} 条数据")
        logging.info(f"⏭️  跳过抓取，直接使用现有数据")
        return True
    else:
        logging.info(f"📊 {table_name} 表在 {date_str} 无数据，开始抓取...")
        return False
