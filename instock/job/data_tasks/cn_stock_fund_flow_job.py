#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
个股资金流向数据任务 - 独立执行脚本
====================================
功能：抓取并保存个股资金流向数据（今日/3日/5日/10日）
目标表：cn_stock_fund_flow
API请求数：约440次（4个周期 × 110页）
执行时间：约40-50分钟（按每分钟10次计算）

运行方式：
    python instock/job/fund_flow/stock_fund_flow_job.py
    
依赖关系：
    无（独立任务）
    
数据用途：
    - 寻找主力流入的股票
    - 避免主力流出的股票
    - 判断资金趋势
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd
import time
import random
from instock.config.delay_manager import sleep_with_delay

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


def fetch_single_period(k):
    """
    抓取单个周期的资金流向数据
    
    参数:
        k: 周期索引 (0=今日, 1=3日, 2=5日, 3=10日)
    
    返回:
        DataFrame 或 None
    """
    period_names = {0: '今日', 1: '3日', 2: '5日', 3: '10日'}
    period_name = period_names.get(k, f'周期{k}')
    
    try:
        logging.info("")
        logging.info(f"[INFO] 正在拉取{period_name}个股资金流向数据...")
        
        # 打印目标数据库表名
        tbs_table = tbs.TABLE_CN_STOCK_FUND_FLOW
        table_name = tbs_table['name']
        table_cn = tbs_table.get('cn', '个股资金流向')
        logging.info(f"   目标表: {table_name} ({table_cn})")
        
        _data = stf.fetch_stocks_fund_flow(k)
        
        if _data is not None:
            logging.info(f"✅ {period_name}资金流向数据拉取成功：{_data.shape[0]}条记录，{_data.shape[1]}个字段")
            logging.info(f"   字段列表: {', '.join(_data.columns.tolist()[:5])}...")
            return _data
        else:
            logging.warning(f"⚠️ {period_name}资金流向数据拉取失败")
            return None
    except Exception as e:
        logging.error(f"❌ 抓取{period_name}资金流向数据异常: {e}")
        return None


def save_stock_fund_flow_data(date, before=True):
    """
    主函数：抓取并保存个股资金流向数据
    
    参数:
        date: 日期对象
        before: 是否在开盘前运行（默认True，跳过）
    """
    if before:
        logging.info("⚠️ 开盘前不执行个股资金流向任务")
        return

    try:
        logging.info("=" * 20)
        logging.info(f"[{date}] 开始获取个股资金流向数据（今日/3日/5日/10日）...")
        
        # 定义时间周期：0=今日，1=3日，2=5日，3=10日
        times = tuple(range(4))
        
        # 顺序抓取4个周期的资金流向数据
        results = {}
        for k in times:
            _data = fetch_single_period(k)
            if _data is not None:
                results[k] = _data
        
        if not results:
            logging.warning("⚠️ 所有周期数据都为空，跳过")
            return

        # 合并4个周期的数据
        logging.info(f"\n[INFO] 开始合并{len(results)}个周期的资金流向数据...")
        data = None
        for t in sorted(results.keys()):
            if t == 0:
                # 第一个周期（今日），作为基础数据
                data = results.get(t)
                logging.info(f"   基础数据（今日）：{data.shape[0]}条记录")
            else:
                # 其他周期，合并到基础数据
                r = results.get(t)
                if r is not None:
                    period_name = {1: '3日', 2: '5日', 3: '10日'}.get(t, f'周期{t}')
                    logging.info(f"   合并{period_name}数据：{r.shape[0]}条记录")
                    # 删除重复列（name和new_price在第一个周期已有）
                    r.drop(columns=['name', 'new_price'], inplace=True)
                    # 按代码合并
                    data = pd.merge(data, r, on=['code'], how='left')
        
        if data is None or len(data.index) == 0:
            logging.warning("⚠️ 合并后数据为空")
            return
        
        logging.info(f"✅ 数据合并完成：最终{data.shape[0]}条记录，{data.shape[1]}个字段")

        # 添加日期列
        data.insert(0, 'date', date.strftime("%Y-%m-%d"))

        table_name = tbs.TABLE_CN_STOCK_FUND_FLOW['name']
        table_cn = tbs.TABLE_CN_STOCK_FUND_FLOW.get('cn', '个股资金流向')
        
        # 步骤1: 检查当天是否已有数据
        if check_and_skip_if_exists(table_name, date):
            return
        
        # 步骤2: 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} ({table_cn})")
        logging.info(f"   目标日期: {date.strftime('%Y-%m-%d')}")
        logging.info(f"   数据量: {len(data)}条记录")
        
        # 检查表是否存在
        if mdb.checkTableIsExist(table_name):
            cols_type = None  # 表已存在，不需要字段类型
        else:
            logging.info(f"   ⚠️ 表不存在，将创建新表")
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_FUND_FLOW['columns'])

        logging.info(f"   开始插入数据...")
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"✅ 保存个股资金流向数据成功：{len(data)}条")
        logging.info(f"   表名: {table_name}")
        logging.info(f"   主键: date, code")
        logging.info(f"   字段数: {len(data.columns)}")
        
    except Exception as e:
        logging.error(f"❌ 个股资金流向任务处理异常：{e}", exc_info=True)


def main():
    """主入口函数"""
    logging.info("=" * 20)
    logging.info("开始执行个股资金流向数据任务")
    logging.info("=" * 20)
    
    # 直接使用日期参数，并设置 before=False（盘后执行）
    runt.run_with_args(save_stock_fund_flow_data, False)
    
    logging.info("=" * 20)
    logging.info("个股资金流向数据任务执行完成")
    logging.info("=" * 20)


if __name__ == '__main__':
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()
