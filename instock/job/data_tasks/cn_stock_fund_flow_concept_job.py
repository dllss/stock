#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
概念板块资金流向数据任务 - 独立执行脚本
========================================
功能：抓取并保存概念板块资金流向数据（今日/3日/5日）
目标表：cn_stock_fund_flow_concept
API请求数：约30-50次（3个周期 × 页数）
执行时间：约3-5分钟

运行方式：
    python instock/job/fund_flow/concept_fund_flow_job.py
    
依赖关系：
    无（独立任务）
    
数据用途：
    - 发现热门概念板块
    - 寻找热点题材
    - 概念轮动分析
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd

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


def run_check_concept_fund_flow(times):
    """
    顺序抓取概念板块资金流向数据
    
    参数:
        times: 时间周期元组 (0, 1, 2)
    
    返回:
        dict: {周期索引: DataFrame}
    """
    data = {}
    try:
        # 顺序执行，避免日志混乱和反爬虫风险
        for k in times:
            period_names = {0: '今日', 1: '3日', 2: '5日'}
            period_name = period_names.get(k, f'周期{k}')
            logging.info("")
            logging.info(f"[INFO] 开始获取{period_name}概念板块资金流向数据...")
            
            # 打印目标数据库表名
            tbs_table = tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT
            table_name = tbs_table['name']
            table_cn = tbs_table.get('cn', '概念资金流向')
            logging.info(f"   目标表: {table_name} ({table_cn})")
            
            _data_ = stf.fetch_stocks_sector_fund_flow(1, k)
            if _data_ is not None:
                data[k] = _data_
                logging.info(f"✅ {period_name}数据获取成功: {len(_data_)}条记录")
            else:
                logging.warning(f"⚠️ {period_name}数据获取失败")
    except Exception as e:
        logging.error(f"❌ 概念板块资金流向任务异常：{e}", exc_info=True)
    
    if not data:
        return None
    else:
        return data


def save_concept_fund_flow_data(date, before=True):
    """
    主函数：抓取并保存概念板块资金流向数据
    
    参数:
        date: 日期对象
        before: 是否在开盘前运行（默认True，跳过）
    """
    if before:
        logging.info("⚠️ 开盘前不执行概念资金流向任务")
        return

    sector_type = '概念'
    
    try:
        # ==================== 步骤1: 检查是否已有当天数据 ====================
        table_name = tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT['name']
        if check_and_skip_if_exists(table_name, date):
            return
        
        logging.info("=" * 20)
        logging.info(f"[{date}] 开始获取{sector_type}资金流向数据...")
        
        # 抓取3个时间周期：今日、3日、5日
        times = tuple(range(3))
        period_names = {0: '今日', 1: '3日', 2: '5日'}
        
        logging.info(f"[INFO] 正在拉取{sector_type}资金流向数据...")
        results = run_check_concept_fund_flow(times)
        if results is None:
            logging.warning(f"⚠️ {sector_type}资金流向数据为空，跳过")
            return

        # 合并3个周期的数据
        logging.info("")
        logging.info(f"[INFO] 开始合并{len(times)}个周期的{sector_type}资金流向数据...")
        data = None
        for t in times:
            if t == 0:
                data = results.get(t)
                logging.info(f"   基础数据（今日）：{data.shape[0]}条记录")
            else:
                r = results.get(t)
                if r is not None:
                    period_name = period_names.get(t, f'周期{t}')
                    logging.info(f"   合并{period_name}数据：{r.shape[0]}条记录")
                    # 按板块名称合并
                    data = pd.merge(data, r, on=['name'], how='left')
        
        if data is None or len(data.index) == 0:
            logging.warning("⚠️ 合并后数据为空")
            return
        
        logging.info(f"✅ 数据合并完成：最终{data.shape[0]}条记录，{data.shape[1]}个字段")

        # 添加日期列
        data.insert(0, 'date', date.strftime("%Y-%m-%d"))

        # 选择概念资金流向表
        tbs_table = tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT
        
        table_name = tbs_table['name']
        table_cn = tbs_table.get('cn', f'{sector_type}资金流向')
        
        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} ({table_cn})")
        logging.info(f"   目标日期: {date.strftime('%Y-%m-%d')}")
        logging.info(f"   数据量: {len(data)}条记录")
        
        # 检查表是否存在
        if mdb.checkTableIsExist(table_name):
            cols_type = None  # 表已存在，不需要字段类型
        else:
            logging.info(f"   ⚠️ 表不存在，将创建新表")
            cols_type = tbs.get_field_types(tbs_table['columns'])

        logging.info(f"   开始插入数据...")
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`name`")
        
        logging.info(f"✅ 保存{sector_type}资金流向数据成功：{len(data)}条")
        logging.info(f"   表名: {table_name}")
        logging.info(f"   主键: date, name")
        logging.info(f"   字段数: {len(data.columns)}")
        
    except Exception as e:
        logging.error(f"❌ 概念资金流向任务处理异常：{e}", exc_info=True)


def main():
    """主入口函数"""
    logging.info("=" * 20)
    logging.info("开始执行概念板块资金流向数据任务")
    logging.info("=" * 20)
    
    # 设置 before=False（盘后执行）
    runt.run_with_args(save_concept_fund_flow_data, False)
    
    logging.info("")
    logging.info("=" * 20)
    logging.info("概念板块资金流向数据任务执行完成")
    logging.info("=" * 20)


if __name__ == '__main__':
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()
