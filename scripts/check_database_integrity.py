#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库完整性综合检查工具
================================

功能说明：
整合了多个数据库检查功能，包括：
1. 基础数据检查（特定日期、特定股票）
2. ETF数据有效性检查
3. 数据异常值检测（负值、特殊字符等）
4. 资金流向表结构检查

使用示例：
# 检查所有表的基础完整性
python check_database_integrity.py

# 检查特定日期数据
python check_database_integrity.py --date 2024-01-15

# 检查特定股票
python check_database_integrity.py --code 600519

# 仅检查ETF数据
python check_database_integrity.py --check etf

# 仅检查异常值
python check_database_integrity.py --check anomalies
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import pandas as pd
from instock.lib import database
from instock.core import tablestructure
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_specific_date(date_str):
    """检查特定日期的数据"""
    logger.info(f"检查日期: {date_str}")
    
    # 查询cn_stock_spot表
    sql = f"SELECT COUNT(*) as count FROM cn_stock_spot WHERE date='{date_str}'"
    result = database.executeSql(sql)
    
    if result and len(result) > 0:
        count = result[0]['count']
        logger.info(f"✅ cn_stock_spot: {count}条记录")
    else:
        logger.warning(f"⚠️ cn_stock_spot: 无数据")
    
    # 可以扩展检查其他表


def check_specific_stock(code):
    """检查特定股票的数据"""
    logger.info(f"检查股票: {code}")
    
    sql = f"SELECT * FROM cn_stock_spot WHERE code='{code}' ORDER BY date DESC LIMIT 5"
    result = database.executeSql(sql)
    
    if result and len(result) > 0:
        df = pd.DataFrame(result)
        logger.info(f"✅ 最近5条记录:")
        print(df[['date', 'code', 'name', 'new_price', 'change_rate']].to_string(index=False))
    else:
        logger.warning(f"⚠️ 未找到股票 {code} 的数据")


def check_etf_invalid_values():
    """检查ETF数据中的无效值"""
    logger.info("检查ETF数据无效值...")
    
    sql = "SELECT * FROM cn_etf_spot WHERE new_price='-' OR new_price='--' OR new_price='N/A'"
    result = database.executeSql(sql)
    
    if result and len(result) > 0:
        logger.warning(f"⚠️ 发现 {len(result)} 条包含无效值的记录")
        df = pd.DataFrame(result)
        print(df.head(10).to_string(index=False))
    else:
        logger.info("✅ ETF数据无无效值")


def check_negative_values():
    """检查数值字段中的异常负值"""
    logger.info("检查异常负值...")
    
    tables_to_check = [
        ('cn_stock_spot', ['new_price', 'open_price', 'high_price', 'low_price']),
        ('cn_etf_spot', ['new_price', 'volume', 'deal_amount'])
    ]
    
    for table_name, columns in tables_to_check:
        for col in columns:
            sql = f"SELECT COUNT(*) as count FROM `{table_name}` WHERE `{col}` < 0"
            result = database.executeSql(sql)
            
            if result and result[0]['count'] > 0:
                logger.warning(f"⚠️ {table_name}.{col}: {result[0]['count']}条负值记录")
            else:
                logger.info(f"✅ {table_name}.{col}: 无负值")


def check_exact_dash():
    """检查精确的'-'字符"""
    logger.info("检查'-'字符...")
    
    sql = "SELECT COUNT(*) as count FROM cn_stock_spot WHERE new_price='-'"
    result = database.executeSql(sql)
    
    if result and result[0]['count'] > 0:
        logger.warning(f"⚠️ 发现 {result[0]['count']} 条'-'记录")
    else:
        logger.info("✅ 无'-'字符")


def check_fund_flow_table_structure():
    """检查资金流向表结构"""
    logger.info("检查资金流向表结构...")
    
    tables = [
        'cn_stock_fund_flow',
        'cn_stock_fund_flow_industry',
        'cn_stock_fund_flow_concept'
    ]
    
    for table_name in tables:
        sql = f"SHOW COLUMNS FROM `{table_name}`"
        result = database.executeSql(sql)
        
        if result:
            logger.info(f"✅ {table_name}: {len(result)}个字段")
        else:
            logger.warning(f"⚠️ {table_name}: 表不存在或无字段")


def main():
    parser = argparse.ArgumentParser(description='数据库完整性综合检查工具')
    parser.add_argument('--date', type=str, help='检查特定日期 (YYYY-MM-DD)')
    parser.add_argument('--code', type=str, help='检查特定股票代码')
    parser.add_argument('--check', type=str, choices=['etf', 'anomalies', 'fund_flow', 'all'],
                       default='all', help='检查类型')
    
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("开始数据库完整性检查")
    logger.info("="*80)
    
    try:
        if args.date:
            check_specific_date(args.date)
        
        if args.code:
            check_specific_stock(args.code)
        
        if args.check in ['etf', 'all']:
            check_etf_invalid_values()
        
        if args.check in ['anomalies', 'all']:
            check_negative_values()
            check_exact_dash()
        
        if args.check in ['fund_flow', 'all']:
            check_fund_flow_table_structure()
        
        logger.info("="*80)
        logger.info("检查完成")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"❌ 检查失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
