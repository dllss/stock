#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
资金流向API综合测试工具
================================

功能说明：
整合了资金流向相关的多个测试功能，包括：
1. API连接测试
2. 数据获取功能测试
3. 快速数据预览
4. 数据保存测试

使用示例：
# 运行所有测试
python test_fund_flow_comprehensive.py

# 仅测试API连接
python test_fund_flow_comprehensive.py --test api

# 仅测试数据获取
python test_fund_flow_comprehensive.py --test fetch

# 快速预览数据
python test_fund_flow_comprehensive.py --test quick

# 测试数据保存
python test_fund_flow_comprehensive.py --test save
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import pandas as pd
from instock.core.crawling import stock_fund_em
from instock.core import stockfetch as stf
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_api_connection():
    """测试API连接"""
    logger.info("="*80)
    logger.info("测试1: API连接测试")
    logger.info("="*80)
    
    try:
        # 测试个股资金流向API
        logger.info("测试个股资金流向API...")
        df = stock_fund_em.stock_individual_fund_flow_rank(indicator="今日")
        
        if df is not None and len(df) > 0:
            logger.info(f"✅ API连接成功，获取 {len(df)} 条记录")
            logger.info(f"   字段: {list(df.columns)[:5]}...")
        else:
            logger.warning("⚠️ API返回空数据")
            
    except Exception as e:
        logger.error(f"❌ API连接失败: {e}", exc_info=True)
        return False
    
    return True


def test_data_fetch():
    """测试数据获取功能"""
    logger.info("\n" + "="*80)
    logger.info("测试2: 数据获取功能测试")
    logger.info("="*80)
    
    try:
        # 测试行业资金流向
        logger.info("测试行业资金流向...")
        industry_data = stf.fetch_stocks_sector_fund_flow(0, 0)  # 行业, 今日
        
        if industry_data is not None and len(industry_data) > 0:
            logger.info(f"✅ 行业资金流向: {len(industry_data)}条记录")
            print(industry_data.head(3).to_string(index=False))
        else:
            logger.warning("⚠️ 行业资金流向数据为空")
        
        # 测试概念资金流向
        logger.info("\n测试概念资金流向...")
        concept_data = stf.fetch_stocks_sector_fund_flow(1, 0)  # 概念, 今日
        
        if concept_data is not None and len(concept_data) > 0:
            logger.info(f"✅ 概念资金流向: {len(concept_data)}条记录")
        else:
            logger.warning("⚠️ 概念资金流向数据为空")
            
    except Exception as e:
        logger.error(f"❌ 数据获取失败: {e}", exc_info=True)
        return False
    
    return True


def test_quick_preview():
    """快速数据预览"""
    logger.info("\n" + "="*80)
    logger.info("测试3: 快速数据预览")
    logger.info("="*80)
    
    try:
        # 获取今日资金流向前10
        df = stock_fund_em.stock_individual_fund_flow_rank(indicator="今日")
        
        if df is not None and len(df) > 0:
            logger.info("今日资金流向前10:")
            preview_cols = ['代码', '名称', '最新价', '今日涨跌幅', '今日主力净流入-净额']
            available_cols = [col for col in preview_cols if col in df.columns]
            
            if available_cols:
                print(df[available_cols].head(10).to_string(index=False))
            else:
                print(df.head(10).to_string(index=False))
        else:
            logger.warning("⚠️ 无数据可预览")
            
    except Exception as e:
        logger.error(f"❌ 预览失败: {e}", exc_info=True)
        return False
    
    return True


def test_data_save():
    """测试数据保存功能"""
    logger.info("\n" + "="*80)
    logger.info("测试4: 数据保存测试")
    logger.info("="*80)
    
    try:
        from datetime import date
        today = date.today().strftime('%Y-%m-%d')
        
        logger.info(f"测试日期: {today}")
        
        # 尝试获取并显示保存逻辑（不实际保存）
        data = stf.fetch_stocks_fund_flow(today)
        
        if data is not None and len(data) > 0:
            logger.info(f"✅ 数据准备成功: {len(data)}条记录, {len(data.columns)}个字段")
            logger.info(f"   字段列表: {list(data.columns)}")
            logger.info(f"   ⚠️ 注意: 此为测试模式，未实际保存到数据库")
        else:
            logger.warning("⚠️ 数据为空，无法测试保存")
            
    except Exception as e:
        logger.error(f"❌ 保存测试失败: {e}", exc_info=True)
        return False
    
    return True


def main():
    parser = argparse.ArgumentParser(description='资金流向API综合测试工具')
    parser.add_argument('--test', type=str, 
                       choices=['api', 'fetch', 'quick', 'save', 'all'],
                       default='all', help='测试类型')
    
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("资金流向API综合测试")
    logger.info("="*80)
    
    results = {}
    
    try:
        if args.test in ['api', 'all']:
            results['API连接'] = test_api_connection()
        
        if args.test in ['fetch', 'all']:
            results['数据获取'] = test_data_fetch()
        
        if args.test in ['quick', 'all']:
            results['快速预览'] = test_quick_preview()
        
        if args.test in ['save', 'all']:
            results['数据保存'] = test_data_save()
        
        # 打印总结
        logger.info("\n" + "="*80)
        logger.info("测试总结")
        logger.info("="*80)
        
        for test_name, result in results.items():
            status = "✅ 通过" if result else "❌ 失败"
            logger.info(f"{test_name}: {status}")
        
        all_passed = all(results.values())
        logger.info("="*80)
        if all_passed:
            logger.info("✅ 所有测试通过")
        else:
            logger.warning("⚠️ 部分测试失败，请检查上方错误信息")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"❌ 测试执行失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
