#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询特定日期的股票收盘数据
================================

功能说明：
从数据库中查询指定日期的股票收盘数据
支持多种筛选和排序方式

使用示例：
# 查询某日所有股票
python query_daily_close.py 2024-01-15

# 查询涨跌幅前20
python query_daily_close.py 2024-01-15 --top 20 --order-by change_rate

# 查询涨停股票
python query_daily_close.py 2024-01-15 --filter "change_rate >= 9.5"

# 查询指定行业
python query_daily_close.py 2024-01-15 --industry "银行"
"""

import sys
import os
import argparse
import pandas as pd

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import instock.lib.database as mdb


def query_daily_close(date, top_n=None, order_by='change_rate', 
                      ascending=False, filter_condition=None,
                      industry=None, columns=None):
    """
    查询特定日期的股票收盘数据
    
    参数：
    date (str): 日期，格式 YYYY-MM-DD
    top_n (int): 返回前N条记录
    order_by (str): 排序字段，默认涨跌幅
    ascending (bool): 是否升序，默认降序
    filter_condition (str): 筛选条件SQL片段
    industry (str): 行业筛选
    columns (list): 要查询的列
    
    返回：
    pd.DataFrame: 查询结果
    """
    
    # 默认查询的列
    if columns is None:
        columns = [
            'code',
            'name',
            'new_price',
            'change_rate',
            'ups_downs',
            'volume',
            'deal_amount',
            'turnoverrate',
            'amplitude',
            'pe9',
            'pbnewmrq',
            'total_market_cap',
            'free_cap',
            'industry'
        ]
    
    # 构建SQL查询
    select_cols = ', '.join(columns)
    
    sql = f"""
    SELECT {select_cols}
    FROM cn_stock_spot
    WHERE date = '{date}'
    """
    
    # 添加行业筛选
    if industry:
        sql += f" AND industry LIKE '%{industry}%'"
    
    # 添加自定义筛选条件
    if filter_condition:
        sql += f" AND {filter_condition}"
    
    # 添加排序
    order_direction = "ASC" if ascending else "DESC"
    sql += f" ORDER BY {order_by} {order_direction}"
    
    # 添加数量限制
    if top_n:
        sql += f" LIMIT {top_n}"
    
    try:
        # 执行查询
        df = mdb.executeSql(sql)
        
        if df.empty:
            print(f"⚠️  {date} 没有数据，可能需要先运行数据抓取任务")
            return None
        
        # 格式化输出
        print(f"\n{'='*80}")
        print(f"📊 {date} 股票收盘数据")
        print(f"{'='*80}")
        print(f"共 {len(df)} 条记录")
        
        if top_n:
            direction = "最小" if ascending else "最大"
            print(f"按 {order_by} {direction}的 {top_n} 条记录\n")
        else:
            print(f"按 {order_by} 排序\n")
        
        # 显示数据
        pd.set_option('display.max_rows', top_n if top_n else 100)
        pd.set_option('display.width', 200)
        pd.set_option('display.float_format', lambda x: f'{x:.2f}')
        
        # 重命名列为中文
        column_names_cn = {
            'code': '代码',
            'name': '名称',
            'new_price': '收盘价',
            'change_rate': '涨跌幅%',
            'ups_downs': '涨跌额',
            'volume': '成交量',
            'deal_amount': '成交额',
            'turnoverrate': '换手率%',
            'amplitude': '振幅%',
            'pe9': '市盈率TTM',
            'pbnewmrq': '市净率',
            'total_market_cap': '总市值',
            'free_cap': '流通市值',
            'industry': '行业'
        }
        
        df_display = df.rename(columns=column_names_cn)
        print(df_display)
        
        # 统计信息
        print(f"\n{'='*80}")
        print("📈 统计信息：")
        print(f"{'='*80}")
        print(f"平均涨跌幅: {df['change_rate'].mean():.2f}%")
        print(f"中位数涨跌幅: {df['change_rate'].median():.2f}%")
        print(f"涨停家数: {len(df[df['change_rate'] >= 9.5])}")
        print(f"跌停家数: {len(df[df['change_rate'] <= -9.5])}")
        print(f"上涨家数: {len(df[df['change_rate'] > 0])}")
        print(f"下跌家数: {len(df[df['change_rate'] < 0])}")
        print(f"平盘家数: {len(df[df['change_rate'] == 0])}")
        
        return df
        
    except Exception as e:
        print(f"❌ 查询失败: {e}")
        return None


def main():
    """主函数 - 解析命令行参数"""
    
    parser = argparse.ArgumentParser(
        description='查询特定日期的股票收盘数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例：
  # 查询某日所有股票
  python query_daily_close.py 2024-01-15
  
  # 查询涨跌幅前20
  python query_daily_close.py 2024-01-15 --top 20
  
  # 查询跌幅前10
  python query_daily_close.py 2024-01-15 --top 10 --ascending
  
  # 查询涨停股票
  python query_daily_close.py 2024-01-15 --filter "change_rate >= 9.5"
  
  # 查询银行行业
  python query_daily_close.py 2024-01-15 --industry "银行"
  
  # 查询高换手率股票
  python query_daily_close.py 2024-01-15 --top 20 --order-by turnoverrate
        """
    )
    
    parser.add_argument('date', help='查询日期，格式：YYYY-MM-DD')
    parser.add_argument('--top', type=int, help='返回前N条记录')
    parser.add_argument('--order-by', default='change_rate',
                       choices=['change_rate', 'new_price', 'volume', 
                               'deal_amount', 'turnoverrate', 'amplitude',
                               'pe9', 'total_market_cap'],
                       help='排序字段（默认：涨跌幅）')
    parser.add_argument('--ascending', action='store_true',
                       help='升序排列（默认降序）')
    parser.add_argument('--filter', help='自定义筛选条件（SQL WHERE子句）')
    parser.add_argument('--industry', help='行业筛选')
    
    args = parser.parse_args()
    
    # 执行查询
    query_daily_close(
        date=args.date,
        top_n=args.top,
        order_by=args.order_by,
        ascending=args.ascending,
        filter_condition=args.filter,
        industry=args.industry
    )


if __name__ == '__main__':
    main()
