#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""查询指定日期的除权股票"""
import sys
sys.path.insert(0, 'd:/WorkProject/stock')

import instock.lib.database as mdb
import pandas as pd

# 查询2026-05-21发生除权的股票
target_date = '2026-05-21'

print("=" * 80)
print(f"查询 {target_date} 发生除权的股票")
print("=" * 80)

sql = f"""
SELECT 
    code,
    name,
    ex_dividend_date,
    bonusaward_rate,
    convertible_total_rate
FROM cn_stock_bonus 
WHERE ex_dividend_date = '{target_date}'
ORDER BY code
"""

try:
    df = pd.read_sql(sql, con=mdb.engine())
    
    if len(df) == 0:
        print(f"✅ {target_date} 没有股票除权")
    else:
        print(f"\n发现 {len(df)} 只股票在 {target_date} 除权:\n")
        print(df.to_string())
        
        # 检查这些股票是否在价格异常列表中
        abnormal_codes = [
            '301283', '301319', '301608', '603162', '605289', '002245',
            '002956', '301232', '603179', '301310', '301128', '300516',
            '301181', '001316', '000034', '301076', '301349', '605499',
            '301162', '002458', '002560', '603039', '300818'
        ]
        
        matched = df[df['code'].isin(abnormal_codes)]
        if len(matched) > 0:
            print(f"\n⚠️ 其中 {len(matched)} 只股票存在价格异常:")
            print(matched[['code', 'name']].to_string())
            
except Exception as e:
    print(f"❌ 查询失败: {e}")
    import traceback
    traceback.print_exc()
