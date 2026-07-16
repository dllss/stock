#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""查询最近发生除权的股票"""
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

import instock.lib.database as mdb
import pandas as pd

print("=" * 80)
print("查询最近发生除权的股票")
print("=" * 80)

# 查询最近30天内发生除权的股票
sql = """
SELECT 
    code,
    name,
    ex_dividend_date,
    bonusaward_rate,
    gift_rate,
    allotment_rate
FROM cn_stock_bonus 
WHERE ex_dividend_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
ORDER BY ex_dividend_date DESC
LIMIT 50
"""

try:
    df = pd.read_sql(sql, con=mdb.engine())
    
    if len(df) == 0:
        print("✅ 最近30天没有除权记录")
    else:
        print(f"发现 {len(df)} 条除权记录:\n")
        print(df.to_string())
        
        # 检查这些股票是否在价格异常列表中
        print("\n" + "=" * 80)
        print("与价格异常跳变股票对比")
        print("=" * 80)
        
        # 之前发现的23只异常股票
        abnormal_codes = [
            '301283', '301319', '301608', '603162', '605289', '002245',
            '002956', '301232', '603179', '301310', '301128', '300516',
            '301181', '001316', '000034', '301076', '301349', '605499',
            '301162', '002458', '002560', '603039', '300818'
        ]
        
        matched = df[df['code'].isin(abnormal_codes)]
        if len(matched) > 0:
            print(f"\n✅ 发现 {len(matched)} 只异常股票确实有除权记录:")
            print(matched.to_string())
        else:
            print("\n⚠️ 异常股票在bonus表中没有找到除权记录（可能数据未采集）")
            
except Exception as e:
    print(f"❌ 查询失败: {e}")
    import traceback
    traceback.print_exc()
