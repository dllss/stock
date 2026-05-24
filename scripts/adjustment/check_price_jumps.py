#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""检查cn_stock_spot表中是否存在除权导致的价格跳变"""
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

import instock.lib.database as mdb
import pandas as pd

print("=" * 80)
print("检查价格异常跳变的股票")
print("=" * 80)

# 查询最近10天的数据，检查相邻两天的价格变化
sql = """
SELECT 
    a.code,
    a.name,
    a.date as date1,
    a.new_price as price1,
    b.date as date2,
    b.new_price as price2,
    b.change_rate,
    ROUND((b.new_price - a.new_price) / a.new_price * 100, 2) as actual_change
FROM cn_stock_spot a
INNER JOIN cn_stock_spot b ON a.code = b.code 
    AND b.date = (SELECT MIN(c.date) FROM cn_stock_spot c WHERE c.code = a.code AND c.date > a.date)
WHERE a.date >= DATE_SUB(CURDATE(), INTERVAL 10 DAY)
    AND ABS((b.new_price - a.new_price) / a.new_price) > 0.15
ORDER BY ABS((b.new_price - a.new_price) / a.new_price) DESC
LIMIT 50
"""

try:
    df = pd.read_sql(sql, con=mdb.engine())
    
    if len(df) == 0:
        print("✅ 未发现价格异常跳变的股票")
    else:
        print(f"⚠️ 发现 {len(df)} 只股票存在价格异常跳变（>15%）\n")
        print(df.to_string())
        
        print("\n" + "=" * 80)
        print("分析结果")
        print("=" * 80)
        
        # 统计change_rate和actual_change差异大的记录
        df['diff'] = abs(df['change_rate'] - df['actual_change'])
        suspicious = df[df['diff'] > 5]  # 差异超过5%
        
        if len(suspicious) > 0:
            print(f"\n❌ 发现 {len(suspicious)} 条可疑记录（涨跌幅与实际变化不符）:")
            print(suspicious[['code', 'name', 'date1', 'date2', 'price1', 'price2', 'change_rate', 'actual_change', 'diff']].to_string())
            print("\n这些记录可能存在除权数据不一致问题！")
        else:
            print("\n✅ 所有记录的涨跌幅与实际变化一致")
            
except Exception as e:
    print(f"❌ 查询失败: {e}")
    import traceback
    traceback.print_exc()
