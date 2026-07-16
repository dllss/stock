#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""查询唯特偶的收益率数据"""
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

import instock.lib.database as mdb
import pandas as pd

# 查询数据库
sql = """
SELECT code, name, date, rate_1, rate_2, rate_3, rate_5, rate_10 
FROM cn_stock_indicators_buy 
WHERE code='301319' AND date='2026-05-20'
"""

df = pd.read_sql(sql, con=mdb.engine())
print("=" * 80)
print("唯特偶 (301319) 在 2026-05-20 的回测数据:")
print("=" * 80)
print(df.to_string())
print()

# 同时查询该股票的历史价格数据，看看是否有问题
sql2 = """
SELECT date, close, change_rate 
FROM cn_stock_hist 
WHERE code='301319' 
AND date >= '2026-05-20' 
AND date <= '2026-06-10'
ORDER BY date
LIMIT 15
"""

print("=" * 80)
print("唯特偶 (301319) 从 2026-05-20 开始的历史价格:")
print("=" * 80)
df2 = pd.read_sql(sql2, con=mdb.engine())
print(df2.to_string())
print()

# 计算手动验证
if len(df2) > 1:
    base_price = df2.iloc[0]['close']
    print(f"基准价格 (2026-05-20): {base_price}")
    print()
    
    for i in range(1, min(len(df2), 11)):
        future_price = df2.iloc[i]['close']
        days = i
        manual_rate = round((future_price - base_price) / base_price * 100, 2)
        print(f"第{days}天 ({df2.iloc[i]['date']}): 价格={future_price}, 手动计算收益率={manual_rate}%")
