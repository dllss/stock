#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""检查唯特偶股票的价格数据"""
import sys
sys.path.insert(0, 'd:/WorkProject/stock')

import instock.lib.database as mdb
import pandas as pd

# 查询cn_stock_spot表中的唯特偶数据
sql = """
SELECT date, code, name, open_price, new_price, high_price, low_price, change_rate
FROM cn_stock_spot 
WHERE code='301319' 
AND date >= '2026-05-15' 
AND date <= '2026-06-10'
ORDER BY date
"""

print("=" * 80)
print("唯特偶 (301319) 在 cn_stock_spot 表中的价格数据:")
print("=" * 80)

df = pd.read_sql(sql, con=mdb.engine())
print(df.to_string())
print()

if len(df) > 1:
    base_date = df.iloc[0]['date']
    base_price = df.iloc[0]['new_price']
    print(f"基准日期: {base_date}, 基准价格: {base_price}")
    print()
    
    for i in range(1, min(len(df), 11)):
        future_date = df.iloc[i]['date']
        future_price = df.iloc[i]['new_price']
        days = i
        manual_rate = round((future_price - base_price) / base_price * 100, 2)
        print(f"第{days}天 ({future_date}): 价格={future_price}, 手动计算收益率={manual_rate}%")
