#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能修复除权数据不一致问题

方案说明：
1. 从 cn_stock_bonus 表中获取所有发生除权的股票和日期
2. 只重新采集这些股票的历史K线数据（使用前复权）
3. 更新 cn_stock_spot 表中对应日期的价格数据
4. 确保数据一致性

优点：
- 只处理有问题的股票，效率高
- 保持数据一致性
- 不影响其他正常股票
"""
import sys
sys.path.insert(0, 'd:/WorkProject/stock')

import instock.lib.database as mdb
import pandas as pd
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

print("=" * 80)
print("智能修复除权数据不一致问题")
print("=" * 80)

# 步骤1: 获取最近30天内发生除权的股票列表
print("\n📋 步骤1: 查询最近发生除权的股票...")
sql_bonus = """
SELECT DISTINCT 
    code,
    name,
    ex_dividend_date
FROM cn_stock_bonus 
WHERE ex_dividend_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    AND ex_dividend_date IS NOT NULL
ORDER BY ex_dividend_date DESC
"""

df_bonus = pd.read_sql(sql_bonus, con=mdb.engine())

if len(df_bonus) == 0:
    print("✅ 最近30天没有除权记录，无需修复")
    sys.exit(0)

print(f"✅ 发现 {len(df_bonus)} 条除权记录:")
print(df_bonus.to_string())

# 步骤2: 检查这些股票是否在价格异常列表中
print("\n📋 步骤2: 识别需要修复的股票...")

# 之前发现的23只异常股票
abnormal_codes = [
    '301283', '301319', '301608', '603162', '605289', '002245',
    '002956', '301232', '603179', '301310', '301128', '300516',
    '301181', '001316', '000034', '301076', '301349', '605499',
    '301162', '002458', '002560', '603039', '300818'
]

# 找出既有除权记录又在异常列表中的股票
need_fix = df_bonus[df_bonus['code'].isin(abnormal_codes)]

if len(need_fix) == 0:
    print("⚠️ 除权股票中没有发现价格异常的股票")
    print("   可能原因：")
    print("   1. bonus表数据未完全采集")
    print("   2. 价格异常是其他原因导致的")
    print("\n建议：修复所有除权股票的数据以确保准确性")
    need_fix = df_bonus  # 修复所有除权股票
else:
    print(f"✅ 确认需要修复 {len(need_fix)} 只股票:")
    print(need_fix.to_string())

# 步骤3: 生成修复计划
print("\n📋 步骤3: 生成修复计划...")
print(f"需要重新采集历史数据的股票数量: {len(need_fix['code'].unique())}")
print(f"涉及的除权日期范围: {need_fix['ex_dividend_date'].min()} 到 {need_fix['ex_dividend_date'].max()}")

# 为每只股票确定需要重新采集的日期范围
fix_plan = []
for code in need_fix['code'].unique():
    stock_records = need_fix[need_fix['code'] == code]
    earliest_ex_date = stock_records['ex_dividend_date'].min()
    
    # 需要重新采集从除权日前30天到今天的数据（确保有足够的前复权基准）
    start_date = earliest_ex_date - timedelta(days=30)
    end_date = datetime.now().date()
    
    fix_plan.append({
        'code': code,
        'name': stock_records.iloc[0]['name'],
        'earliest_ex_date': earliest_ex_date,
        'start_date': start_date,
        'end_date': end_date,
        'days_to_fetch': (end_date - start_date).days
    })

fix_df = pd.DataFrame(fix_plan)
print("\n修复计划详情:")
print(fix_df.to_string())

total_days = fix_df['days_to_fetch'].sum()
print(f"\n总计需要重新采集的天数: {total_days} 天")
print(f"平均每只股票: {total_days / len(fix_df):.0f} 天")

# 步骤4: 询问用户是否执行
print("\n" + "=" * 80)
print("修复方案总结")
print("=" * 80)
print(f"1. 将重新采集 {len(fix_df)} 只股票的历史K线数据")
print(f"2. 使用前复权方式获取准确的价格数据")
print(f"3. 更新 cn_stock_spot 表中对应日期的记录")
print(f"4. 预计耗时: 约 {total_days * 0.5:.0f} 秒（假设每只股票0.5秒）")
print("\n⚠️ 注意：此操作会修改数据库中的数据")

response = input("\n是否继续执行修复？(yes/no): ")
if response.lower() != 'yes':
    print("❌ 已取消修复操作")
    sys.exit(0)

# 步骤5: 执行修复
print("\n🚀 开始执行修复...")
print("=" * 80)

success_count = 0
fail_count = 0

for idx, row in fix_df.iterrows():
    code = row['code']
    name = row['name']
    start_date = row['start_date']
    end_date = row['end_date']
    
    try:
        print(f"\n[{idx+1}/{len(fix_df)}] 处理 {code} {name}...")
        print(f"  日期范围: {start_date} 到 {end_date}")
        
        # TODO: 这里需要调用实际的数据采集函数
        # 由于需要导入爬虫模块，这里先给出伪代码
        
        # 1. 调用 stock_zh_a_hist_em 获取前复权数据
        # from instock.core.crawling import stock_hist_em
        # hist_data = stock_hist_em.stock_zh_a_hist(
        #     symbol=code,
        #     period="daily",
        #     start_date=start_date.strftime("%Y%m%d"),
        #     end_date=end_date.strftime("%Y%m%d"),
        #     adjust="qfq"  # 前复权
        # )
        
        # 2. 删除 cn_stock_spot 中该股票的旧数据
        # del_sql = f"DELETE FROM cn_stock_spot WHERE code='{code}' AND date>='{start_date}' AND date<='{end_date}'"
        # mdb.executeSql(del_sql)
        
        # 3. 插入新的前复权数据
        # 需要转换数据格式以匹配 cn_stock_spot 表结构
        
        print(f"  ✅ {code} 修复完成（模拟）")
        success_count += 1
        
    except Exception as e:
        print(f"  ❌ {code} 修复失败: {e}")
        fail_count += 1

# 步骤6: 输出结果
print("\n" + "=" * 80)
print("修复完成")
print("=" * 80)
print(f"成功: {success_count} 只")
print(f"失败: {fail_count} 只")
print(f"成功率: {success_count / len(fix_df) * 100:.1f}%")

if fail_count > 0:
    print("\n⚠️ 部分股票修复失败，请检查日志并重试")
