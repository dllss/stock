#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
涨停原因数据修复脚本
=====================

功能说明：
本脚本用于补充 cn_stock_limitup_reason 表中缺失的行情数据。
当同花顺API返回的涨停原因数据缺少最新价、涨跌幅等字段时，
从 cn_stock_spot 表中获取对应的行情数据进行补充。

使用场景：
- 同花顺API对某些历史日期不提供完整行情数据
- 需要批量修复缺失的涨停股票行情信息

使用示例：
    # 检查并预览
    python instock/job/fix_cn_stock_limitup_reason_data.py --date 2026-05-15
    
    # 导出SQL文件（推荐）
    python instock/job/fix_cn_stock_limitup_reason_data.py --date 2026-05-15 --export
    
    # 直接执行更新
    python instock/job/fix_cn_stock_limitup_reason_data.py --date 2026-05-15 --execute
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import instock.lib.database as mdb
import pandas as pd
import argparse

def check_missing_data(target_date):
    """检查指定日期缺失的数据"""
    print("="*60)
    print(f"检查 {target_date} 涨停原因数据")
    print("="*60)
    
    # 查询总数
    total = mdb.executeSqlFetch(f"""
        SELECT COUNT(*) FROM cn_stock_limitup_reason 
        WHERE date='{target_date}'
    """)
    
    # 查询空值数量
    null_count = mdb.executeSqlFetch(f"""
        SELECT COUNT(*) FROM cn_stock_limitup_reason 
        WHERE date='{target_date}' AND (new_price IS NULL OR new_price = 0)
    """)
    
    print(f"总记录数: {total[0][0]}")
    print(f"最新价为空: {null_count[0][0]}")
    print(f"有数据的记录: {total[0][0] - null_count[0][0]}")
    
    if null_count[0][0] > 0:
        print("\n⚠️  发现缺失数据，需要补充")
        return True
    else:
        print("\n✅ 数据完整，无需补充")
        return False

def get_spot_data_for_date(target_date):
    """从 cn_stock_spot 表获取指定日期的行情数据（只获取涨停股票）"""
    print(f"\n从 cn_stock_spot 表获取 {target_date} 涨停股票的行情数据...")
    
    # 先获取涨停股票的代码列表
    limitup_codes = mdb.executeSqlFetch(f"""
        SELECT DISTINCT code FROM cn_stock_limitup_reason 
        WHERE date='{target_date}' AND (new_price IS NULL OR new_price = 0)
    """)
    
    if limitup_codes is None or len(limitup_codes) == 0:
        print("❌ 没有需要更新的涨停股票")
        return None
    
    # 提取代码列表
    codes = [row[0] for row in limitup_codes]
    codes_str = ','.join([f"'{code}'" for code in codes])
    
    print(f"找到 {len(codes)} 只需要更新的涨停股票")
    
    # 从 cn_stock_spot 获取这些股票的行情数据
    df = mdb.executeSqlFetch(f"""
        SELECT code, new_price, change_rate, ups_downs, volume, deal_amount, turnoverrate
        FROM cn_stock_spot
        WHERE date='{target_date}' AND code IN ({codes_str})
    """)
    
    if df is None or len(df) == 0:
        print("❌ cn_stock_spot 表中没有找到这些股票的行情数据")
        return None
    
    df = pd.DataFrame(df, columns=['code', 'new_price', 'change_rate', 'ups_downs', 'volume', 'deal_amount', 'turnoverrate'])
    print(f"✅ 获取到 {len(df)} 条行情数据")
    return df

def generate_update_sql(spot_df, target_date):
    """生成UPDATE SQL语句"""
    print("\n生成UPDATE SQL...")
    
    updates = []
    for _, row in spot_df.iterrows():
        code = row['code']
        new_price = row['new_price']
        change_rate = row['change_rate']
        ups_downs = row['ups_downs']
        volume = row['volume']
        deal_amount = row['deal_amount']
        turnoverrate = row['turnoverrate']
        
        sql = f"""UPDATE cn_stock_limitup_reason 
SET new_price={new_price}, 
    change_rate={change_rate}, 
    ups_downs={ups_downs}, 
    volume={volume}, 
    deal_amount={deal_amount}, 
    turnoverrate={turnoverrate}
WHERE date='{target_date}' AND code='{code}' AND (new_price IS NULL OR new_price = 0);"""
        
        updates.append(sql)
    
    print(f"✅ 生成 {len(updates)} 条UPDATE语句")
    return updates

def preview_updates(updates, count=5):
    """预览前几条UPDATE语句"""
    print("\n" + "="*60)
    print(f"预览前 {count} 条UPDATE语句:")
    print("="*60)
    for i, sql in enumerate(updates[:count], 1):
        print(f"\n{i}. {sql}")

def execute_updates(updates):
    """执行UPDATE语句"""
    print("\n" + "="*60)
    print("开始执行UPDATE...")
    print("="*60)
    
    success_count = 0
    fail_count = 0
    
    for i, sql in enumerate(updates, 1):
        try:
            mdb.executeSql(sql)
            success_count += 1
            
            # 每10条显示一次进度
            if i % 10 == 0 or i == len(updates):
                print(f"进度: {i}/{len(updates)} (成功:{success_count}, 失败:{fail_count})")
        except Exception as e:
            fail_count += 1
            print(f"❌ 第{i}条失败: {str(e)[:100]}")
    
    print("\n" + "="*60)
    print(f"执行完成！成功:{success_count}, 失败:{fail_count}")
    print("="*60)
    
    return success_count, fail_count

def export_sql_file(updates, target_date):
    """导出SQL文件"""
    # 生成文件名：update_limitup_YYYYMMDD.sql
    date_str = target_date.replace('-', '')
    filename = f'update_limitup_{date_str}.sql'
    filepath = os.path.join(os.path.dirname(__file__), filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"-- 补充{target_date}涨停原因缺失的行情数据\n")
        f.write(f"-- 生成时间: {pd.Timestamp.now()}\n")
        f.write(f"-- 总记录数: {len(updates)}\n\n")
        f.write("BEGIN;\n\n")
        
        for sql in updates:
            f.write(sql + "\n\n")
        
        f.write("COMMIT;\n")
    
    print(f"✅ SQL文件已导出: {filepath}")
    return filepath

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='补充涨停原因缺失数据')
    parser.add_argument('--date', type=str, required=True, help='目标日期，格式：YYYY-MM-DD')
    parser.add_argument('--execute', action='store_true', help='直接执行UPDATE')
    parser.add_argument('--export', action='store_true', help='导出SQL文件')
    args = parser.parse_args()
    
    target_date = args.date
    
    has_missing = check_missing_data(target_date)
    
    if not has_missing:
        sys.exit(0)
    
    # 获取行情数据
    spot_df = get_spot_data_for_date(target_date)
    
    if spot_df is None:
        print(f"\n❌ 无法获取行情数据，请先运行 basic_data_daily_job.py 抓取{target_date}的基础数据")
        sys.exit(1)
    
    # 生成UPDATE语句
    updates = generate_update_sql(spot_df, target_date)
    
    # 预览
    preview_updates(updates, count=3)
    
    print("\n" + "="*60)
    print("检查完成！")
    print("="*60)
    
    # 根据参数执行操作
    if args.execute:
        print("\n⚠️  即将执行数据库更新操作，请确认...")
        confirm = input("确认执行？(yes/no): ")
        
        if confirm.lower() in ['yes', 'y']:
            success, fail = execute_updates(updates)
            
            if fail == 0:
                print("\n✅ 所有更新执行成功！")
            else:
                print(f"\n⚠️  有 {fail} 条更新失败，请检查")
        else:
            print("\n❌ 已取消执行")
    
    elif args.export:
        filepath = export_sql_file(updates, target_date)
        print(f"\n✅ 请在数据库工具中执行: {filepath}")
    
    else:
        print("\n下一步操作：")
        print("1. 检查上面的UPDATE语句是否正确")
        print("2. 如果正确，运行以下命令之一：")
        print(f"   - python instock/job/fix_cn_stock_limitup_reason_data.py --date {target_date} --execute  (直接执行)")
        print(f"   - python instock/job/fix_cn_stock_limitup_reason_data.py --date {target_date} --export   (导出SQL文件)")
