#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试回测代码（无未来函数版本）
从数据库表 cn_stock_spot 读取真实K线数据
"""

import sys
import os
import pandas as pd

# 添加项目根目录到 sys.path
cpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '.'))
sys.path.append(cpath)

import instock.lib.database as mdb
from instock.backtest.backtest_runner import run_backtest, summarize


def test_with_real_data():
    """使用数据库中的真实数据测试回测逻辑"""
    print("=" * 60)
    print("测试回测代码（无未来函数版本）")
    print("=" * 60)

    # 连接数据库
    try:
        engine = mdb.engine()
        if engine is None:
            print("❌ 数据库连接失败")
            return
        print("✅ 数据库连接成功")
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return

    # 从 cn_stock_spot 表读取K线数据
    # 选择2只测试股票
    test_codes = ['000001', '000002']

    sql = f"""
    SELECT `code`, `name`, `date`,
           `open_price` as `open`,
           `new_price` as `close`,
           `high_price` as `high`,
           `low_price` as `low`,
           `volume`,
           `change_rate` as `p_change`
    FROM `cn_stock_spot`
    WHERE `code` IN ({','.join([f"'{c}'" for c in test_codes])})
      AND `date` >= '2023-01-01'
    ORDER BY `code`, `date`
    """

    print("\n从数据库读取K线数据...")
    try:
        df = pd.read_sql(sql=sql, con=engine)
        if len(df) == 0:
            print("❌ 未找到K线数据，请先运行数据抓取任务")
            return
        print(f"✅ 读取到 {len(df)} 条K线记录")
    except Exception as e:
        print(f"❌ 读取数据失败: {e}")
        return

    # 按股票代码分组
    kline_dict = {}
    for code, group in df.groupby('code'):
        kline_dict[code] = group.sort_values('date').reset_index(drop=True)
        name = group['name'].iloc[0] if 'name' in group.columns else code
        print(f"  ✅ {code}({name}): {len(group)} 条K线数据")

    # 运行回测
    print("\n" + "=" * 60)
    print("开始回测（无未来函数版本）...")
    print("=" * 60)
    print("策略: breakthrough_volume")
    print("回测区间: 2023-06-01 ~ 2024-01-01")
    print("=" * 60)

    trades = run_backtest(
        kline_dict=kline_dict,
        start_date='2023-06-01',
        end_date='2024-01-01',
        strategy_id='breakthrough_volume'
    )

    # 显示结果
    print("\n" + "=" * 60)
    print("回测结果：")
    print("=" * 60)
    summarize(trades, 'breakthrough_volume')


if __name__ == '__main__':
    test_with_real_data()
