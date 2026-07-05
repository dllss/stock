#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单测试：验证去掉未来函数后的回测逻辑
"""

import sys
import os
import pandas as pd

# 添加项目根目录到 sys.path
sys.path.append(os.path.abspath('.'))

from instock.backtest.backtest_runner import backtest_stock, Trade, check_sell_signal

# ==================== 创建测试数据 ====================

def create_test_data():
    """创建简单的测试K线数据"""
    dates = pd.date_range('2023-06-01', periods=100, freq='B')

    # 创建一个简单的价格序列（上涨趋势）
    data = []
    price = 10.0
    for i, date in enumerate(dates):
        # 模拟价格波动
        if i % 2 == 0:
            price += 0.1
        else:
            price -= 0.05

        open_price = price - 0.05
        close_price = price
        high_price = price + 0.1
        low_price = price - 0.1
        volume = 1000000 + i * 10000

        data.append({
            'date': date.strftime('%Y-%m-%d'),
            'open': round(open_price, 2),
            'close': round(close_price, 2),
            'high': round(high_price, 2),
            'low': round(low_price, 2),
            'volume': volume,
            'amount': volume * close_price
        })

    return pd.DataFrame(data)

# ==================== 简单的信号函数 ====================

def simple_buy_signal(data, i, **params):
    """简单的买入信号：收盘价 > 前一日收盘价（上涨）"""
    if i < 1:
        return False
    return data.iloc[i]['close'] > data.iloc[i-1]['close']

# ==================== 主测试函数 ====================

def test_no_lookahead():
    """测试无未来函数的回测逻辑"""
    print("=" * 60)
    print("测试：验证无未来函数逻辑")
    print("=" * 60)

    # 创建测试数据
    kline = create_test_data()
    print(f"\n✅ 创建测试数据：{len(kline)} 条K线")

    # 运行回测
    print("\n开始回测...")
    trades = backtest_stock(
        code='TEST001',
        name='测试股票',
        kline=kline,
        start_date='2023-06-01',
        end_date='2023-12-31',
        strategy_id='test_strategy',
        signal_func=simple_buy_signal,
        signal_params={},
        sell_params={
            'stop_loss': -0.05,      # 止损 -5%
            'stop_profit': 0.10,      # 止盈 +10%
            'max_hold_days': 20,      # 最大持有20天
            'trailing_stop': -0.03    # 移动止损 -3%
        }
    )

    # 显示结果
    print(f"\n回测完成，共 {len(trades)} 笔交易")
    print("=" * 60)

    if len(trades) > 0:
        print("\n前5笔交易详情：")
        for i, trade in enumerate(trades[:5]):
            print(f"\n交易 #{i+1}:")
            print(f"  股票: {trade.code}({trade.name})")
            print(f"  买入日期: {trade.buy_date}")
            print(f"  买入价格: {trade.buy_price:.2f}")
            print(f"  卖出日期: {trade.sell_date}")
            print(f"  卖出价格: {trade.sell_price:.2f}")
            print(f"  收益率: {trade.profit_pct:.2f}%")
            print(f"  持有天数: {trade.hold_days}")
            print(f"  卖出原因: {trade.sell_reason}")

        # 计算统计
        total_profit = sum(t.profit_pct for t in trades)
        win_count = sum(1 for t in trades if t.profit_pct > 0)
        loss_count = sum(1 for t in trades if t.profit_pct <= 0)

        print("\n" + "=" * 60)
        print("统计结果：")
        print("=" * 60)
        print(f"总交易次数: {len(trades)}")
        print(f"盈利次数: {win_count}")
        print(f"亏损次数: {loss_count}")
        print(f"胜率: {win_count/len(trades)*100:.1f}%")
        print(f"总收益率: {total_profit:.2f}%")
        print(f"平均收益率: {total_profit/len(trades):.2f}%")
    else:
        print("\n⚠️ 没有产生任何交易，请检查信号函数")

    print("\n" + "=" * 60)
    print("✅ 测试完成")
    print("=" * 60)

if __name__ == '__main__':
    test_no_lookahead()
