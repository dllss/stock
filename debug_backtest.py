#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试脚本：排查回测收益异常问题
"""

import os
import sys
import logging

# 添加项目路径
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import pandas as pd
import instock.lib.database as mdb
from instock.backtest.backtest_runner import (
    backtest_stock,
    SIGNAL_FUNCTIONS,
    STRATEGY_CONFIGS,
    simulate_portfolio,
    print_trades,
    print_portfolio_summary,
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)

def debug_single_stock():
    """调试单只股票"""
    print("\n" + "=" * 60)
    print("🔍 调试单只股票")
    print("=" * 60)

    # 1. 加载一只股票的K线数据
    sql = """
        SELECT `code`, `name`, `date`,
               `open_price` as `open`,
               `new_price` as `close`,
               `high_price` as `high`,
               `low_price` as `low`,
               `volume`,
               `change_rate` as `p_change`
        FROM `cn_stock_spot`
        WHERE `code` = '000001' AND `date` >= '2023-06-01' AND `date` <= '2026-06-01'
        ORDER BY `code`, `date`
    """

    print("\n📊 加载K线数据...")
    df = pd.read_sql(sql=sql, con=mdb.engine())

    if len(df) == 0:
        print("  ❌ 没有数据")
        return

    print(f"  ✅ 加载 {len(df)} 条记录")

    kline = df.sort_values('date').reset_index(drop=True)

    # 2. 运行回测
    print("\n🚀 运行回测（低ATR成长策略）...")

    trades = backtest_stock(
        code='000001',
        name='平安银行',
        kline=kline,
        start_date='2023-06-01',
        end_date='2026-06-01',
        strategy_id='low_atr',
        signal_func=SIGNAL_FUNCTIONS['low_atr'],
        signal_params={},
        sell_params={
            'stop_loss': -0.08,
            'stop_profit': 0.20,
            'trailing_stop': -0.05,
            'max_hold_days': 60
        }
    )

    print(f"\n  ✅ 生成 {len(trades)} 笔交易")

    # 3. 打印交易明细
    if trades:
        print("\n" + "=" * 60)
        print("📋 交易明细（前20笔）")
        print("=" * 60)

        for i, t in enumerate(trades[:20]):
            print(
                f"  {i+1}. 买入: {t.buy_date} {t.buy_price:.2f} -> "
                f"卖出: {t.sell_date} {t.sell_price:.2f} "
                f"收益: {t.profit_pct:+.2f}% 原因: {t.sell_reason}"
            )

        # 4. 模拟资金账户
        print("\n" + "=" * 60)
        print("💰 模拟资金账户")
        print("=" * 60)

        kline_dict = {'000001': kline}
        portfolio_result = simulate_portfolio(
            trades,
            kline_dict,
            initial_cash=1000000.0,
            max_positions=10,
            start_date='2023-06-01',
            end_date='2026-06-01',
        )

        print_portfolio_summary(portfolio_result)

        # 5. 分析问题
        print("\n" + "=" * 60)
        print("🔍 问题分析")
        print("=" * 60)

        if portfolio_result['final_value'] < 1000000:
            print(f"  ⚠️ 亏损！最终资产: {portfolio_result['final_value']:.2f} 元")

            # 检查是否有重复买入
            buy_dates = [t.buy_date for t in trades]
            unique_buy_dates = set(buy_dates)
            if len(buy_dates) != len(unique_buy_dates):
                print(f"  ⚠️ 发现重复买入日期！")

            # 检查卖出价格是否合理
            for t in trades[:10]:
                if t.sell_price == t.buy_price:
                    print(f"  ⚠️ 卖出价=买入价: {t.buy_date} {t.buy_price:.2f}")
                if t.sell_price == 0:
                    print(f"  ⚠️ 卖出价=0: {t.buy_date}")

        else:
            print(f"  ✅ 盈利！最终资产: {portfolio_result['final_value']:.2f} 元")
    else:
        print("\n  ⚠️ 没有交易记录")


def debug_multiple_stocks():
    """调试多只股票（小规模）"""
    print("\n" + "=" * 60)
    print("🔍 调试多只股票（小规模测试）")
    print("=" * 60)

    # 加载5只股票
    sql = """
        SELECT `code`, `name`, `date`,
               `open_price` as `open`,
               `new_price` as `close`,
               `high_price` as `high`,
               `low_price` as `low`,
               `volume`,
               `change_rate` as `p_change`
        FROM `cn_stock_spot`
        WHERE `code` IN ('000001', '000002', '600000', '600036', '601318')
          AND `date` >= '2023-06-01' AND `date` <= '2026-06-01'
        ORDER BY `code`, `date`
    """

    print("\n📊 加载K线数据...")
    df = pd.read_sql(sql=sql, con=mdb.engine())

    if len(df) == 0:
        print("  ❌ 没有数据")
        return

    print(f"  ✅ 加载 {len(df)} 条记录")

    # 按code分组
    kline_dict = {}
    for code, group in df.groupby('code'):
        kline_dict[code] = group.sort_values('date').reset_index(drop=True)

    print(f"  ✅ 共 {len(kline_dict)} 只股票")

    # 运行回测
    print("\n🚀 运行回测（低ATR成长策略）...")

    all_trades = []
    for code, kline in kline_dict.items():
        trades = backtest_stock(
            code=code,
            name=kline.iloc[0]['name'] if 'name' in kline.columns else code,
            kline=kline,
            start_date='2023-06-01',
            end_date='2026-06-01',
            strategy_id='low_atr',
            signal_func=SIGNAL_FUNCTIONS['low_atr'],
            signal_params={},
            sell_params={
                'stop_loss': -0.08,
                'stop_profit': 0.20,
                'trailing_stop': -0.05,
                'max_hold_days': 60
            }
        )
        all_trades.extend(trades)

    print(f"\n  ✅ 共生成 {len(all_trades)} 笔交易")

    # 模拟资金账户
    print("\n" + "=" * 60)
    print("💰 模拟资金账户")
    print("=" * 60)

    portfolio_result = simulate_portfolio(
        all_trades,
        kline_dict,
        initial_cash=1000000.0,
        max_positions=10,
        start_date='2023-06-01',
        end_date='2026-06-01',
    )

    print_portfolio_summary(portfolio_result)

    # 打印交易详情
    print_trades(all_trades, top_n=10)


if __name__ == '__main__':
    print("=" * 60)
    print("🔍 回测调试脚本")
    print("=" * 60)

    # 调试单只股票
    debug_single_stock()

    print("\n" + "=" * 60)

    # 调试多只股票
    debug_multiple_stocks()

    print("\n" + "=" * 60)
    print("✅ 调试完成")
    print("=" * 60)
