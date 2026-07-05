#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测参数优化模块

功能：对策略参数和卖出参数进行网格搜索，
      找出收益率最高的参数组合。
"""

import itertools
import logging
from typing import Dict, List, Any

import pandas as pd

from instock.backtest.backtest_runner import (
    run_backtest,
    simulate_portfolio,
    STRATEGY_CONFIGS,
)


# 默认卖出参数网格
DEFAULT_SELL_GRID = {
    'stop_loss': [-0.05, -0.08, -0.10],
    'stop_profit': [0.15, 0.20, 0.25, 0.30],
    'trailing_stop': [-0.03, -0.05, -0.08],
    'max_hold_days': [30, 60, 90, 120],
}

# 策略参数网格（可选）
STRATEGY_PARAM_GRIDS = {
    'breakthrough_volume': {
        'break_days': [10, 15, 20, 25],
        'vol_ratio': [1.2, 1.5, 2.0],
        'ma_days': [30, 60, 120],
    },
    'turtle_trade': {
        'break_days': [20, 30, 60, 90],
    },
    'keep_increasing': {
        'ma_days': [10, 20, 30, 60],
        'min_increase': [0.10, 0.15, 0.20],
    },
}


def run_optimization(
    kline_dict: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    strategy_id: str = 'breakthrough_volume',
    sell_param_grid: Dict[str, List[float]] = None,
    strategy_param_grid: Dict[str, List[Any]] = None,
    initial_cash: float = 1000000.0,
    max_positions: int = 10,
    top_n: int = 20,
) -> List[Dict[str, Any]]:
    """
    运行参数优化扫描

    参数:
        kline_dict: {code: DataFrame}
        start_date/end_date: 回测区间
        strategy_id: 策略ID
        sell_param_grid: 卖出参数网格（None则用默认）
        strategy_param_grid: 策略参数网格（None则不优化策略参数）
        initial_cash: 初始资金
        max_positions: 最大持仓数
        top_n: 返回TOP N个结果

    返回:
        [{'rank': 1, 'total_return_pct': ..., 'sell_params': {...}, ...}, ...]
    """
    if sell_param_grid is None:
        sell_param_grid = DEFAULT_SELL_GRID.copy()

    # 生成卖出参数组合
    sell_keys = list(sell_param_grid.keys())
    sell_values = list(sell_param_grid.values())
    sell_combos = list(itertools.product(*sell_values))

    # 生成策略参数组合
    if strategy_param_grid:
        strategy_keys = list(strategy_param_grid.keys())
        strategy_values = list(strategy_param_grid.values())
        strategy_combos = list(itertools.product(*strategy_values))
    else:
        strategy_combos = [()]
        strategy_keys = []

    total = len(sell_combos) * max(len(strategy_combos), 1)
    results = []
    current = 0

    logging.info(f"\n{'=' * 70}")
    logging.info(f"🔍 参数优化扫描: {STRATEGY_CONFIGS[strategy_id]['name']}")
    logging.info(f"📊 总组合数: {total}")
    logging.info(f"📅 回测区间: {start_date} ~ {end_date}")
    logging.info(f"{'=' * 70}")

    # 保存原始策略参数
    original_params = STRATEGY_CONFIGS[strategy_id]['params'].copy()

    try:
        for s_combo in sell_combos:
            sell_params = dict(zip(sell_keys, s_combo))

            for st_combo in strategy_combos:
                current += 1

                # 设置策略参数
                if strategy_param_grid:
                    STRATEGY_CONFIGS[strategy_id]['params'] = dict(zip(strategy_keys, st_combo))

                # 进度日志
                if current % 10 == 0 or current == 1:
                    logging.info(f"  进度: {current}/{total} ...")

                try:
                    trades = run_backtest(
                        kline_dict, start_date, end_date, strategy_id, sell_params
                    )
                    portfolio_result = simulate_portfolio(
                        trades, kline_dict,
                        initial_cash=initial_cash,
                        max_positions=max_positions,
                        start_date=start_date,
                        end_date=end_date,
                    )

                    results.append({
                        'sell_params': sell_params.copy(),
                        'strategy_params': dict(zip(strategy_keys, st_combo)) if strategy_param_grid else {},
                        'total_return_pct': portfolio_result['total_return_pct'],
                        'total_profit_amount': portfolio_result['total_profit_amount'],
                        'n_trades': portfolio_result['n_trades'],
                        'win_rate': portfolio_result['win_rate'],
                        'final_value': portfolio_result['final_value'],
                    })
                except Exception as e:
                    logging.debug(f"  参数组合异常: {e}")

                # 恢复原策略参数
                STRATEGY_CONFIGS[strategy_id]['params'] = original_params.copy()

    finally:
        # 确保恢复原策略参数
        STRATEGY_CONFIGS[strategy_id]['params'] = original_params

    # 按收益率降序排序
    results.sort(key=lambda x: x['total_return_pct'], reverse=True)

    # 添加排名
    for i, r in enumerate(results[:top_n], 1):
        r['rank'] = i

    logging.info(f"\n{'=' * 70}")
    logging.info(f"✅ 参数优化完成，共 {len(results)} 组有效结果")
    logging.info(f"{'=' * 70}")

    return results[:top_n]


def print_optimization_results(results: List[Dict[str, Any]]):
    """打印参数优化结果"""
    if not results:
        logging.info("  无有效结果")
        return

    lines = [
        f"\n{'=' * 130}",
        f"  🏆 参数优化结果 TOP {len(results)}",
        f"{'=' * 130}",
        f"  {'排名':<4s} {'收益率':>8s} {'收益额':>12s} {'交易数':>6s} {'胜率':>7s} {'终值':>12s} {'卖出参数':<55s} {'策略参数':<30s}",
        f"  {'-' * 128}",
    ]

    for r in results:
        sell_str = ' '.join(f"{k}={v}" for k, v in r['sell_params'].items())
        strat_str = ' '.join(f"{k}={v}" for k, v in r['strategy_params'].items()) if r['strategy_params'] else ""
        lines.append(
            f"  {r['rank']:<4d} {r['total_return_pct']:>7.2f}% "
            f"{r['total_profit_amount']:>11,.0f}元 "
            f"{r['n_trades']:>5d} {r['win_rate']:>6.1f}% "
            f"{r['final_value']:>11,.0f}元 "
            f"{sell_str:<55s} {strat_str:<30s}"
        )

    lines.append(f"{'=' * 130}")
    for line in lines:
        logging.info(line)


def find_best_params(
    kline_dict: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    strategy_id: str = 'breakthrough_volume',
    target_return_pct: float = 20.0,
) -> Dict[str, Any]:
    """
    寻找能达到目标收益率的参数组合

    返回第一个达到目标收益率的参数组合，如果没有则返回最佳组合
    """
    results = run_optimization(
        kline_dict, start_date, end_date, strategy_id,
        sell_param_grid=DEFAULT_SELL_GRID.copy(),
        top_n=50,
    )

    # 查找达到目标收益率的组合
    for r in results:
        if r['total_return_pct'] >= target_return_pct:
            logging.info(
                f"\n🎯 找到达到目标收益率({target_return_pct}%)的参数组合: "
                f"收益率={r['total_return_pct']:.2f}%, 排名={r['rank']}"
            )
            return r

    # 没找到则返回最佳组合
    if results:
        logging.info(
            f"\n⚠️ 未找到达到目标收益率({target_return_pct}%)的参数组合，返回最佳组合: "
            f"收益率={results[0]['total_return_pct']:.2f}%"
        )
        return results[0]

    return {}
