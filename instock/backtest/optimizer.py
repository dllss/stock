#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测参数优化模块

功能：对策略参数和卖出参数进行网格搜索，
      找出收益率最高的参数组合。
"""

import itertools
import logging
import random
from typing import Dict, List, Any, Optional

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
    'trailing_stop_activation': [0.03, 0.06, 0.10, 0.15],
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


def _sample_kline_dict(
    kline_dict: Dict[str, pd.DataFrame],
    sample_size: Optional[int],
    seed: int = 42,
) -> Dict[str, pd.DataFrame]:
    """
    从全市场股票中随机抽样，加速参数优化

    参数:
        kline_dict: 完整股票字典
        sample_size: 抽样数量，None或0表示不抽样
        seed: 随机种子
    """
    if not sample_size or sample_size >= len(kline_dict):
        return kline_dict
    random.seed(seed)
    sampled_codes = random.sample(list(kline_dict.keys()), sample_size)
    logging.info(f"  🎲 随机抽样: {len(kline_dict)} → {sample_size} 只股票")
    return {code: kline_dict[code] for code in sampled_codes}


def _run_single_combo(
    kline_dict: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    strategy_id: str,
    sell_params: dict,
    strategy_params: dict,
    initial_cash: float,
    max_positions: int,
) -> Optional[Dict[str, Any]]:
    """执行单组参数回测，返回结果字典"""
    # 临时覆盖策略参数
    original = STRATEGY_CONFIGS[strategy_id]['params']
    if strategy_params:
        STRATEGY_CONFIGS[strategy_id]['params'] = strategy_params
    try:
        trades = run_backtest(
            kline_dict, start_date, end_date, strategy_id, sell_params
        )
        pr = simulate_portfolio(
            trades, kline_dict,
            initial_cash=initial_cash,
            max_positions=max_positions,
            start_date=start_date,
            end_date=end_date,
        )
        return {
            'sell_params': sell_params,
            'strategy_params': strategy_params,
            'total_return_pct': pr['total_return_pct'],
            'total_profit_amount': pr['total_profit_amount'],
            'n_trades': pr['n_trades'],
            'win_rate': pr['win_rate'],
            'final_value': pr['final_value'],
            'max_drawdown_pct': pr['max_drawdown_pct'],
        }
    except Exception as e:
        logging.warning(f"  参数组合异常: {e}", exc_info=True)
        return None
    finally:
        STRATEGY_CONFIGS[strategy_id]['params'] = original


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
    sample_size: Optional[int] = None,
    n_random: Optional[int] = None,
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
        sample_size: 抽样股票数量（None/0=不抽样，200-500可大幅加速）
        n_random: 随机搜索组合数（替代全量网格搜索，None=全量，建议200-500）

    返回:
        [{'rank': 1, 'total_return_pct': ..., 'sell_params': {...}, ...}, ...]
    """
    if sell_param_grid is None:
        sell_param_grid = DEFAULT_SELL_GRID.copy()

    # 可选：抽样股票
    kline_dict = _sample_kline_dict(kline_dict, sample_size)

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

    # ========== 随机搜索模式 ==========
    full_total = len(sell_combos) * max(len(strategy_combos), 1)
    if n_random and n_random < full_total:
        # 从全量组合中随机采样
        all_combos = list(itertools.product(sell_combos, strategy_combos))
        random.seed(42)
        sampled_combos = random.sample(all_combos, n_random)
        total = len(sampled_combos)
        logging.info(f"\n{'=' * 70}")
        logging.info(f"🎲 随机搜索模式: {STRATEGY_CONFIGS[strategy_id]['name']}")
        logging.info(f"📊 搜索组合数: {total} (全量 {full_total} 中随机采样)")
        logging.info(f"📅 回测区间: {start_date} ~ {end_date}")
        logging.info(f"{'=' * 70}")
    else:
        total = full_total
        logging.info(f"\n{'=' * 70}")
        logging.info(f"🔍 网格搜索模式: {STRATEGY_CONFIGS[strategy_id]['name']}")
        logging.info(f"📊 总组合数: {total}")
        logging.info(f"📅 回测区间: {start_date} ~ {end_date}")
        logging.info(f"{'=' * 70}")

    results = []
    current = 0

    original_params = STRATEGY_CONFIGS[strategy_id]['params'].copy()

    try:
        if n_random and n_random < full_total:
            # 随机搜索：从采样组合中迭代
            for s_combo, st_combo in sampled_combos:
                current += 1
                sell_params = dict(zip(sell_keys, s_combo))
                strat_params = dict(zip(strategy_keys, st_combo)) if strategy_param_grid else {}

                if current % 10 == 0 or current == 1:
                    logging.info(f"  进度: {current}/{total} ...")

                r = _run_single_combo(
                    kline_dict, start_date, end_date, strategy_id,
                    sell_params, strat_params,
                    initial_cash, max_positions,
                )
                if r:
                    results.append(r)
        else:
            # 全量网格搜索
            for s_combo in sell_combos:
                sell_params = dict(zip(sell_keys, s_combo))
                for st_combo in strategy_combos:
                    current += 1
                    strat_params = dict(zip(strategy_keys, st_combo)) if strategy_param_grid else {}

                    if current % 10 == 0 or current == 1:
                        logging.info(f"  进度: {current}/{total} ...")

                    r = _run_single_combo(
                        kline_dict, start_date, end_date, strategy_id,
                        sell_params, strat_params,
                        initial_cash, max_positions,
                    )
                    if r:
                        results.append(r)

    finally:
        STRATEGY_CONFIGS[strategy_id]['params'] = original_params

    # 按收益率降序排序
    results.sort(key=lambda x: x['total_return_pct'], reverse=True)

    for i, r in enumerate(results[:top_n], 1):
        r['rank'] = i

    logging.info(f"\n{'=' * 70}")
    logging.info(f"✅ 参数优化完成，共 {len(results)} 组有效结果")
    logging.info(f"{'=' * 70}")

    return results[:top_n]


def run_two_phase_optimization(
    kline_dict: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    strategy_id: str = 'breakthrough_volume',
    sell_param_grid: Dict[str, List[float]] = None,
    strategy_param_grid: Dict[str, List[Any]] = None,
    initial_cash: float = 1000000.0,
    max_positions: int = 10,
    top_n: int = 20,
    sample_size: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    两阶段参数优化：先定卖出参数，再定策略参数

    核心思想：策略参数和卖出参数之间存在一定的独立性，
    分开优化可将 N×M 变为 N+M。

    阶段1: 固定默认策略参数，扫描卖出参数网格
    阶段2: 固定最优卖出参数，扫描策略参数网格

    参数同 run_optimization()
    """
    if sell_param_grid is None:
        sell_param_grid = DEFAULT_SELL_GRID.copy()

    kline_dict = _sample_kline_dict(kline_dict, sample_size)

    strategy_name = STRATEGY_CONFIGS[strategy_id]['name']
    original_params = STRATEGY_CONFIGS[strategy_id]['params'].copy()

    # ---- 阶段1：优化卖出参数（固定默认策略参数）----
    sell_keys = list(sell_param_grid.keys())
    sell_values = list(sell_param_grid.values())
    sell_combos = list(itertools.product(*sell_values))
    n_sell = len(sell_combos)

    logging.info(f"\n{'=' * 70}")
    logging.info(f"⚡ 两阶段优化: {strategy_name}")
    logging.info(f"📊 阶段1: 扫描卖出参数 ({n_sell} 组，固定默认策略参数)")
    if sample_size:
        logging.info(f"📊 抽样股票: {sample_size} 只")
    logging.info(f"📅 回测区间: {start_date} ~ {end_date}")
    logging.info(f"{'=' * 70}")

    phase1_results = []
    for i, s_combo in enumerate(sell_combos, 1):
        sell_params = dict(zip(sell_keys, s_combo))
        if i % 10 == 0 or i == 1:
            logging.info(f"  阶段1 进度: {i}/{n_sell} ...")

        r = _run_single_combo(
            kline_dict, start_date, end_date, strategy_id,
            sell_params, {},
            initial_cash, max_positions,
        )
        if r:
            phase1_results.append(r)

    if not phase1_results:
        logging.warning("阶段1无有效结果")
        STRATEGY_CONFIGS[strategy_id]['params'] = original_params
        return []

    phase1_results.sort(key=lambda x: x['total_return_pct'], reverse=True)
    best_sell = phase1_results[0]['sell_params']
    logging.info(
        f"\n  ✅ 阶段1完成: 最优卖出参数 {best_sell}, "
        f"收益率: {phase1_results[0]['total_return_pct']:.2f}%"
    )

    # ---- 阶段2：优化策略参数（固定最优卖出参数）----
    if strategy_param_grid:
        strategy_keys = list(strategy_param_grid.keys())
        strategy_values = list(strategy_param_grid.values())
        strategy_combos = list(itertools.product(*strategy_values))
        n_strategy = len(strategy_combos)

        logging.info(f"\n  📊 阶段2: 扫描策略参数 ({n_strategy} 组，固定最优卖出参数)")

        phase2_results = []
        for i, st_combo in enumerate(strategy_combos, 1):
            strat_params = dict(zip(strategy_keys, st_combo))
            if i % 10 == 0 or i == 1:
                logging.info(f"  阶段2 进度: {i}/{n_strategy} ...")

            r = _run_single_combo(
                kline_dict, start_date, end_date, strategy_id,
                best_sell, strat_params,
                initial_cash, max_positions,
            )
            if r:
                phase2_results.append(r)

        if phase2_results:
            phase2_results.sort(key=lambda x: x['total_return_pct'], reverse=True)
            best_strategy = phase2_results[0]
            logging.info(
                f"\n  ✅ 阶段2完成: 最优策略参数 {best_strategy['strategy_params']}, "
                f"收益率: {best_strategy['total_return_pct']:.2f}%"
            )

            # 合并结果：把阶段1的TOP N卖出参数 + 阶段2的策略参数组合在一起
            # 用阶段1的TOP N卖出参数 × 阶段2的TOP 3策略参数做最终验证（可选，默认只返回最佳）
            results = []
            # 把阶段2的结果放前面（包含最优卖出+最优策略）
            for r in phase2_results:
                r['sell_params'] = best_sell  # 确保卖出参数一致
                results.append(r)

            total_combo = n_sell + n_strategy
            logging.info(f"\n{'=' * 70}")
            logging.info(
                f"✅ 两阶段优化完成: {total_combo} 组 (原全量: {n_sell * n_strategy})"
            )
            logging.info(f"⏱️  节省: {(1 - total_combo / (n_sell * n_strategy)) * 100:.0f}% 时间")
            logging.info(f"{'=' * 70}")

            # 排序并排名
            results.sort(key=lambda x: x['total_return_pct'], reverse=True)
            for i, r in enumerate(results[:top_n], 1):
                r['rank'] = i
            return results[:top_n]
    else:
        # 没有策略参数网格，只返回阶段1结果
        for i, r in enumerate(phase1_results[:top_n], 1):
            r['rank'] = i
        total_combo = n_sell
        logging.info(f"\n{'=' * 70}")
        logging.info(f"✅ 优化完成: {total_combo} 组")
        logging.info(f"{'=' * 70}")
        return phase1_results[:top_n]

    return []


def run_random_optimization(
    kline_dict: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    strategy_id: str = 'breakthrough_volume',
    n_iter: int = 300,
    sell_param_grid: Dict[str, List[float]] = None,
    strategy_param_grid: Dict[str, List[Any]] = None,
    initial_cash: float = 1000000.0,
    max_positions: int = 10,
    top_n: int = 20,
    sample_size: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    随机搜索参数优化

    从参数空间中随机采样 n_iter 组，比全量网格搜索更高效。
    研究表明，对于高维参数空间，随机搜索通常优于网格搜索。

    参数:
        n_iter: 随机搜索迭代次数（默认300，约5-50分钟取决于股票数）
        其他参数同 run_optimization()
    """
    if sell_param_grid is None:
        sell_param_grid = DEFAULT_SELL_GRID.copy()

    kline_dict = _sample_kline_dict(kline_dict, sample_size)

    logging.info(f"\n{'=' * 70}")
    logging.info(f"🎲 随机搜索: {STRATEGY_CONFIGS[strategy_id]['name']}")
    logging.info(f"📊 迭代次数: {n_iter}")
    if sample_size:
        logging.info(f"📊 抽样股票: {sample_size} 只")
    logging.info(f"📅 回测区间: {start_date} ~ {end_date}")
    logging.info(f"{'=' * 70}")

    sell_keys = list(sell_param_grid.keys())
    sell_values = list(sell_param_grid.values())

    if strategy_param_grid:
        strategy_keys = list(strategy_param_grid.keys())
        strategy_values = list(strategy_param_grid.values())
    else:
        strategy_keys = []
        strategy_values = [[]]

    original_params = STRATEGY_CONFIGS[strategy_id]['params'].copy()
    results = []
    seen = set()

    try:
        for i in range(1, n_iter + 1):
            # 随机选择一组参数
            sell_params = {
                k: random.choice(sell_param_grid[k]) for k in sell_keys
            }
            strat_params = {}
            if strategy_param_grid:
                strat_params = {
                    k: random.choice(strategy_param_grid[k]) for k in strategy_keys
                }

            # 去重
            combo_key = str(sell_params) + str(strat_params)
            if combo_key in seen:
                continue
            seen.add(combo_key)

            if i % 10 == 0 or i == 1:
                logging.info(f"  进度: {i}/{n_iter} ...")

            r = _run_single_combo(
                kline_dict, start_date, end_date, strategy_id,
                sell_params, strat_params,
                initial_cash, max_positions,
            )
            if r:
                results.append(r)

    finally:
        STRATEGY_CONFIGS[strategy_id]['params'] = original_params

    results.sort(key=lambda x: x['total_return_pct'], reverse=True)
    for i, r in enumerate(results[:top_n], 1):
        r['rank'] = i

    logging.info(f"\n{'=' * 70}")
    logging.info(f"✅ 随机搜索完成，共 {len(results)} 组有效结果 ({n_iter} 次迭代)")
    logging.info(f"{'=' * 70}")

    return results[:top_n]


def print_optimization_results(results: List[Dict[str, Any]]):
    """打印参数优化结果"""
    if not results:
        logging.info("  无有效结果")
        return

    lines = [
        f"\n{'=' * 140}",
        f"  🏆 参数优化结果 TOP {len(results)}",
        f"{'=' * 140}",
        f"  {'排名':<4s} {'收益率':>8s} {'最大回撤':>8s} {'收益额':>12s} {'交易数':>6s} {'胜率':>7s} {'终值':>12s} {'卖出参数':<50s} {'策略参数':<30s}",
        f"  {'-' * 138}",
    ]

    for r in results:
        sell_str = ' '.join(f"{k}={v}" for k, v in r['sell_params'].items())
        strat_str = ' '.join(f"{k}={v}" for k, v in r['strategy_params'].items()) if r['strategy_params'] else ""
        dd_pct = r.get('max_drawdown_pct', 0)
        lines.append(
            f"  {r['rank']:<4d} {r['total_return_pct']:>7.2f}% "
            f"{dd_pct:>7.2f}% "
            f"{r['total_profit_amount']:>11,.0f}元 "
            f"{r['n_trades']:>5d} {r['win_rate']:>6.1f}% "
            f"{r['final_value']:>11,.0f}元 "
            f"{sell_str:<50s} {strat_str:<30s}"
        )

    lines.append(f"{'=' * 140}")
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
