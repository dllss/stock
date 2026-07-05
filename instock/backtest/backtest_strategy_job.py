#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测策略任务模块（多策略版）
==============================

完整的选股→买入→卖出→统计回测流程。
支持 10 种策略独立运行或多策略对比。

与 backtest_data_daily_job.py 的区别：
- backtest_data_daily_job：对现有策略选股结果，固定持有算N日收益率
- 本模块：独立运行，模拟真实交易（止损/止盈/移动止损/最大持仓）

支持的策略ID：
  turtle_trade, breakthrough_volume, breakthrough_platform,
  backtrace_ma250, keep_increasing, low_atr, low_backtrace,
  parking_apron, high_tight_flag, climax_limitdown

运行方式：
  # 运行单个策略
  python backtest_strategy_job.py turtle_trade
  python backtest_strategy_job.py breakthrough_volume 2025-01-01 2026-01-01

  # 运行全部策略对比
  python backtest_strategy_job.py all

  # 运行指定几个策略对比
  python backtest_strategy_job.py compare turtle_trade,keep_increasing,parking_apron

  # 无参数默认运行量能突破
  python backtest_strategy_job.py
"""

import os
import sys
import logging
import datetime
import json
import pandas as pd

# 必须先添加项目根目录到 sys.path，才能 import instock 模块
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import instock.lib.database as mdb  # noqa: E402
from instock.backtest.backtest_runner import (  # noqa: E402
    run_backtest,
    run_multi_backtest,
    summarize,
    print_summary,
    print_trades,
    print_comparison,
    simulate_portfolio,
    print_portfolio_summary,
    STRATEGY_CONFIGS,
    DEFAULT_SELL_PARAMS,
)
from instock.backtest.optimizer import (  # noqa: E402
    run_optimization,
    print_optimization_results,
    find_best_params,
)

__author__ = 'myh '
__date__ = '2026/06/08 '


# ==================== 数据加载 ====================


def load_kline_data(start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
    """
    从 cn_stock_spot 加载K线数据

    返回:
        {code: DataFrame} 每只股票的完整K线数据
    """
    buffer_days = 300  # 往前多加载，确保回踩年线等策略有足够数据
    start_with_buffer = (
        pd.to_datetime(start_date) - pd.Timedelta(days=buffer_days)
    ).strftime('%Y-%m-%d')

    logging.info(f'📊 加载K线数据: {start_with_buffer} ~ {end_date}')

    sql = f"""
        SELECT `code`, `name`, `date`,
               `open_price` as `open`,
               `new_price` as `close`,
               `high_price` as `high`,
               `low_price` as `low`,
               `volume`,
               `change_rate` as `p_change`
        FROM `cn_stock_spot`
        WHERE `date` >= '{start_with_buffer}' AND `date` <= '{end_date}'
        ORDER BY `code`, `date`
    """

    try:
        logging.info('  ⏳ 正在查询数据库...')
        df = pd.read_sql(sql=sql, con=mdb.engine())
        logging.info(f'  ✅ 加载 {len(df)} 条K线记录')

        if len(df) == 0:
            logging.warning('  ⚠️ 没有K线数据')
            return {}

        # 按 code 分组
        logging.info('  ⏳ 正在按股票分组...')
        kline_dict = {}
        total_codes = df['code'].nunique()
        grouped = df.groupby('code')
        for i, (code, group) in enumerate(grouped):
            kline_dict[code] = group.sort_values('date').reset_index(drop=True)
            if (i + 1) % 500 == 0 or (i + 1) == total_codes:
                logging.info(f'  📦 分组进度: {i + 1}/{total_codes} 只股票')

        logging.info(f'  ✅ 共 {len(kline_dict)} 只股票')
        return kline_dict

    except Exception as e:
        logging.error(f'  ❌ 加载K线数据失败: {e}')
        return {}


# ==================== 单策略运行 ====================


def run_single_strategy(
    kline_dict: dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    strategy_id: str,
    sell_params: dict[str, int | float] | None = None,
):
    """运行单个策略回测"""
    if strategy_id not in STRATEGY_CONFIGS:
        logging.error(f'  ❌ 未知策略ID: {strategy_id}')
        logging.info(f'  可用策略: {", ".join(STRATEGY_CONFIGS.keys())}')
        return

    config = STRATEGY_CONFIGS[strategy_id]
    strategy_name: str = str(config['name'])

    if sell_params is None:
        sell_params = DEFAULT_SELL_PARAMS.copy()

    logging.info('=' * 60)
    logging.info(f'🚀 回测策略: {strategy_name} ({strategy_id})')
    logging.info(f'📋 策略描述: {config["description"]}')
    logging.info(f'📋 卖出参数: {json.dumps(sell_params, ensure_ascii=False)}')
    logging.info(f'💰 初始资金: 1,000,000 元')
    logging.info('=' * 60)

    trades = run_backtest(kline_dict, start_date, end_date, strategy_id, sell_params)
    summary = summarize(trades, strategy_name, start_date, end_date)
    print_summary(summary)
    print_trades(trades)

    # 资金账户模拟（初始100万，最多持10只）
    portfolio_result = simulate_portfolio(
        trades,
        kline_dict,
        initial_cash=1000000.0,
        max_positions=10,
        start_date=start_date,
        end_date=end_date,
    )
    print_portfolio_summary(portfolio_result)

    # 输出最终收益汇总
    if portfolio_result:
        logging.info(
            f"\n{'=' * 60}\n"
            f"  📊 最终收益汇总 - {strategy_name}\n"
            f"{'=' * 60}\n"
            f"  初始资金:      1,000,000.00 元\n"
            f"  最终资产:      {portfolio_result['final_value']:>12,.2f} 元\n"
            f"  总收益:        {portfolio_result['final_value'] - 1000000:>+12,.2f} 元\n"
            f"  总收益率:      {portfolio_result['total_return_pct']:>+7.2f}%\n"
            f"{'=' * 60}"
        )


# ==================== 多策略对比 ====================


def run_compare(
    kline_dict: dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    strategy_ids: list[str] | None = None,
    sell_params: dict[str, int | float] | None = None,
    separate_logs: bool = False,
):
    """多策略对比回测
    
    参数:
        separate_logs: 是否为每个策略生成单独的日志文件
    """
    if strategy_ids is None:
        strategy_ids = list(STRATEGY_CONFIGS.keys())

    if sell_params is None:
        sell_params = DEFAULT_SELL_PARAMS.copy()

    logging.info('=' * 60)
    logging.info('🚀 多策略对比回测')
    logging.info(f'📋 策略数量: {len(strategy_ids)}')
    logging.info(f'📋 策略列表: {", ".join(strategy_ids)}')
    logging.info('=' * 60)

    results = run_multi_backtest(
        kline_dict, start_date, end_date, strategy_ids, sell_params
    )

    # 汇总对比
    summaries = {}
    portfolio_results = {}
    
    for sid, trades in results.items():
        config = STRATEGY_CONFIGS[sid]
        strategy_name: str = str(config['name'])
        
        # 如果要求单独日志，切换日志文件
        if separate_logs:
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d%H%M')
            log_filename = f"{strategy_name}_{timestamp}.log"
            from instock.lib.logger_config import setup_job_logging
            setup_job_logging(log_filename)
            
            logging.info('=' * 60)
            logging.info(f'🚀 策略: {strategy_name} ({sid})')
            logging.info('=' * 60)

        s = summarize(trades, strategy_name, start_date, end_date)
        summaries[sid] = s

        # 打印交易详情（前30笔盈利 + 前30笔亏损）
        if len(trades) > 0:
            logging.info('\n' + '=' * 60)
            logging.info(f'📋 策略: {strategy_name} ({sid}) - 交易详情')
            logging.info('=' * 60)
            print_trades(trades, top_n=30)

        # 模拟账户收益
        if len(trades) > 0:
            portfolio_result = simulate_portfolio(
                trades,
                kline_dict,
                initial_cash=1000000.0,
                max_positions=10,
                start_date=start_date,
                end_date=end_date,
            )
            portfolio_results[sid] = portfolio_result
            print_portfolio_summary(portfolio_result)

    # 打印对比表
    print_comparison(summaries)

    # 打印模拟账户对比
    if portfolio_results:
        logging.info('\n' + '=' * 60)
        logging.info('💰 模拟账户收益对比 (初始资金: 1,000,000 元)')
        logging.info('=' * 60)
        logging.info(f"{'策略':<20} {'最终资产':>15} {'收益率':>10} {'交易数':>8}")
        logging.info('-' * 60)
        for sid, pr in portfolio_results.items():
            config = STRATEGY_CONFIGS[sid]
            strategy_name = config['name']
            logging.info(f"{strategy_name:<20} {pr['final_value']:>15,.2f} {pr['total_return_pct']:>9.2f}% {pr['n_trades']:>8}")
        logging.info('=' * 60)


# ==================== 参数优化 ====================


def run_optimize(
    kline_dict: dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    strategy_id: str = 'breakthrough_volume',
    target_return: float = 20.0,
):
    """
    运行参数优化，寻找最优参数组合

    参数:
        kline_dict: {code: DataFrame}
        start_date/end_date: 回测区间
        strategy_id: 策略ID
        target_return: 目标收益率（%），默认20%
    """
    logging.info(f"\n{'=' * 70}")
    logging.info(f"🎯 参数优化: {STRATEGY_CONFIGS[strategy_id]['name']}")
    logging.info(f"🎯 目标收益率: {target_return}%")
    logging.info(f"{'=' * 70}")

    # 运行参数扫描
    results = run_optimization(
        kline_dict, start_date, end_date,
        strategy_id=strategy_id,
        top_n=20,
    )

    # 打印结果
    print_optimization_results(results)

    # 查找达到目标收益率的参数
    best = find_best_params(
        kline_dict, start_date, end_date,
        strategy_id=strategy_id,
        target_return_pct=target_return,
    )

    if best:
        logging.info(f"\n{'=' * 70}")
        logging.info(f"🏆 推荐参数组合 (排名 {best.get('rank', 1)})")
        logging.info(f"{'=' * 70}")
        logging.info(f"  收益率:   {best['total_return_pct']:.2f}%")
        logging.info(f"  收益额:   {best['total_profit_amount']:,.0f} 元")
        logging.info(f"  交易次数: {best['n_trades']}")
        logging.info(f"  胜率:     {best['win_rate']:.1f}%")
        logging.info(f"  卖出参数: {best['sell_params']}")
        if best.get('strategy_params'):
            logging.info(f"  策略参数: {best['strategy_params']}")
        logging.info(f"{'=' * 70}")

    return results, best


# ==================== 主流程 ====================


def main(strategy_spec: str | None = None, start_date: str | None = None, end_date: str | None = None):
    """
    执行回测

    参数:
        strategy_spec:
            - None / 空: 默认运行 breakthrough_volume
            - 'all': 运行全部10个策略对比
            - 'compare:id1,id2,...': 指定策略对比
            - 'optimize[:id]': 参数优化（可指定策略ID，默认 breakthrough_volume）
            - 'turtle_trade': 单个策略ID
        start_date: 回测起始日期
        end_date: 回测结束日期
    """
    # 确定日期范围
    if end_date is None:
        end_date = datetime.date.today().strftime('%Y-%m-%d')
    if start_date is None:
        start_date = (datetime.date.today() - datetime.timedelta(days=365)).strftime(
            '%Y-%m-%d'
        )

    # 生成日志文件名
    from datetime import datetime as dt
    timestamp = dt.now().strftime('%Y%m%d%H%M')
    
    if strategy_spec is None or strategy_spec == '' or (not strategy_spec.startswith('all') and not strategy_spec.startswith('compare') and not strategy_spec.startswith('optimize')):
        # 单个策略
        if strategy_spec and strategy_spec in STRATEGY_CONFIGS:
            strategy_name = STRATEGY_CONFIGS[strategy_spec]['name']
        else:
            strategy_name = '量能突破'  # 默认
        log_filename = f"{strategy_name}_{timestamp}.log"
    elif strategy_spec == 'all':
        # 全部策略对比
        log_filename = f"多策略对比_{timestamp}.log"
    elif strategy_spec.startswith('compare:'):
        # 指定策略对比
        log_filename = f"多策略对比_{timestamp}.log"
    else:
        # 参数优化等其他模式
        log_filename = f"回测_{timestamp}.log"
    
    # 配置日志
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging(log_filename)

    logging.info(f'📅 回测区间: {start_date} ~ {end_date}')
    logging.info(f'📋 日志文件: {log_filename}')

    # 1. 加载数据
    kline_dict = load_kline_data(start_date, end_date)
    if not kline_dict:
        logging.error('❌ 无法加载K线数据，回测终止')
        return

    # 2. 解析策略参数
    if strategy_spec is None or strategy_spec == '':
        # 默认运行量能突破
        run_single_strategy(kline_dict, start_date, end_date, 'breakthrough_volume')
    elif strategy_spec == 'all':
        # 全部策略对比
        run_compare(kline_dict, start_date, end_date)
    elif strategy_spec.startswith('compare:'):
        # 指定策略对比
        ids_str = strategy_spec.replace('compare:', '')
        ids = [s.strip() for s in ids_str.split(',') if s.strip() in STRATEGY_CONFIGS]
        if ids:
            run_compare(kline_dict, start_date, end_date, ids)
        else:
            logging.error(f'  ❌ 没有有效的策略ID: {ids_str}')
            logging.info(f'  可用策略: {", ".join(STRATEGY_CONFIGS.keys())}')
    elif strategy_spec.startswith('optimize'):
        # 参数优化
        if strategy_spec == 'optimize':
            strategy_id = 'breakthrough_volume'  # 默认优化量能突破
        else:
            strategy_id = strategy_spec.replace('optimize:', '').strip()
            if strategy_id not in STRATEGY_CONFIGS:
                logging.error(f'  ❌ 未知策略: {strategy_id}')
                logging.info(f'  可用策略: {", ".join(STRATEGY_CONFIGS.keys())}')
                return
        _ = run_optimize(kline_dict, start_date, end_date, strategy_id)
    else:
        # 单个策略
        run_single_strategy(kline_dict, start_date, end_date, strategy_spec)

    logging.info('=' * 60)
    logging.info('🎉 回测任务完成')
    logging.info('=' * 60)


# ==================== 程序入口 ====================

if __name__ == '__main__':
    from instock.lib.logger_config import setup_job_logging

    _ = setup_job_logging(log_filename='stock_backtest_strategy.log')

    # 支持命令行参数
    args = sys.argv[1:]

    strategy_spec = None
    start_date = None
    end_date = None

    if len(args) >= 3:
        strategy_spec = args[0]
        start_date = args[1]
        end_date = args[2]
    elif len(args) == 2:
        # 可能是 strategy date 或 date1 date2
        if (
            args[0] in STRATEGY_CONFIGS
            or args[0] in ('all',)
            or args[0].startswith('compare:')
            or args[0].startswith('optimize')
        ):
            strategy_spec = args[0]
            start_date = args[1]
        else:
            start_date = args[0]
            end_date = args[1]
    elif len(args) == 1:
        if (
            args[0] in STRATEGY_CONFIGS
            or args[0] in ('all',)
            or args[0].startswith('compare:')
            or args[0].startswith('optimize')
        ):
            strategy_spec = args[0]
        else:
            start_date = args[0]

    main(strategy_spec, start_date, end_date)
