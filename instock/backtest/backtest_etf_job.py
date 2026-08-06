#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF 择时策略回测脚本
=====================

直接对 fund_etf_hist_em 表中的 ETF 运行策略回测。

与 backtest_strategy_job.py 的区别：
  - 数据源: fund_etf_hist_em (ETF) vs cn_stock_spot (个股)
  - ETF 数量少(3只)，无需并行加速，串行更清晰

运行方式：
  # 运行单个策略
  python instock/backtest/backtest_etf_job.py dual_momentum
  python instock/backtest/backtest_etf_job.py ma200_trend 2024-01-01 2026-07-21

  # 全部 ETF 策略对比
  python instock/backtest/backtest_etf_job.py all

  # 指定策略对比
  python instock/backtest/backtest_etf_job.py compare dual_momentum,ma200_trend,bollinger_reversion

支持的ETF策略：dual_momentum, ma200_trend, bollinger_reversion, vol_targeting, adaptive_momentum
"""

import os
import sys
import logging

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import pandas as pd  # noqa: E402
import instock.lib.database as mdb  # noqa: E402
from instock.backtest.backtest_runner import (  # noqa: E402
    run_backtest,
    run_multi_backtest,
    summarize,
    print_summary,
    print_trades,
    print_comparison,
    STRATEGY_CONFIGS,
    DEFAULT_SELL_PARAMS,
)

# ETF 策略ID列表
ETF_STRATEGY_IDS = [
    'dual_momentum', 'ma200_trend', 'bollinger_reversion',
    'vol_targeting', 'adaptive_momentum',
]

_BACKTEST_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log')


def _setup_logging(log_filename='stock_etf_backtest.log'):
    """配置日志：终端 + 文件双向输出"""
    if not os.path.exists(_BACKTEST_LOG_DIR):
        os.makedirs(_BACKTEST_LOG_DIR)
    log_file = os.path.join(_BACKTEST_LOG_DIR, log_filename)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
        force=True,
    )
    return log_file


# ==================== 数据加载 ====================

def load_etf_kline(start_date: str, end_date: str,
                   codes: list[str] | None = None,
                   table_name: str = 'fund_etf_hist_em') -> dict[str, pd.DataFrame]:
    """
    从 fund_etf_hist_em 加载 ETF K 线数据

    参数:
        start_date: 回测开始日期 'YYYY-MM-DD'
        end_date:   回测结束日期
        codes:      ETF 代码列表，None=全量
        table_name: 表名

    返回:
        {code: DataFrame(必须有: date/open/close/high/low/volume/p_change/name)}
    """
    buffer_days = 300
    start_with_buffer = (
        pd.to_datetime(start_date) - pd.Timedelta(days=buffer_days)
    ).strftime('%Y-%m-%d')

    code_filter = ""
    if codes:
        codes_str = "','".join(str(c) for c in codes)
        code_filter = f"AND `code` IN ('{codes_str}')"

    sql = f"""
        SELECT `code`, `name`, `date`,
               `open`, `close`, `high`, `low`, `volume`,
               `quote_change` AS `p_change`
        FROM `{table_name}`
        WHERE `date` >= '{start_with_buffer}' AND `date` <= '{end_date}'
        {code_filter}
        ORDER BY `code`, `date`
    """

    logging.info(f'📊 加载ETF历史数据: {start_with_buffer} ~ {end_date}')
    df = pd.read_sql(sql=sql, con=mdb.engine())
    logging.info(f'  ✅ 共 {len(df)} 条 K 线记录')

    if len(df) == 0:
        logging.warning('  ⚠️ 没有数据！请先运行 python script/fetch_etf_history.py')
        return {}

    kline_dict = {}
    grouped = df.groupby('code')
    for code, group in grouped:
        group = group.sort_values('date').reset_index(drop=True)
        group['date'] = group['date'].astype(str).str[:10]
        kline_dict[code] = group
        logging.info(f'  📦 {code}: {len(group)}条 ({group.iloc[0]["date"]} ~ {group.iloc[-1]["date"]})')

    logging.info(f'  ✅ 共加载 {len(kline_dict)} 只 ETF')
    return kline_dict


# ==================== 主入口 ====================

def main():
    args = sys.argv[1:]
    start_date = '2020-01-01'
    end_date = '2026-07-21'
    mode = 'single'
    strategy_id = 'ma200_trend'
    strategy_ids = []

    # 解析参数
    date_args = []
    strategy_args = []
    for a in args:
        if a.startswith('20') and len(a) == 10:
            date_args.append(a)
        else:
            strategy_args.append(a)

    if len(date_args) >= 2:
        start_date, end_date = date_args[0], date_args[1]

    if not strategy_args:
        strategy_id = 'ma200_trend'
        strategy_ids = [strategy_id]
    elif strategy_args[0] == 'all':
        mode = 'compare'
        strategy_ids = ETF_STRATEGY_IDS
    elif strategy_args[0] == 'compare' and len(strategy_args) >= 2:
        mode = 'compare'
        strategy_ids = strategy_args[1].split(',')
    else:
        strategy_id = strategy_args[0]
        strategy_ids = [strategy_id]

    # 验证策略ID
    invalid = [s for s in strategy_ids if s not in STRATEGY_CONFIGS]
    if invalid:
        logging.error(f'❌ 未知策略ID: {invalid}')
        logging.info(f'可用ETF策略: {", ".join(ETF_STRATEGY_IDS)}')
        return

    _setup_logging()
    logging.info('=' * 60)
    logging.info('🚀 ETF 择时策略回测')
    logging.info(f'📅 区间: {start_date} ~ {end_date}')
    logging.info(f'📊 策略: {strategy_ids}')
    logging.info('=' * 60)

    # 加载数据
    kline_dict = load_etf_kline(start_date, end_date)
    if not kline_dict:
        return

    # 设置卖出参数（ETF持仓周期可更长）
    etf_sell_params = DEFAULT_SELL_PARAMS.copy()
    etf_sell_params.update({
        'stop_loss': -0.10,       # ETF波动小，止损放宽到10%
        'stop_profit': 0.25,      # 止盈放宽到25%
        'trailing_stop': -0.08,   # 移动止损从高点回撤8%
        'trailing_stop_activation': 0.10,  # 浮盈10%激活移动止损
        'max_hold_days': 120,     # ETF持仓可更长（120交易日≈半年）
        'cooldown_days': 20,
    })

    if mode == 'compare':
        results = run_multi_backtest(kline_dict, start_date, end_date, strategy_ids, etf_sell_params)
        summaries = {}
        for sid, trades in results.items():
            name = STRATEGY_CONFIGS[sid]['name']
            summaries[sid] = summarize(trades, name, start_date, end_date)
        print_comparison(summaries)
    else:
        trades = run_backtest(kline_dict, start_date, end_date, strategy_id, etf_sell_params)
        name = STRATEGY_CONFIGS[strategy_id]['name']
        summary = summarize(trades, name, start_date, end_date)
        print_summary(summary)
        print_trades(trades)


if __name__ == '__main__':
    main()
