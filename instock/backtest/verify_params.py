"""验证参数 + 策略组合的回测表现

用法：
    # 默认参数（均线多头 + 默认卖出参数）
    python instock/backtest/verify_params.py

    # 指定策略
    python instock/backtest/verify_params.py -s turtle_trade

    # 指定卖出参数
    python instock/backtest/verify_params.py -s breakthrough_volume --stop-loss -0.08 --stop-profit 0.20

    # 自定义时间段
    python instock/backtest/verify_params.py -s keep_increasing --start 2025-06-01 --end 2026-01-01

    # 列出所有可用策略
    python instock/backtest/verify_params.py --list

    # 指定日志文件
    python instock/backtest/verify_params.py --log verify_result.log
"""
import argparse
import os
import sys
import logging

# 添加项目根目录
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from instock.backtest.backtest_strategy_job import load_kline_data
from instock.backtest.backtest_runner import (
    run_backtest,
    summarize,
    print_summary,
    print_trades,
    simulate_portfolio,
    print_portfolio_summary,
    STRATEGY_CONFIGS,
    DEFAULT_SELL_PARAMS,
)

# 策略名称映射
_strategy_names = {k: v['name'] for k, v in STRATEGY_CONFIGS.items()}

# ==================== 默认配置 ====================
DEFAULT_STRATEGY = 'keep_increasing'
DEFAULT_START = '2026-01-01'
DEFAULT_END = '2026-07-01'
DEFAULT_LOG_FILE = 'verify_params.log'


def build_parser():
    p = argparse.ArgumentParser(
        description='验证回测参数 + 策略组合的表现',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='示例:\n'
               '  python instock/backtest/verify_params.py\n'
               '  python instock/backtest/verify_params.py -s breakthrough_volume\n'
               '  python instock/backtest/verify_params.py -s keep_increasing --stop-loss -0.08 --stop-profit 0.20\n'
               '  python instock/backtest/verify_params.py --start 2025-06-01 --end 2026-01-01 -s low_atr\n'
               '  python instock/backtest/verify_params.py --list\n',
    )
    p.add_argument('-s', '--strategy', default=DEFAULT_STRATEGY,
                   help=f'策略ID（默认: {DEFAULT_STRATEGY}）')
    p.add_argument('--start', default=DEFAULT_START,
                   help=f'回测起始日期（默认: {DEFAULT_START}）')
    p.add_argument('--end', default=DEFAULT_END,
                   help=f'回测结束日期（默认: {DEFAULT_END}）')
    p.add_argument('--stop-loss', type=float, default=DEFAULT_SELL_PARAMS['stop_loss'],
                   help=f'止损比例，负数（默认: {DEFAULT_SELL_PARAMS["stop_loss"]}）')
    p.add_argument('--stop-profit', type=float, default=DEFAULT_SELL_PARAMS['stop_profit'],
                   help=f'止盈比例（默认: {DEFAULT_SELL_PARAMS["stop_profit"]}）')
    p.add_argument('--trailing-stop', type=float, default=DEFAULT_SELL_PARAMS['trailing_stop'],
                   help=f'移动止损回撤比例，负数（默认: {DEFAULT_SELL_PARAMS["trailing_stop"]}）')
    p.add_argument('--trailing-activation', type=float, default=DEFAULT_SELL_PARAMS.get('trailing_stop_activation', 0.06),
                   help=f'移动止损激活盈利阈值（默认: {DEFAULT_SELL_PARAMS.get("trailing_stop_activation", 0.06)}）')
    p.add_argument('--max-hold-days', type=int, default=DEFAULT_SELL_PARAMS['max_hold_days'],
                   help=f'最大持仓天数（默认: {DEFAULT_SELL_PARAMS["max_hold_days"]}）')
    p.add_argument('--initial-cash', type=float, default=1000000.0,
                   help='初始资金（默认: 100万）')
    p.add_argument('--max-positions', type=int, default=10,
                   help='最大持仓数（默认: 10）')
    p.add_argument('--log', default=DEFAULT_LOG_FILE,
                   help=f'日志文件路径（默认: {DEFAULT_LOG_FILE}）')
    p.add_argument('--list', action='store_true', help='列出所有可用策略')
    return p


if __name__ == '__main__':
    parser = build_parser()
    args = parser.parse_args()

    if args.list:
        print('\n可用策略:')
        print('-' * 50)
        for sid, cfg in STRATEGY_CONFIGS.items():
            print(f'  {sid:<25s} {cfg["name"]}  — {cfg["description"]}')
        print()
        sys.exit(0)

    strategy_id = args.strategy
    start_date = args.start
    end_date = args.end

    if strategy_id not in STRATEGY_CONFIGS:
        print(f'❌ 未知策略: {strategy_id}')
        print(f'   可用策略: {", ".join(STRATEGY_CONFIGS.keys())}')
        sys.exit(1)

    strategy_name = STRATEGY_CONFIGS[strategy_id]['name']
    sell_params = {
        'stop_loss': args.stop_loss,
        'stop_profit': args.stop_profit,
        'trailing_stop': args.trailing_stop,
        'trailing_stop_activation': args.trailing_activation,
        'max_hold_days': args.max_hold_days,
    }

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(args.log, encoding='utf-8'),
        ],
    )

    logging.info('=' * 60)
    logging.info(f'🔬 验证: {strategy_name} ({strategy_id}) | {start_date} ~ {end_date}')
    sell_str = ' '.join(f'{k}={v}' for k, v in sell_params.items())
    logging.info(f'📋 卖出参数: {sell_str}')
    logging.info('=' * 60)

    kline_dict = load_kline_data(start_date, end_date)
    if not kline_dict:
        logging.error('❌ 无K线数据')
        sys.exit(1)

    trades = run_backtest(kline_dict, start_date, end_date, strategy_id, sell_params)
    summary = summarize(trades, strategy_name, start_date, end_date)
    print_summary(summary)
    print_trades(trades)

    pf = simulate_portfolio(trades, kline_dict,
                            initial_cash=args.initial_cash, max_positions=args.max_positions,
                            start_date=start_date, end_date=end_date)
    print_portfolio_summary(pf)
