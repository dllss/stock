#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整回测引擎模块（多策略版）
===============================

功能：选股 → 买入 → 卖出 → 统计，全流程回测。
支持 10 种选股策略，可单独或对比运行。

与现有 backtest_data_daily_job.py 的区别：
- 现有回测：策略选股后，固定持有N天，只算收益率
- 本模块：模拟真实交易，有止损/止盈/移动止损/最大持仓天数

支持的策略：
  1. turtle_trade       — 海龟交易法则（60日新高突破）
  2. breakthrough_volume — 量能突破（N日高点+放量+MA过滤）
  3. breakthrough_platform— 突破平台（横盘后放量突破MA60）
  4. backtrace_ma250     — 回踩年线（突破MA250→回踩不破→缩量）
  5. keep_increasing     — 均线多头（MA30持续递增+月涨>20%）
  6. low_atr             — 低ATR成长（低波动+10天涨>10%）
  7. low_backtrace       — 无大幅回撤（60天涨60%+无大跌）
  8. parking_apron       — 停机坪（涨停后高位横盘3天）
  9. high_tight_flag     — 高而窄旗形（短期涨90%+连续涨停）
  10. climax_limitdown   — 放量跌停（跌停+成交额>=2亿+量比>=4）

卖出规则（所有策略统一）：
  - 止损 -8%
  - 止盈 +20%
  - 移动止损：从高点回落 -5%（浮盈>=6%时启用）
  - 最大持仓 60 天

数据表：
  - cn_backtest_trade: 每笔交易明细
  - cn_backtest_summary: 汇总统计
"""

import logging
import datetime
import numpy as np
import pandas as pd
from typing import Optional, Dict, List, Tuple, Callable, Any
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

__author__ = 'myh '
__date__ = '2026/06/08 '


# ==================== 交易记录类 ====================


class Trade:
    """单笔交易记录"""

    def __init__(self, code: str, name: str, buy_date: str, buy_price: float, signal_score: float = 0.0):
        self.code = code
        self.name = name
        self.buy_date = buy_date
        self.buy_price = buy_price
        self.signal_score = signal_score  # 买入信号质量评分（越高越好）
        self.sell_date: Optional[str] = None
        self.sell_price: float = 0.0
        self.sell_reason: str = (
            ''  # stop_loss / stop_profit / trailing_stop / max_hold / data_end
        )
        self.profit_pct: float = 0.0
        self.hold_days: int = 0
        self.max_profit_pct: float = 0.0  # 持仓期间最大浮盈
        self.max_loss_pct: float = 0.0  # 持仓期间最大浮亏

    def to_dict(self) -> dict:
        return {
            'code': self.code,
            'name': self.name,
            'buy_date': self.buy_date,
            'buy_price': round(self.buy_price, 2),
            'signal_score': round(self.signal_score, 2),
            'sell_date': self.sell_date,
            'sell_price': round(self.sell_price, 2),
            'sell_reason': self.sell_reason,
            'profit_pct': round(self.profit_pct, 2),
            'hold_days': self.hold_days,
            'max_profit_pct': round(self.max_profit_pct, 2),
            'max_loss_pct': round(self.max_loss_pct, 2),
        }


# ==================== 资金账户类 ====================


class Portfolio:
    """资金账户，模拟真实资金分配和收益"""

    def __init__(self, initial_cash: float = 1000000.0, max_positions: int = 10):
        """
        参数:
            initial_cash: 初始资金（默认100万）
            max_positions: 最大持仓数量（默认10只）
        """
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.pending_cash = {}  # {date: amount} T+1延迟到账资金
        self.max_positions = max_positions
        self.positions = {}  # {code: {'shares': int, 'cost': float, 'buy_date': str, 'name': str, 'buy_commission': float}}
        self.closed_trades = []  # 已平仓交易记录
        self.portfolio_history = []  # 每日资产快照

    def update_pending_cash(self, date: str):
        """更新T+1延迟到账的资金（卖出资金第二个交易日可用）"""
        # 检查今天是否有到账的资金
        if date in self.pending_cash:
            amount = self.pending_cash[date]
            self.cash += amount
            del self.pending_cash[date]
            pending_total = sum(self.pending_cash.values())
            logging.info(
                f'  💰 T+1资金到账 {date} 金额:{amount:,.2f}元 | '
                f'现金:{self.cash:,.2f}元(可用) + {pending_total:,.2f}元(延迟) = {self.cash + pending_total:,.2f}元(总)'
            )

    def get_portfolio_value(self, price_dict: dict) -> float:
        """计算当前总资产 = 现金 + pending_cash + 持仓市值"""
        position_value = 0.0
        for code, pos in self.positions.items():
            price = price_dict.get(code, pos['cost'])
            position_value += pos['shares'] * price
        return self.cash + sum(self.pending_cash.values()) + position_value

    def try_buy(self, code: str, name: str, date: str, price: float) -> dict or None:
        """尝试买入，成功返回买入记录dict，失败返回None"""
        if len(self.positions) >= self.max_positions:
            return None  # 已满仓
        if code in self.positions:
            return None  # 已持有
        if self.cash <= 0:
            return None  # 没钱

        # 每只股票分配资金 = 初始资金 / 最大持仓数
        alloc = self.initial_cash / self.max_positions
        alloc = min(alloc, self.cash)

        # 计算可买股数（100股的整数倍）
        shares = int(alloc / price / 100) * 100
        if shares < 100:
            return None  # 不够买1手

        cost = shares * price
        commission = max(cost * 0.00025, 5.0)  # 佣金0.025%，最低5元
        total_cost = cost + commission

        if total_cost > self.cash:
            return None

        self.cash -= total_cost
        self.positions[code] = {
            'shares': shares,
            'cost': price,
            'buy_date': date,
            'name': name,
            'buy_commission': commission,
        }

        # 打印买入信息
        position_value = sum(p['shares'] * p['cost'] for p in self.positions.values())
        logging.info(
            f'  📈 买入 {date} {code}({name}) '
            f'价格:{price:.2f} 股数:{shares} 成本:{total_cost:.0f}元 | '
            f'持仓:{len(self.positions)}只 持仓成本:{position_value:.0f}元 '
            f'现金:{self.cash:,.0f}元'
        )

        return {
            'code': code,
            'name': name,
            'date': date,
            'price': price,
            'shares': shares,
            'cost': total_cost,
        }

    def try_sell(self, code: str, date: str, price: float, reason: str) -> dict or None:
        """尝试卖出，返回交易记录字典，失败返回None"""
        if code not in self.positions:
            return None

        pos = self.positions[code]
        shares = pos['shares']
        buy_price = pos['cost']

        revenue = shares * price
        commission = max(revenue * 0.00025, 5.0)  # 佣金0.025%
        stamp_tax = revenue * 0.0005  # 印花税0.05%（卖出单边）
        total_revenue = revenue - commission - stamp_tax

        cost_total = shares * buy_price + pos['buy_commission']
        profit = total_revenue - cost_total
        profit_pct = profit / cost_total * 100

        # A股规则：卖出当天资金即可用于买入（T+1仅限制提现，不限制交易）
        self.cash += total_revenue

        # 计算持仓天数
        hold_days = 0
        try:
            d1 = datetime.datetime.strptime(pos['buy_date'], '%Y-%m-%d')
            d2 = datetime.datetime.strptime(date, '%Y-%m-%d')
            hold_days = (d2 - d1).days
        except Exception:
            pass

        trade_record = {
            'code': code,
            'name': pos['name'],
            'buy_date': pos['buy_date'],
            'buy_price': buy_price,
            'sell_date': date,
            'sell_price': price,
            'sell_reason': reason,
            'shares': shares,
            'profit': round(profit, 2),
            'profit_pct': round(profit_pct, 2),
            'hold_days': hold_days,
            'profit_amount': round(profit, 2),
        }

        self.closed_trades.append(trade_record)

        # 打印卖出信息
        # 注意：此时self.positions还未删除当前持仓，需要排除
        position_value = sum(p['shares'] * p['cost'] for k, p in self.positions.items() if k != code)
        logging.info(
            f'  📉 卖出 {date} {code}({pos["name"]}) '
            f'价格:{price:.2f} 股数:{shares} '
            f'收益:{profit:+.0f}元({profit_pct:+.1f}%) 原因:{reason} | '
            f'持仓:{len(self.positions)-1}只 持仓成本:{position_value:.0f}元 '
            f'现金:{self.cash:,.0f}元'
        )

        del self.positions[code]
        return trade_record


def simulate_portfolio(
    trades: List[Trade],
    kline_dict: Dict[str, pd.DataFrame],
    initial_cash: float = 1000000.0,
    max_positions: int = 10,
    start_date: str = None,
    end_date: str = None,
) -> dict[str, Any]:
    """
    用资金账户模拟回测，返回统计结果

    参数:
        trades: backtest_stock 生成的 Trade 列表
        kline_dict: {code: kline DataFrame}
        initial_cash: 初始资金（默认100万）
        max_positions: 最大持仓数（默认10）
        start_date: 回测起始日期（用于显示）
        end_date: 回测结束日期（用于显示）
    """
    portfolio = Portfolio(initial_cash, max_positions)

    # 构建 {code: [Trade, ...]} 并按买入日期排序
    from collections import defaultdict

    trade_map: Dict[str, List[Trade]] = defaultdict(list)
    for t in trades:
        trade_map[t.code].append(t)
    for code in trade_map:
        trade_map[code].sort(key=lambda x: x.buy_date)

    # 收集所有交易日期
    all_trade_dates = set()
    for code, tl in trade_map.items():
        for t in tl:
            all_trade_dates.add(t.buy_date)
            if t.sell_date:
                all_trade_dates.add(t.sell_date)

    # 生成完整的交易日历（回测区间内所有有K线数据的日期）
    from datetime import datetime as dt, timedelta

    # 收集所有K线日期
    kline_dates = set()
    for df in kline_dict.values():
        for d in df['date']:
            kline_dates.add(str(d)[:10])

    if start_date and end_date:
        range_start = start_date
        range_end = end_date
    elif all_trade_dates:
        range_start = min(all_trade_dates)
        range_end = max(all_trade_dates)
    else:
        sorted_dates = []
        range_start = None
        range_end = None

    if range_start and range_end:
        all_dates = []
        d = dt.strptime(range_start, '%Y-%m-%d')
        end = dt.strptime(range_end, '%Y-%m-%d')
        while d <= end:
            if d.weekday() < 5:
                all_dates.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)
        sorted_dates = sorted(set(all_dates) & kline_dates)
    else:
        sorted_dates = []

    # 按日期顺序模拟
    peak_value = initial_cash
    max_drawdown = 0.0
    max_drawdown_date = ''
    for date in sorted_dates:
        # 更新T+1延迟到账的资金
        portfolio.update_pending_cash(date)

        # 先处理卖出（使用Trade对象中记录的正确价格）
        for code, tl in sorted(trade_map.items(), key=lambda x: x[0]):
            for t in tl:
                if t.sell_date == date and code in portfolio.positions:
                    portfolio.try_sell(code, date, t.sell_price, t.sell_reason)

        # 再处理买入（使用Trade对象中记录的正确价格）
        # 注意：最后一天不买入，因为买完立刻会被强制平仓，白白损失手续费
        if date != sorted_dates[-1]:
            # 收集当天所有买入候选，按信号评分降序排列（高分优先买入）
            buy_candidates = []
            for tl in trade_map.values():
                for t in tl:
                    if t.buy_date == date and t.signal_score > 0:
                        buy_candidates.append(t)

            buy_candidates.sort(key=lambda t: t.signal_score, reverse=True)

            current_positions = len(portfolio.positions)
            for t in buy_candidates:
                if current_positions >= portfolio.max_positions:
                    break  # 已满仓，跳过买入
                # 买入时检查是否已在持仓中
                if t.code in portfolio.positions:
                    continue
                result = portfolio.try_buy(t.code, t.name, date, t.buy_price)
                if result:
                    current_positions += 1  # 买入成功，更新持仓数量

        # 每日收盘后记录总资产净值（用于计算回撤）
        price_dict = {}
        for code in portfolio.positions:
            if code in kline_dict:
                df = kline_dict[code]
                rows = df[df['date'] <= date]
                if len(rows) > 0:
                    price_dict[code] = rows.iloc[-1]['close']
        total_value = portfolio.get_portfolio_value(price_dict)
        portfolio.portfolio_history.append({
            'date': date,
            'cash': round(portfolio.cash, 2),
            'position_value': round(total_value - portfolio.cash, 2),
            'total_value': round(total_value, 2),
        })

        # 更新最大回撤
        if total_value > peak_value:
            peak_value = total_value
        drawdown_pct = (total_value - peak_value) / peak_value * 100
        if drawdown_pct < max_drawdown:
            max_drawdown = drawdown_pct
            max_drawdown_date = date

    # 回测结束，强制平仓剩余持仓
    # 注意：强制平仓日期应该是 end_date，而不是 sorted_dates[-1]
    # 因为 sorted_dates 可能包含扩展的日期（用于容纳T+1资金到账）
    if end_date:
        last_date = end_date
    elif sorted_dates:
        last_date = sorted_dates[-1]
    else:
        last_date = None
    
    if last_date and portfolio.positions:
        logging.info(f'\n  ⚠️ 回测结束({last_date})，强制平仓 {len(portfolio.positions)} 只持仓:')
        for code in list(portfolio.positions.keys()):
            # 使用最后一天的开盘价作为平仓价（与 backtest_stock 一致）
            close_price = portfolio.positions[code]['cost']  # 兜底
            if code in kline_dict:
                df = kline_dict[code]
                last_rows = df[df['date'] <= last_date]
                if len(last_rows) > 0:
                    close_price = last_rows.iloc[-1]['open']
            portfolio.try_sell(
                code, last_date, close_price, 'data_end'
            )

    # 回测结束，把 pending_cash 中的所有资金转到 cash（不再遵守T+1）
    # 原因：回测已结束，不需要再等待T+1，否则这些资金不会被计算在最终资产中
    if portfolio.pending_cash:
        logging.info(
            f'  ℹ️ 回测结束，将 {len(portfolio.pending_cash)} 笔T+1延迟资金转入现金账户:'
        )
        for date, amount in sorted(portfolio.pending_cash.items()):
            portfolio.cash += amount
            logging.info(
                f'    应到账日期: {date} | 转账金额: {amount:,.2f} 元 | 转账后现金余额: {portfolio.cash:,.2f} 元'
            )
        portfolio.pending_cash.clear()
        logging.info(f'  ✅ 所有延迟资金已转入，最终现金余额: {portfolio.cash:,.2f} 元')

    # 计算最终总资产（现金 + 持仓市值，此时应已无持仓）
    final_value = portfolio.cash
    total_return_pct = (final_value - initial_cash) / initial_cash * 100
    n_trades = len(portfolio.closed_trades)
    n_win = sum(1 for t in portfolio.closed_trades if t['profit'] > 0)
    n_lose = sum(1 for t in portfolio.closed_trades if t['profit'] <= 0)

    total_profit_amount = sum(t['profit'] for t in portfolio.closed_trades)
    win_rate = round(n_win / n_trades * 100, 2) if n_trades > 0 else 0.0

    # 打印最终资金汇总
    logging.info(
        f'\n{"=" * 60}\n'
        f'  💰 资金账户最终状态（初始资金: {initial_cash:,.0f} 元）\n'
        f'{"=" * 60}\n'
        f'  最终总资产:    {final_value:>12,.2f} 元\n'
        f'  总收益:        {final_value - initial_cash:>+12,.2f} 元\n'
        f'  总收益率:      {(final_value - initial_cash) / initial_cash * 100:>+7.2f}%\n'
        f'  最大回撤:      {max_drawdown:>+7.2f}%   ({max_drawdown_date})\n'
        f'  峰值资产:      {peak_value:>12,.2f} 元\n'
        f'  交易次数:      {n_trades:>6d}\n'
        f'  盈利次数:      {n_win:>6d}\n'
        f'  亏损次数:      {n_lose:>6d}\n'
        f'  胜率:          {win_rate:>7.1f}%\n'
        f'{"=" * 60}'
    )

    return {
        'initial_cash': initial_cash,
        'final_value': round(final_value, 2),
        'total_return_pct': round(total_return_pct, 2),
        'total_profit_amount': round(total_profit_amount, 2),
        'max_drawdown_pct': round(max_drawdown, 2),
        'max_drawdown_date': max_drawdown_date,
        'peak_value': round(peak_value, 2),
        'n_trades': n_trades,
        'n_win': n_win,
        'n_lose': n_lose,
        'win_rate': win_rate,
        'start_date': start_date,
        'end_date': end_date,
        'daily_values': portfolio.portfolio_history,
        'closed_trades': portfolio.closed_trades,
    }


# ==================== 策略参数定义 ====================

STRATEGY_CONFIGS = {
    'turtle_trade': {
        'name': '海龟交易法则',
        'description': '收盘价创60日新高时买入',
        'min_data_days': 60,
        'params': {},
        'use_market_filter': True,   # 趋势追涨，熊市空仓
    },
    'breakthrough_volume': {
        'name': '量能突破',
        'description': '收盘价突破N日高点+放量+MA过滤',
        'min_data_days': 60,
        'params': {
            'break_days': 20,
            'use_volume': True,
            'vol_ratio': 1.5,
            'vol_days': 5,
            'use_ma_filter': True,
            'ma_days': 60,
        },
        'use_market_filter': True,   # 趋势追涨，熊市空仓
    },
    'breakthrough_platform': {
        'name': '突破平台',
        'description': '横盘于MA60附近→放量突破MA60',
        'min_data_days': 120,
        'params': {},
        'use_market_filter': True,   # 趋势追涨，熊市空仓
    },
    'backtrace_ma250': {
        'name': '回踩年线',
        'description': '突破MA250→回踩不破→缩量',
        'min_data_days': 250,
        'params': {},
        'use_market_filter': False,  # 回调低吸，不需牛市
    },
    'keep_increasing': {
        'name': '均线多头',
        'description': 'MA30持续递增+月涨>20%',
        'min_data_days': 60,
        'params': {},
        'use_market_filter': True,   # 趋势追涨，熊市空仓
    },
    'low_atr': {
        'name': '低ATR成长',
        'description': '低波动(日波动<10%)+10天涨>10%',
        'min_data_days': 250,
        'params': {},
        'use_market_filter': False,  # 低波横盘，不需牛市
    },
    'low_backtrace': {
        'name': '无大幅回撤',
        'description': '60天涨60%+无单日跌>7%/连续跌>10%',
        'min_data_days': 60,
        'params': {},
        'use_market_filter': True,   # 趋势追涨，熊市空仓
    },
    'parking_apron': {
        'name': '停机坪',
        'description': '涨停后高位横盘3天',
        'min_data_days': 20,
        'params': {},
        'use_market_filter': False,  # 涨停后横盘，有独立动量
    },
    'high_tight_flag': {
        'name': '高而窄旗形',
        'description': '短期涨90%+连续涨停（回测模式忽略机构条件）',
        'min_data_days': 60,
        'params': {},
        'use_market_filter': True,   # 趋势追涨，熊市空仓
    },
    'climax_limitdown': {
        'name': '放量跌停',
        'description': '跌停+成交额>=2亿+量比>=4（高风险抄底）',
        'min_data_days': 61,
        'params': {},
        'use_market_filter': False,  # 恐慌抄底，不需牛市
    },
    # === ETF 择时策略 ===
    'dual_momentum': {
        'name': '双动量(Dual Momentum)',
        'description': '12月收益>3%买入，<0空仓。ETF择时黄金标准(Gary Antonacci)',
        'min_data_days': 252,
        'params': {'lookback_days': 252},
        'use_market_filter': False,  # 自身含趋势过滤
    },
    'ma200_trend': {
        'name': 'MA200趋势过滤',
        'description': '价格>MA200买入+突破放量确认。最大回撤减半(Siegel/Faber)',
        'min_data_days': 200,
        'params': {},
        'use_market_filter': False,  # MA200自身就是过滤器
    },
    'bollinger_reversion': {
        'name': '布林带均值回归',
        'description': '触及下轨+RSI超卖+缩量买入。适合低波动ETF(Bollinger)',
        'min_data_days': 60,
        'params': {},
        'use_market_filter': False,  # 抄底策略不需牛市
    },
    'vol_targeting': {
        'name': '波动率自适应',
        'description': '趋势向上+年化波动率<40%买入。机构级风控(桥水/潘兴)',
        'min_data_days': 60,
        'params': {},
        'use_market_filter': False,  # 自身含趋势过滤
    },
    'adaptive_momentum': {
        'name': '自适应动量',
        'description': 'MA20>MA50>MA200+20日动量>0+放量。短中长趋势共振',
        'min_data_days': 200,
        'params': {},
        'use_market_filter': False,  # 自身含MA200过滤
    },
    'low_absorption': {
        'name': '低吸共振',
        'description': '同花顺低吸公式：RSI低吸+坚决买进+出击+波段买入+大资金进场 多信号共振买入（抄底型）',
        'min_data_days': 120,
        'params': {'min_signals': 1},
        'use_market_filter': False,  # 抄底策略，熊市也参与（与backtrace_ma250/climax_limitdown同列）
    },
}

# 默认卖出参数
DEFAULT_SELL_PARAMS = {
    'stop_loss': -0.08,              # 不变：27.7%亏损精确落在[-10%,-8%)，是自然断崖
    'stop_profit': 0.20,             # 不变：12.2%交易可达20%+，降到15%会截断大赢家
    'trailing_stop': -0.07,          # 5%→7%：震荡市正常回撤不应触发止损（数据驱动）
    'trailing_stop_activation': 0.08, # 6%→8%：31%盈利卡在[0,3%]区间，因6%激活太早被截断
    'max_hold_days': 60,
    'cooldown_days': 20,  # 卖出后冷却20个交易日，避免同一股票反复割肉
}


# ==================== 选股信号函数（内嵌版） ====================


def _prepare_data(data: pd.DataFrame, end_idx: int) -> pd.DataFrame:
    """从历史数据中截取到 end_idx 为止的部分，方便策略计算"""
    return data.iloc[: end_idx + 1].copy()


# ---- 1. 海龟交易法则 ----
def _signal_turtle_trade(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """收盘价 >= N日最高收盘价，返回突破强度评分"""
    need = kwargs.get('break_days', 60)
    if end_idx < need - 1:
        return None
    window = data.iloc[end_idx - need + 1 : end_idx + 1]
    max_close = window['close'].max()
    last_close = data.iloc[end_idx]['close']
    if last_close < max_close:
        return None
    # 评分：突破幅度(%)，越高越好
    return (last_close / max_close - 1) * 100 if max_close > 0 else 0.0


# ---- 2. 量能突破 ----
def _signal_breakthrough_volume(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """收盘价突破N日高点 + 放量 + MA过滤，返回量价综合评分"""
    break_days = kwargs.get('break_days', 20)
    use_volume = kwargs.get('use_volume', True)
    vol_ratio = kwargs.get('vol_ratio', 1.5)
    vol_days = kwargs.get('vol_days', 5)
    use_ma_filter = kwargs.get('use_ma_filter', True)
    ma_days = kwargs.get('ma_days', 60)

    min_need = max(break_days, vol_days, ma_days) + 1
    if end_idx < min_need:
        return None

    last_close = data.iloc[end_idx]['close']
    last_vol = data.iloc[end_idx]['volume']

    # 突破条件
    prev_high = data['high'].iloc[end_idx - break_days : end_idx].max()
    if last_close < prev_high:
        return None

    # 均线过滤
    if use_ma_filter:
        ma = data['close'].iloc[end_idx - ma_days + 1 : end_idx + 1].mean()
        if last_close < ma:
            return None

    # 放量确认
    vol_ma = data['volume'].iloc[end_idx - vol_days : end_idx].mean()
    if use_volume and (vol_ma <= 0 or last_vol / vol_ma < vol_ratio):
        return None

    # 评分：突破幅度(%) + 量比（截断上限避免极端值主导）
    breakout_pct = (last_close / prev_high - 1) * 100 if prev_high > 0 else 0.0
    vol_score = min(last_vol / vol_ma / vol_ratio * 5, 10) if vol_ma > 0 else 0.0
    return breakout_pct * 0.5 + vol_score


# ---- 3. 突破平台 ----
def _signal_breakthrough_platform(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """横盘于MA60附近 → 放量突破MA60，返回放量强度评分"""
    threshold = 60
    if end_idx < threshold + threshold - 1:
        return None

    # 取最后60天（ma60 已在 backtest_stock 中预计算）
    start = max(0, end_idx - threshold + 1)
    window = data.iloc[start : end_idx + 1].copy()

    # 寻找突破点：open < ma60 <= close
    breakthrough_idx = None
    best_vol_ratio = 0.0
    for j in range(len(window)):
        row = window.iloc[j]
        if pd.notna(row['ma60']) and row['open'] < row['ma60'] <= row['close']:
            # 检查放量（量比>=2）
            vol_5 = window['volume'].iloc[max(0, j - 4) : j + 1].mean()
            if vol_5 > 0 and row['volume'] / vol_5 >= 2:
                breakthrough_idx = j
                best_vol_ratio = row['volume'] / vol_5
                break

    if breakthrough_idx is None:
        return None

    # 突破前所有天：close与ma60偏离在 -5% ~ 20%
    for j in range(breakthrough_idx):
        row = window.iloc[j]
        if pd.isna(row['ma60']) or row['ma60'] <= 0:
            continue
        deviation = (row['ma60'] - row['close']) / row['ma60']
        if not (-0.05 < deviation < 0.2):
            return None

    # 评分：放量倍数越高越好
    return min(best_vol_ratio, 10)


# ---- 4. 回踩年线 ----
def _signal_backtrace_ma250(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """突破MA250 → 回踩不破 → 缩量，返回缩量+回踩深度综合评分"""
    threshold = 60
    if end_idx < 250 - 1:
        return None

    # 取最后60天（ma250 已在 backtest_stock 中预计算）
    start = max(0, end_idx - threshold + 1)
    subset = data.iloc[start : end_idx + 1].copy()

    if len(subset) < threshold:
        return None

    # 找最高价和最低价（向量化）
    highest_idx = subset['close'].idxmax()
    lowest_idx = subset['close'].idxmin()
    highest_close = subset.loc[highest_idx, 'close']
    highest_vol = subset.loc[highest_idx, 'volume']
    highest_date = subset.loc[highest_idx, 'date']
    lowest_close = subset.loc[lowest_idx, 'close']
    lowest_vol = subset.loc[lowest_idx, 'volume']
    lowest_date = subset.loc[lowest_idx, 'date']

    if highest_vol == 0 or lowest_vol == 0:
        return None

    # 前段：最高价日之前
    front = subset[subset['date'] < highest_date]
    if front.empty:
        return None

    # 前段开始close < ma250, 前段结束close > ma250
    first_row = front.iloc[0]
    last_row = front.iloc[-1]
    if pd.isna(first_row['ma250']) or pd.isna(last_row['ma250']):
        return None
    if not (
        first_row['close'] < first_row['ma250']
        and last_row['close'] > last_row['ma250']
    ):
        return None

    # 后段：最高价日及之后（向量化）
    back = subset[subset['date'] >= highest_date]
    # 检查是否跌破年线
    below_ma = back['close'] < back['ma250']
    if below_ma.dropna().any():
        return None
    # 找后段最低收盘价（仅考虑有效ma250的行）
    back_valid = back.dropna(subset=['ma250'])
    if back_valid.empty:
        return None
    lowest_close_idx = back_valid['close'].idxmin()
    recent_lowest_close = back_valid.loc[lowest_close_idx, 'close']
    recent_lowest_vol = back_valid.loc[lowest_close_idx, 'volume']

    # 检查回调时间：10~50天
    try:
        hd = pd.to_datetime(highest_date)
        ld = pd.to_datetime(lowest_date)
        date_diff = (ld - hd).days
    except Exception:
        return None
    if not (10 <= date_diff <= 50):
        return None

    # 缩量回调
    vol_ratio_val = highest_vol / recent_lowest_vol
    back_ratio_val = recent_lowest_close / highest_close
    if not (vol_ratio_val > 2 and back_ratio_val < 0.8):
        return None

    # 评分：缩量倍数 + 回踩幅度（越接近年线不破越好）
    return min(vol_ratio_val / 2 * 3, 10) + min((1 - back_ratio_val) * 15, 5)


# ---- 5. 均线多头 ----
def _signal_keep_increasing(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """MA持续递增趋势 + 最小涨幅过滤，返回超额涨幅评分"""
    ma_days = kwargs.get('ma_days', 30)
    min_increase = kwargs.get('min_increase', 0.20)
    threshold = max(ma_days, 10)  # 至少需要 10 天数据

    if end_idx < threshold - 1:
        return None

    # 计算 MA
    close_arr = data['close'].values
    ma_arr = np.full(len(close_arr), np.nan)
    for i in range(ma_days - 1, len(close_arr)):
        ma_arr[i] = close_arr[i - ma_days + 1 : i + 1].mean()

    start = max(0, end_idx - threshold + 1)
    ma_values = ma_arr[start : end_idx + 1]
    if len(ma_values) < threshold or np.any(pd.isna(ma_values)):
        return None

    step1 = round(threshold / 3)
    step2 = round(threshold * 2 / 3)

    required_multiplier = 1 + min_increase
    if not (
        ma_values[0] < ma_values[step1] < ma_values[step2] < ma_values[-1]
        and ma_values[-1] > required_multiplier * ma_values[0]
    ):
        return None

    # 评分：超额涨幅（超过 min_increase 的程度）
    total_increase = ma_values[-1] / max(ma_values[0], 0.01) - 1
    return max(total_increase - min_increase, 0) * 50


# ---- 6. 低ATR成长 ----
def _signal_low_atr(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """低波动 + 10天涨>10%（修正ratio bug），返回波动稳定性+涨幅评分"""
    threshold = 10
    if end_idx < 250 - 1:
        return None

    # 取最后10天
    start = end_idx - threshold + 1
    subset = data.iloc[start : end_idx + 1]
    if len(subset) < threshold:
        return None

    highest_close = 0
    lowest_close = 1e9
    total_change = 0.0
    days_count = len(subset)

    for _, row in subset.iterrows():
        pchg = row.get('p_change', 0)
        if pd.isna(pchg):
            pchg = 0
        total_change += abs(pchg)
        if row['close'] > highest_close:
            highest_close = row['close']
        if row['close'] < lowest_close:
            lowest_close = row['close']

    atr = total_change / days_count
    if atr > 10:
        return None

    ratio = (highest_close - lowest_close) / lowest_close if lowest_close > 0 else 0
    # 修正原策略bug：ratio > 1.1 改为 ratio > 0.1
    if ratio <= 0.1:
        return None

    # 评分：波动越低 + 涨幅越大 = 评分越高
    stability_score = max(10 - atr, 0)  # ATR越低越好
    momentum_score = min((ratio - 0.1) * 50, 10)  # 涨幅越高越好
    return stability_score * 0.5 + momentum_score


# ---- 7. 无大幅回撤 ----
def _signal_low_backtrace(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """60天涨60% + 无单日跌>7%/连续跌>10%，返回涨幅评分"""
    threshold = 60
    if end_idx < threshold - 1:
        return None

    start = end_idx - threshold + 1
    subset = data.iloc[start : end_idx + 1]
    if len(subset) < threshold:
        return None

    # 60日涨幅 >= 60%
    ratio_increase = (subset.iloc[-1]['close'] - subset.iloc[0]['close']) / subset.iloc[
        0
    ]['close']
    if ratio_increase < 0.6:
        return None

    # 检查无大幅回撤
    prev_p_change = 100.0
    prev_open = -1e6

    for _, row in subset.iterrows():
        pchg = row.get('p_change', 0)
        if pd.isna(pchg):
            pchg = 0
        close_p = row['close']
        open_p = row['open']

        # 单日跌幅 > 7%
        if pchg < -7:
            return None
        # 高开低走 > 7%
        if open_p > 0 and (close_p - open_p) / open_p * 100 < -7:
            return None
        # 两日累计跌 > 10%
        if prev_p_change + pchg < -10:
            return None
        # 两日高开低走累计 > 10%
        if prev_open > 0 and (close_p - prev_open) / prev_open * 100 < -10:
            return None

        prev_p_change = pchg
        prev_open = open_p

    # 评分：涨幅越大越好（截断上限）
    return min(ratio_increase * 10, 20) / 2


# ---- 8. 停机坪 ----
def _signal_parking_apron(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """涨停后高位横盘3天，返回涨停强度+放量确认评分"""
    threshold = 15
    if end_idx < threshold:
        return None

    start = max(0, end_idx - threshold + 1)
    subset = data.iloc[start : end_idx + 1]
    if len(subset) < threshold:
        return None

    # 需要 p_change 列
    if 'p_change' not in subset.columns:
        return None

    best_score = 0.0

    for i in range(len(subset)):
        row = subset.iloc[i]
        pchg = row.get('p_change', 0)
        if pd.isna(pchg) or pchg <= 9.5:
            continue

        # 涨停日放量确认：涨停日成交量 > 前5日均量 * 1.5
        vol_start = max(0, i - 4)
        if i - vol_start < 2:
            continue
        avg_vol = subset['volume'].iloc[vol_start:i].mean()
        if avg_vol <= 0 or row['volume'] / avg_vol < 1.5:
            continue

        limitup_price = row['close']
        limitup_date = row['date']

        # 涨停后至少3天
        if i + 4 >= len(subset):
            continue

        # 涨停后第1天
        d1 = subset.iloc[i + 1]
        if not (d1['close'] > limitup_price and d1['open'] > limitup_price):
            continue
        if d1['open'] <= 0:
            continue
        if not (0.97 < d1['close'] / d1['open'] < 1.03):
            continue

        # 涨停后第2-3天
        ok = True
        for k in range(i + 2, i + 4):
            dk = subset.iloc[k]
            dk_pchg = dk.get('p_change', 0)
            if pd.isna(dk_pchg):
                dk_pchg = 0
            if dk['open'] <= 0:
                ok = False
                break
            if not (
                0.97 < dk['close'] / dk['open'] < 1.03
                and -5 < dk_pchg < 5
                and dk['close'] > limitup_price
                and dk['open'] > limitup_price
            ):
                ok = False
                break

        if ok:
            # 评分：涨停强度（pchg） + 放量确认
            vol_confirm = min(row['volume'] / max(avg_vol, 1), 5)
            pchg_score = min(pchg - 9.5, 5)
            best_score = max(best_score, vol_confirm + pchg_score)

    if best_score > 0:
        return best_score
    return None


# ---- 9. 高而窄旗形 ----
def _signal_high_tight_flag(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """短期涨90%+连续涨停，返回涨幅强度评分"""
    threshold = 60
    if end_idx < threshold - 1:
        return False

    if 'p_change' not in data.columns:
        return False

    start = max(0, end_idx - threshold + 1)
    subset = data.iloc[start : end_idx + 1]
    if len(subset) < threshold:
        return None

    # 取倒数24~10天（共14天）
    tail24 = subset.tail(24)
    head14 = tail24.head(14)
    if len(head14) < 14:
        return None

    low = head14['low'].min()
    if low <= 0:
        return None
    ratio_increase = head14.iloc[-1]['high'] / low
    if ratio_increase < 1.9:
        return None

    # 连续涨停
    prev_pchg = 0.0
    for _, row in head14.iterrows():
        pchg = row.get('p_change', 0)
        if pd.isna(pchg):
            pchg = 0
        if pchg >= 9.5:
            if prev_pchg >= 9.5:
                # 评分：涨幅倍数越高越好
                return min((ratio_increase - 1.9) * 5, 10)
            else:
                prev_pchg = pchg
        else:
            prev_pchg = 0.0

    return None


# ---- 10. 放量跌停 ----
def _signal_climax_limitdown(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """跌停 + 成交额>=2亿 + 量比>=4，返回量能强度评分"""
    if end_idx < 5:
        return None

    if 'p_change' not in data.columns:
        return None

    row = data.iloc[end_idx]
    pchg = row.get('p_change', 0)
    if pd.isna(pchg) or pchg > -9.5:
        return None

    # 成交额 >= 2亿
    amount = row['close'] * row['volume']
    if amount < 200000000:
        return None

    # 量比 >= 4（当日量 / 5日均量）
    if end_idx < 5:
        return None
    vol_5 = data['volume'].iloc[end_idx - 4 : end_idx + 1].mean()
    if vol_5 <= 0:
        return None
    vol_ratio = row['volume'] / vol_5
    if vol_ratio < 4:
        return None

    # 评分：成交额越大 + 量比越高 = 恐慌越极端（反转潜力越大）
    amount_score = min(amount / 1e8 / 5 * 5, 5)
    vol_score = min(vol_ratio / 4 * 5, 5)
    return amount_score + vol_score


# ---- ETF 择时策略信号 ----


def _signal_dual_momentum(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """双动量：12月收益 > 3% 即买入，得分=收益率"""
    need = kwargs.get('lookback_days', 252)
    if end_idx < need:
        return None
    start_close = data.iloc[end_idx - need + 1]['open']
    end_close = data.iloc[end_idx]['close']
    if start_close <= 0:
        return None
    ret = (end_close - start_close) / start_close * 100
    if ret <= 3.0:
        return None
    return ret


def _signal_ma200_trend(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """MA200趋势过滤：价格>MA200 + 突破放量确认，得分=偏离百分比"""
    need = 200
    if end_idx < need:
        return None
    close = data.iloc[end_idx]['close']
    ma200 = data['close'].iloc[end_idx - need + 1 : end_idx + 1].mean()
    if close <= ma200 or ma200 <= 0:
        return None
    prev_close = data.iloc[end_idx - 1]['close']
    prev_ma200 = data['close'].iloc[end_idx - need : end_idx].mean()
    if prev_close < prev_ma200:
        vol = data.iloc[end_idx]['volume']
        vol_ma20 = data['volume'].iloc[end_idx - 20 : end_idx].mean()
        if vol_ma20 > 0 and vol < vol_ma20 * 1.2:
            return None  # 无量突破
    return (close / ma200 - 1) * 100


def _signal_bollinger_reversion(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """布林带均值回归：触及下轨 + RSI超卖 + 缩量，得分=10-偏离下轨百分比"""
    if end_idx < 60:
        return None
    window = data.iloc[end_idx - 19 : end_idx + 1]
    close = window.iloc[-1]['close']
    ma20 = window['close'].mean()
    std20 = window['close'].std()
    if ma20 <= 0:
        return None
    lower = ma20 - 2 * std20
    if close > lower * 1.02:
        return None

    # 简易 RSI(14)
    rsi_window = data.iloc[end_idx - 13 : end_idx + 1]
    gains = rsi_window['close'].diff().clip(lower=0).iloc[1:]
    losses = (-rsi_window['close'].diff().clip(upper=0)).iloc[1:]
    avg_gain = gains.mean() if len(gains) > 0 else 0
    avg_loss = losses.mean() if len(losses) > 0 else 1e-9
    rs = avg_gain / avg_loss if avg_loss > 0 else 100
    rsi = 100 - 100 / (1 + rs)
    if rsi >= 35:
        return None

    vol = window.iloc[-1]['volume']
    vol_ma20 = data['volume'].iloc[end_idx - 20 : end_idx].mean()
    if vol_ma20 > 0 and vol > vol_ma20 * 0.8:
        return None

    return (ma20 / close - 1) * 100  # 偏离中轨越远，得分越高


def _signal_vol_targeting(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """波动率自适应：趋势向上 + 年化波动率<40%"""
    if end_idx < 60:
        return None
    close = data.iloc[end_idx]['close']
    ma60 = data['close'].iloc[end_idx - 59 : end_idx + 1].mean()
    if close <= ma60 or ma60 <= 0:
        return None

    # 简易 ATR(14)
    atr_window = data.iloc[end_idx - 13 : end_idx + 1]
    trs = []
    for i in range(1, len(atr_window)):
        h = atr_window.iloc[i]['high']; l = atr_window.iloc[i]['low']
        pc = atr_window.iloc[i - 1]['close']
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr14 = sum(trs) / len(trs) if trs else 0
    if atr14 <= 0:
        return None
    annual_vol = (atr14 / close) * (252 ** 0.5)
    if annual_vol > 0.40:
        return None
    # 得分：波动率越低越好（反转思路：低波动入场更安全）
    return (0.40 - annual_vol) * 25


def _signal_adaptive_momentum(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """自适应动量：MA20>MA50>MA200 + 20日动量>0 + 放量"""
    if end_idx < 200:
        return None
    close = data.iloc[end_idx]['close']
    ma20 = data['close'].iloc[end_idx - 19 : end_idx + 1].mean()
    ma50 = data['close'].iloc[end_idx - 49 : end_idx + 1].mean()
    ma200 = data['close'].iloc[end_idx - 199 : end_idx + 1].mean()

    if ma20 <= ma50 or ma50 <= ma200:
        return None
    if close <= ma50:
        return None
    # 20日动量
    close_20d_ago = data.iloc[end_idx - 19]['close']
    if close_20d_ago <= 0 or close <= close_20d_ago:
        return None
    # 放量
    vol = data.iloc[end_idx]['volume']
    vol_ma20 = data['volume'].iloc[end_idx - 20 : end_idx].mean()
    if vol_ma20 > 0 and vol < vol_ma20:
        return None
    momentum = (close / close_20d_ago - 1) * 100
    return momentum


# ---- 低吸共振（同花顺低吸公式复现）----


def _sma(series: pd.Series, n: int, m: int) -> pd.Series:
    """同花顺 SMA(X, N, M) = (M*X_t + (N-M)*SMA_{t-1}) / N，首个有效值作种子，跳过前导NaN"""
    vals = series.to_numpy(dtype=float)
    out = np.full(len(vals), np.nan)
    prev = np.nan
    started = False
    for i in range(len(vals)):
        x = vals[i]
        if np.isnan(x):
            if started:
                out[i] = prev
            continue
        if not started:
            prev = x
            out[i] = x
            started = True
        else:
            prev = (m * x + (n - m) * prev) / n
            out[i] = prev
    return pd.Series(out, index=series.index)


def _filter_signal(bool_series: pd.Series, n: int) -> pd.Series:
    """同花顺 FILTER(X, N)：信号X为真且距上次触发超过N根K线才保留（N周期内只保留首个）"""
    arr = bool_series.to_numpy()
    out = np.zeros(len(arr), dtype=bool)
    last = -n - 1
    for i in range(len(arr)):
        if arr[i] and (i - last) > n:
            out[i] = True
            last = i
    return pd.Series(out, index=bool_series.index)


def _ema(series: pd.Series, span: int) -> pd.Series:
    """同花顺 EMA(X, N) = (2*X_t + (N-1)*EMA_{t-1})/(N+1)，忽略前导NaN继续递推"""
    return series.ewm(span=span, adjust=False, ignore_na=True).mean()


def _compute_low_absorption_indicators(data: pd.DataFrame) -> pd.DataFrame:
    """
    复现同花顺低吸公式的全部中间指标，返回与 data 等长的指标表。
    一次性计算，供 _signal_low_absorption 按 end_idx 直接取用，避免在逐日扫描中 O(len^2) 重复计算。
    """
    close = data['close']
    high = data['high']
    low = data['low']
    open_ = data['open']
    volume = data['volume']

    # --- KDJ ---
    low9 = low.rolling(9, min_periods=9).min()
    high9 = high.rolling(9, min_periods=9).max()
    denom9 = high9 - low9
    rsv = ((close - low9) / denom9 * 100).where(denom9 > 0, 50.0)
    k = _sma(rsv, 3, 1)
    d = _sma(k, 3, 1)
    j = 3 * k - 2 * d

    # --- RSI(6) 低吸 ---
    delta = close - close.shift(1)
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    sma_loss = _sma(loss, 6, 1)
    rsi = (_sma(gain, 6, 1) / sma_loss * 100).where(sma_loss > 0, 50.0)

    # --- B/B1 动能 ---
    varv = (2 * close + high + low) / 4
    varu = low.rolling(30, min_periods=30).min()
    vara1 = high.rolling(30, min_periods=30).max()
    bbase = ((varv - varu) / (vara1 - varu) * 100).where(vara1 > varu, 50.0)
    b = _ema(bbase, 8)
    b1 = _ema(b, 5)

    # --- 坚决买进：VAR7=EMA(成交均价,3)（同花顺 AMOUNT/VOL 即均价≈收盘价），VAR8=EMA(88)，VARA=0.87*VAR8 ---
    var7 = _ema(close, 3)
    var8 = _ema(var7, 88)
    vara = var8 * 0.87
    varb = (low < vara) & (close > close.shift(1) * 1.02)
    坚决买进 = _filter_signal(varb, 6)

    # --- 出击 ---
    varf = (2 * close + high + low) / 4
    llv34 = low.rolling(34, min_periods=34).min()
    hhv34 = high.rolling(34, min_periods=34).max()
    va6 = _ema(((varf - llv34) / (hhv34 - llv34) * 100).where(hhv34 > llv34, 50.0), 6)
    va7 = _ema(0.667 * va6.shift(1) + 0.333 * va6, 4)
    va8_p1 = (close < close.shift(1)).rolling(8, min_periods=8).sum() / 8 > 0.3
    va8_p2 = (va6 > va7).rolling(3, min_periods=3).sum() >= 1
    llv120 = low.rolling(120, min_periods=120).min()
    va8_p3 = ((low.shift(1) - llv120).abs() / llv120) < 0.1
    va8_p4 = close > open_
    chuji = va8_p1 & va8_p2 & va8_p3 & va8_p4

    # --- 波段买入 ---
    a = (3 * close + low + open_ + high) / 6
    d1_terms = [a.shift(k) * (20 - k) for k in range(0, 19)]  # REF(A,0)..REF(A,18)
    d1_terms.append(a.shift(20) * 1)  # REF(A,20) 权重1（原公式跳过 REF(A,19)）
    d1 = sum(d1_terms) / 211.0
    d2 = _ema(d1, 2)
    d3 = _ema(d2, 2)
    k1 = _ema(d3, 2)

    # --- 大资金进场（VAR10/VAR11）---
    llv75 = low.rolling(75, min_periods=75).min()
    hhv75 = high.rolling(75, min_periods=75).max()
    rng75 = (hhv75 - llv75)
    x75 = ((close - llv75) / rng75 * 100).where(rng75 > 0, 50.0)
    x75o = ((open_ - llv75) / rng75 * 100).where(rng75 > 0, 50.0)
    var10 = 100 - 3 * _sma(x75, 20, 1) + 2 * _sma(_sma(x75, 20, 1), 15, 1)
    var11 = 100 - 3 * _sma(x75o, 20, 1) + 2 * _sma(_sma(x75o, 20, 1), 15, 1)
    var12 = (var10 < var11.shift(1)) & (volume > volume.shift(1)) & (close > close.shift(1))
    大资金进场 = var12 & (var12.rolling(30, min_periods=1).sum() == 1)

    return pd.DataFrame(
        {
            'k': k,
            'd': d,
            'j': j,
            'rsi': rsi,
            'b': b,
            'b1': b1,
            'resolute': 坚决买进,
            'chuji': chuji,
            'd1': d1,
            'k1': k1,
            'big_money': 大资金进场,
        },
        index=data.index,
    )


# 进程内缓存：同一只股票的 data 对象在多次信号调用间复用，避免 O(len^2) 重复计算
_LA_CACHE = {'id': None, 'df': None}


def _signal_low_absorption(data: pd.DataFrame, end_idx: int, **kwargs) -> Optional[float]:
    """
    同花顺「低吸」公式复现 —— 多信号共振买入（抄底型）。

    买入信号（满足 min_signals 个即买入，评分=信号权重之和，越高越优先）：
      - 低吸:     RSI(6) 上穿 20 且单日跳升 >10
      - 坚决买进: 价格跌破长期成本线*0.87 后次日放量回升 >2%
      - 出击:     近8日偏跌 + VA6>VA7 + 贴近120日最低 + 收阳
      - 波段买入: 加权均价 D1 上穿其 EMA 链 K1
      - 大资金进场: VAR10<昨VAR11 + 放量 + 收涨（30日内首次）
    卖出沿用框架统一风控（止损/止盈/移动止损/最大持仓）。
    """
    min_signals = kwargs.get('min_signals', 1)
    need = 120  # 出击需 LLV(LOW,120)
    if end_idx < need - 1:
        return None

    cid = id(data)
    if _LA_CACHE['id'] != cid:
        _LA_CACHE['id'] = cid
        _LA_CACHE['df'] = _compute_low_absorption_indicators(data)
    ind = _LA_CACHE['df']

    i = end_idx

    # 低吸：RSI(6) 上穿 20 且单日跳升 >10
    low_xi = False
    rsi_i = ind['rsi'].iloc[i]
    if i >= 1 and pd.notna(rsi_i) and pd.notna(ind['rsi'].iloc[i - 1]):
        if rsi_i > 20 and ind['rsi'].iloc[i - 1] <= 20 and (rsi_i - ind['rsi'].iloc[i - 1]) > 10:
            low_xi = True

    # 坚决买进
    resolute = bool(ind['resolute'].iloc[i]) if pd.notna(ind['resolute'].iloc[i]) else False
    # 出击
    chuji = bool(ind['chuji'].iloc[i]) if pd.notna(ind['chuji'].iloc[i]) else False

    # 波段买入：D1 上穿 K1
    band = False
    d1_i = ind['d1'].iloc[i]
    k1_i = ind['k1'].iloc[i]
    if (
        i >= 1
        and pd.notna(d1_i)
        and pd.notna(k1_i)
        and pd.notna(ind['d1'].iloc[i - 1])
        and pd.notna(ind['k1'].iloc[i - 1])
    ):
        if d1_i > k1_i and ind['d1'].iloc[i - 1] <= ind['k1'].iloc[i - 1]:
            band = True

    # 大资金进场
    big = bool(ind['big_money'].iloc[i]) if pd.notna(ind['big_money'].iloc[i]) else False

    score = 0.0
    if low_xi:
        score += 2.0
    if resolute:
        score += 4.0
    if chuji:
        score += 3.0
    if band:
        score += 2.0
    if big:
        score += 3.0

    n = int(low_xi) + int(resolute) + int(chuji) + int(band) + int(big)
    if n >= min_signals and score > 0:
        return float(score)
    return None


# 策略名 → 信号函数映射
SIGNAL_FUNCTIONS: Dict[str, Callable] = {
    'turtle_trade': _signal_turtle_trade,
    'breakthrough_volume': _signal_breakthrough_volume,
    'breakthrough_platform': _signal_breakthrough_platform,
    'backtrace_ma250': _signal_backtrace_ma250,
    'keep_increasing': _signal_keep_increasing,
    'low_atr': _signal_low_atr,
    'low_backtrace': _signal_low_backtrace,
    'parking_apron': _signal_parking_apron,
    'high_tight_flag': _signal_high_tight_flag,
    'climax_limitdown': _signal_climax_limitdown,
    # ETF 择时策略
    'dual_momentum': _signal_dual_momentum,
    'ma200_trend': _signal_ma200_trend,
    'bollinger_reversion': _signal_bollinger_reversion,
    'vol_targeting': _signal_vol_targeting,
    'adaptive_momentum': _signal_adaptive_momentum,
    'low_absorption': _signal_low_absorption,
}


# ==================== 卖出逻辑 ====================


def check_sell_signal(
    buy_price: float,
    current_close: float,
    current_high: float,
    hold_days: int,
    max_high_since_buy: float,
    stop_loss: float = -0.08,
    stop_profit: float = 0.20,
    trailing_stop: float = -0.05,
    trailing_stop_activation: float = 0.06,
    max_hold_days: int = 60,
    **kwargs,  # 接收 cooldown_days 等不参与卖出判断的参数
) -> Tuple[bool, str]:
    """
    检查是否触发卖出条件
    返回: (是否卖出, 卖出原因)
    """
    profit_pct = (current_close - buy_price) / buy_price

    # 1. 止损
    if profit_pct <= stop_loss:
        return True, 'stop_loss'

    # 2. 止盈
    if profit_pct >= stop_profit:
        return True, 'stop_profit'

    # 3. 移动止损：从持仓期间最高点回落
    max_profit_from_high = (
        (current_close - max_high_since_buy) / max_high_since_buy
        if max_high_since_buy > 0
        else 0
    )
    if max_high_since_buy > buy_price * (1 + trailing_stop_activation):  # 移动止损激活阈值
        if max_profit_from_high <= trailing_stop:
            return True, 'trailing_stop'

    # 4. 最大持仓天数
    if hold_days >= max_hold_days:
        return True, 'max_hold'

    return False, ''


# ==================== 市场宽度 ====================


def compute_market_breadth(
    kline_dict: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    ma_days: int = 60,
    bull_threshold: float = 0.40,
) -> Dict[str, bool]:
    """
    计算每日市场宽度（%股票在MA60之上），用于牛熊判断。

    参数:
        kline_dict: {code: DataFrame} 所有股票K线
        start_date: 回测起始日期
        end_date: 回测结束日期
        ma_days: 均线周期（默认60日）
        bull_threshold: 牛市阈值（MA上方股票占比>=此值视为牛市，默认40%）

    返回:
        {date_str: is_bull} 字典，True表示当日市场偏强，允许买入
    """
    from collections import defaultdict

    date_counts = defaultdict(lambda: {'total': 0, 'above': 0})
    processed = 0
    total_stocks = len(kline_dict)

    logging.info(f'  📊 正在计算市场宽度 (MA{ma_days}上方占比，阈值{bull_threshold*100:.0f}%)...')

    for code, df in kline_dict.items():
        if df is None or len(df) < ma_days:
            continue

        data = df.copy()
        data = data.sort_values('date')

        # 计算MA列（利用预计算的ma60，没有则临时算）
        ma_col = f'ma{ma_days}'
        if ma_col not in data.columns:
            data[ma_col] = data['close'].rolling(ma_days, min_periods=ma_days).mean()

        # 过滤回测日期范围
        mask = (data['date'] >= start_date) & (data['date'] <= end_date)
        data = data.loc[mask]

        for _, row in data.iterrows():
            date = row['date']
            ma_val = row[ma_col]
            if pd.notna(ma_val) and ma_val > 0:
                date_counts[date]['total'] += 1
                if row['close'] > ma_val:
                    date_counts[date]['above'] += 1

        processed += 1
        if processed % 500 == 0:
            logging.info(f'  📊 市场宽度计算进度: {processed}/{total_stocks} 只股票...')

    # 计算每日宽度比例
    result = {}
    bull_days = 0
    total_days = 0

    for date in sorted(date_counts.keys()):
        counts = date_counts[date]
        if counts['total'] > 0:
            ratio = counts['above'] / counts['total']
            is_bull = ratio >= bull_threshold
            result[date] = is_bull
            total_days += 1
            if is_bull:
                bull_days += 1

    if total_days > 0:
        pct = bull_days / total_days * 100
        logging.info(
            f'  📊 市场宽度统计: {total_days}个交易日, '
            f'牛市{bull_days}天({pct:.1f}%), '
            f'熊市{total_days - bull_days}天({100 - pct:.1f}%), '
            f'阈值={bull_threshold * 100:.0f}%'
        )

    return result


# ==================== 单只股票回测 ====================


def backtest_stock(
    code: str,
    name: str,
    kline: pd.DataFrame,
    start_date: str,
    end_date: str,
    strategy_id: str,
    signal_func: Callable,
    signal_params: dict,
    sell_params: dict,
    market_breadth: Dict[str, bool] | None = None,
) -> List[Trade]:
    """
    对单只股票使用指定策略进行完整回测

    逐日扫描：
    - 每天检查买入信号
    - 持仓后每天检查卖出信号
    - 卖出后可再次买入
    """
    trades: List[Trade] = []
    min_data = STRATEGY_CONFIGS.get(strategy_id, {}).get('min_data_days', 60)

    if kline is None or len(kline) < min_data:
        return trades

    data = kline.copy()
    data = data.sort_values('date').reset_index(drop=True)

    # 预计算均线列（在过滤前，利用缓冲区数据保证 MA 值有效）
    data['ma60'] = data['close'].rolling(60, min_periods=60).mean()
    data['ma250'] = data['close'].rolling(250, min_periods=250).mean()

    # 过滤回测时间范围
    mask = (data['date'] >= start_date) & (data['date'] <= end_date)
    data = data.loc[mask].reset_index(drop=True)

    if len(data) < min_data:
        return trades

    current_trade: Optional[Trade] = None
    max_high_since_buy: float = 0.0
    min_low_since_buy: float = float('inf')
    hold_day_counter: int = 0
    pending_buy_score: Optional[float] = None  # 待买入信号的评分（None=无待买入）
    pending_sell: bool = False  # 标记是否需要延迟一天卖出
    pending_sell_reason: str = ''
    last_sell_index: int = -999  # 上次卖出的数据行索引，用于冷却期判断
    cooldown_days: int = sell_params.get('cooldown_days', 20)

    # 自适应移动止损：牛市放宽让利润跑，熊市收紧保命
    bull_trailing_activation = sell_params.get('trailing_stop_activation', 0.08)
    bull_trailing_stop = sell_params.get('trailing_stop', -0.07)
    bear_trailing_activation = 0.06  # 熊市收紧：涨6%就激活
    bear_trailing_stop = -0.05       # 熊市收紧：回撤5%就离场
    stop_loss = sell_params.get('stop_loss', -0.08)
    stop_profit = sell_params.get('stop_profit', 0.20)
    max_hold_days = sell_params.get('max_hold_days', 60)

    scan_start = min_data  # 从第 min_data 天开始扫描

    for i in range(scan_start, len(data)):
        today_date = data.iloc[i]['date']
        today_open = data.iloc[i]['open']
        today_high = data.iloc[i]['high']
        today_low = data.iloc[i]['low']
        today_close = data.iloc[i]['close']

        # ========== 1. 执行待卖出订单（延迟一天）==========
        if pending_sell and current_trade is not None:
            # 在前一天触发卖出信号，今天以开盘价卖出
            sell_price = today_open
            current_trade.sell_date = today_date
            current_trade.sell_price = sell_price
            current_trade.sell_reason = pending_sell_reason
            current_trade.profit_pct = (
                (sell_price - current_trade.buy_price) / current_trade.buy_price * 100
            )
            current_trade.hold_days = hold_day_counter
            current_trade.max_profit_pct = (
                (max_high_since_buy - current_trade.buy_price)
                / current_trade.buy_price
                * 100
            )
            current_trade.max_loss_pct = (
                (min_low_since_buy - current_trade.buy_price)
                / current_trade.buy_price
                * 100
            )

            trades.append(current_trade)
            current_trade = None
            max_high_since_buy = 0.0
            min_low_since_buy = float('inf')
            hold_day_counter = 0
            pending_sell = False
            pending_sell_reason = ''
            last_sell_index = i  # 记录卖出位置，用于冷却期判断
            continue  # 卖出后不再检查买入

        # ========== 2. 执行待买入订单（延迟一天）==========
        if pending_buy_score is not None and current_trade is None:
            # 跳空熔断：开盘价相对昨日收盘低开 >5%，取消买入
            if i > 0:
                yesterday_close = data.iloc[i - 1]['close']
                if yesterday_close > 0:
                    gap_pct = (today_open - yesterday_close) / yesterday_close
                    if gap_pct < -0.05:
                        pending_buy_score = None
                        continue  # 跳空过大，取消买入

            # 在前一天触发买入信号，今天以开盘价买入
            buy_price = today_open
            current_trade = Trade(code, name, today_date, buy_price, pending_buy_score)
            max_high_since_buy = today_high
            min_low_since_buy = today_low
            hold_day_counter = 0
            pending_buy_score = None
            continue  # 买入后不再检查卖出

        # ========== 3. 持仓状态：检查卖出信号 ==========
        if current_trade is not None:
            hold_day_counter += 1
            max_high_since_buy = max(max_high_since_buy, today_high)
            min_low_since_buy = min(min_low_since_buy, today_low)

            # 使用今天的数据检查卖出信号（实际卖出在明天）
            # 自适应：熊市收紧移动止损参数
            if market_breadth is not None and market_breadth:
                is_bull = market_breadth.get(today_date, True)
            else:
                is_bull = True

            ts_activation = bull_trailing_activation if is_bull else bear_trailing_activation
            ts = bull_trailing_stop if is_bull else bear_trailing_stop

            should_sell, reason = check_sell_signal(
                buy_price=current_trade.buy_price,
                current_close=today_close,
                current_high=today_high,
                hold_days=hold_day_counter,
                max_high_since_buy=max_high_since_buy,
                stop_loss=stop_loss,
                stop_profit=stop_profit,
                trailing_stop=ts,
                trailing_stop_activation=ts_activation,
                max_hold_days=max_hold_days,
            )

            if should_sell:
                pending_sell = True
                pending_sell_reason = reason
                continue  # 延迟到明天卖出

        # ========== 4. 空仓状态：检查买入信号 ==========
        if current_trade is None and pending_buy_score is None and not pending_sell:
            # 冷却期检查：卖出后N个交易日内不重新买入同一股票
            if cooldown_days > 0 and last_sell_index > 0 and (i - last_sell_index) <= cooldown_days:
                continue

            # 市场环境过滤：熊市不买入
            if market_breadth is not None and market_breadth:
                if not market_breadth.get(today_date, True):
                    continue  # 市场弱势，跳过所有买入信号

            # 使用今天的数据计算信号，明天以开盘价买入
            score = signal_func(data, i, **signal_params)
            if score is not None:
                # 兼容旧版布尔返回（但新版已全部改为 Optional[float]）
                pending_buy_score = float(score) if not isinstance(score, bool) else 0.0
                continue  # 延迟到明天买入

    # ========== 回测结束，强制平仓 ==========
    # 情况1：有 pending_sell（在最后一天触发了卖出信号）
    if pending_sell and current_trade is not None:
        # 以最后一天的开盘价卖出
        last_date = data.iloc[-1]['date']
        last_open = data.iloc[-1]['open']

        current_trade.sell_date = last_date
        current_trade.sell_price = last_open
        current_trade.sell_reason = 'data_end'
        current_trade.profit_pct = (
            (last_open - current_trade.buy_price) / current_trade.buy_price * 100
        )
        current_trade.hold_days = hold_day_counter
        current_trade.max_profit_pct = (
            (max_high_since_buy - current_trade.buy_price)
            / current_trade.buy_price
            * 100
        )
        current_trade.max_loss_pct = (
            (min_low_since_buy - current_trade.buy_price)
            / current_trade.buy_price * 100
        )

        trades.append(current_trade)
        current_trade = None  # 避免重复添加

    # 情况2：还有持仓，但没有 pending_sell
    if current_trade is not None:
        last_date = data.iloc[-1]['date']
        last_open = data.iloc[-1]['open']

        current_trade.sell_date = last_date
        current_trade.sell_price = last_open
        current_trade.sell_reason = 'data_end'
        current_trade.profit_pct = (
            (last_open - current_trade.buy_price) / current_trade.buy_price * 100
        )
        current_trade.hold_days = hold_day_counter
        current_trade.max_profit_pct = (
            (max_high_since_buy - current_trade.buy_price)
            / current_trade.buy_price
            * 100
        )
        current_trade.max_loss_pct = (
            (min_low_since_buy - current_trade.buy_price)
            / current_trade.buy_price * 100
        ) if min_low_since_buy != float('inf') else current_trade.profit_pct

        trades.append(current_trade)

    # 如果还有待买入的，取消买入（回测结束）
    if pending_buy_score is not None:
        pending_buy_score = None

    return trades


# ==================== 批量回测 ====================


def _backtest_single_stock(args: Tuple) -> List[Trade]:
    """
    单只股票回测的包装函数（用于并行处理）

    参数:
        args: (code, kline_dict[code], strategy_id, signal_params, sell_params,
               start_date, end_date, market_breadth)

    返回:
        该股票的回测交易列表
    """
    code, kline, strategy_id, signal_params, sell_params, start_date, end_date, market_breadth = args

    # 在工作进程中获取信号函数（避免传递函数引用）
    signal_func = SIGNAL_FUNCTIONS.get(strategy_id)
    if signal_func is None:
        return []

    name = '未知'
    if 'name' in kline.columns and len(kline) > 0:
        name_val = kline.iloc[-1]['name']
        name = str(name_val) if pd.notna(name_val) else '未知'

    try:
        trades = backtest_stock(
            code=code,
            name=name,
            kline=kline,
            start_date=start_date,
            end_date=end_date,
            strategy_id=strategy_id,
            signal_func=signal_func,
            signal_params=signal_params,
            sell_params=sell_params,
            market_breadth=market_breadth,
        )
        return trades
    except Exception as e:
        logging.debug(f'  ⚠️ {code} 回测异常: {e}')
        return []


def run_backtest(
    kline_dict: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    strategy_id: str = 'turtle_trade',
    sell_params: dict[str, int | float] | None = None,
    n_workers: int | None = None,
    enable_market_filter: bool = True,
    market_breadth: Dict[str, bool] | None = None,
) -> List[Trade]:
    """
    批量回测所有股票（使用指定策略）

    参数:
        kline_dict: {code: DataFrame} 所有股票K线数据
        start_date: 回测起始日期
        end_date: 回测结束日期
        strategy_id: 策略ID（见 STRATEGY_CONFIGS 的 key）
        sell_params: 卖出参数，None则使用默认值
        n_workers: 并行工作进程数，None则使用CPU核心数-1
        enable_market_filter: 是否启用市场宽度过滤（熊市空仓）
        market_breadth: 预计算的市场宽度，None则内部计算（多策略对比时传入可避免重复计算）
    """
    if sell_params is None:
        sell_params = DEFAULT_SELL_PARAMS.copy()

    config = STRATEGY_CONFIGS.get(strategy_id)
    if config is None:
        logging.error(f'  ❌ 未知策略: {strategy_id}')
        return []

    signal_params = config.get('params', {})
    strategy_name = config.get('name', strategy_id)

    logging.info(f'  📈 策略: {strategy_name} ({strategy_id})')
    logging.info(f'  📊 股票数量: {len(kline_dict)}')
    logging.info(f'  📅 回测区间: {start_date} ~ {end_date}')

    # 市场宽度过滤：全局开关 + 策略级开关 同时生效
    use_filter = enable_market_filter and config.get('use_market_filter', True)
    if use_filter:
        if market_breadth is not None:
            # 多策略对比时从外部传入，避免重复计算
            effective_breadth = market_breadth
            logging.info(f'  📊 市场宽度已由外部预计算')
        else:
            effective_breadth = compute_market_breadth(kline_dict, start_date, end_date)
    else:
        effective_breadth = None
        filter_reason = '策略不需要' if not config.get('use_market_filter', True) else '全局关闭'
        logging.info(f'  🔓 市场宽度过滤已跳过（{filter_reason}）')

    # 准备并行处理的参数
    tasks = []
    for code, kline in kline_dict.items():
        tasks.append(
            (code, kline, strategy_id, signal_params, sell_params,
             start_date, end_date, effective_breadth)
        )

    all_trades: List[Trade] = []
    total = len(tasks)
    failed = 0

    # 如果股票数量太少，不使用并行
    if total < 10:
        logging.info(f'  🔄 股票数量较少，使用串行模式')
        for args in tasks:
            trades = _backtest_single_stock(args)
            all_trades.extend(trades)
    else:
        # 使用并行处理
        n_workers = n_workers or max(1, multiprocessing.cpu_count() - 1)
        logging.info(f'  🚀 使用 {n_workers} 个进程并行回测...')

        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            futures = {
                executor.submit(_backtest_single_stock, args): i
                for i, args in enumerate(tasks)
            }

            completed = 0
            for future in as_completed(futures):
                completed += 1
                if completed % 500 == 0 or completed == total:
                    logging.info(f'  🔄 回测进度: {completed}/{total} 只股票...')

                try:
                    trades = future.result()
                    all_trades.extend(trades)
                except Exception as e:
                    failed += 1
                    logging.warning(f'  ⚠️ 子进程异常 (第{failed}个): {e}')

    # 按 (code, buy_date) 排序，确保每次运行结果确定一致
    all_trades.sort(key=lambda t: (t.code, t.buy_date))
    if failed:
        logging.warning(f'  ⚠️ 策略 [{strategy_name}] 回测完成，{failed}/{total} 只股票失败，共 {len(all_trades)} 笔交易')
    else:
        logging.info(f'  ✅ 策略 [{strategy_name}] 回测完成，共 {len(all_trades)} 笔交易')
    return all_trades


def run_multi_backtest(
    kline_dict: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    strategy_ids: List[str] | None = None,
    sell_params: dict[str, int | float] | None = None,
) -> Dict[str, List[Trade]]:
    """
    多策略对比回测

    参数:
        kline_dict: {code: DataFrame}
        start_date/end_date: 回测区间
        strategy_ids: 要运行的策略ID列表，None则运行全部
        sell_params: 卖出参数

    返回:
        {strategy_id: [Trade, ...]}
    """
    if strategy_ids is None:
        strategy_ids = list(STRATEGY_CONFIGS.keys())

    if sell_params is None:
        sell_params = DEFAULT_SELL_PARAMS.copy()

    # 预计算市场宽度（多策略共用，避免重复计算）
    any_needs_filter = any(
        STRATEGY_CONFIGS.get(sid, {}).get('use_market_filter', True)
        for sid in strategy_ids
    )
    shared_breadth = None
    if any_needs_filter:
        shared_breadth = compute_market_breadth(kline_dict, start_date, end_date)

    results = {}
    for sid in strategy_ids:
        logging.info(f'\n{"=" * 60}')
        logging.info(f'🚀 运行策略: {STRATEGY_CONFIGS[sid]["name"]}')
        logging.info(f'{"=" * 60}')
        trades = run_backtest(
            kline_dict, start_date, end_date, sid, sell_params,
            market_breadth=shared_breadth,
        )
        results[sid] = trades
    return results


# ==================== 统计汇总 ====================


def summarize(
    trades: List[Trade],
    strategy_name: str = '默认策略',
    start_date: str = None,
    end_date: str = None,
) -> dict[str, Any]:
    """统计回测结果"""
    if not trades:
        return {
            'strategy_name': strategy_name,
            'start_date': start_date,
            'end_date': end_date,
            'total_trades': 0,
            'win_trades': 0,
            'lose_trades': 0,
            'win_rate': 0.0,
            'avg_profit': 0.0,
            'avg_loss': 0.0,
            'total_profit': 0.0,
            'max_profit': 0.0,
            'max_loss': 0.0,
            'avg_hold_days': 0.0,
            'profit_factor': 0.0,
        }

    profits = [t.profit_pct for t in trades]
    win_trades = [t for t in trades if t.profit_pct > 0]
    lose_trades = [t for t in trades if t.profit_pct <= 0]

    total_win = sum(t.profit_pct for t in win_trades)
    total_loss = abs(sum(t.profit_pct for t in lose_trades))

    summary = {
        'strategy_name': strategy_name,
        'start_date': start_date,
        'end_date': end_date,
        'total_trades': len(trades),
        'win_trades': len(win_trades),
        'lose_trades': len(lose_trades),
        'win_rate': round(len(win_trades) / len(trades) * 100, 2) if trades else 0.0,
        'avg_profit': round(np.mean([t.profit_pct for t in win_trades]), 2) if win_trades else 0.0,
        'avg_loss': round(np.mean([t.profit_pct for t in lose_trades]), 2)
        if lose_trades
        else 0.0,
        'total_profit': round(sum(profits), 2),  # 每笔收益简单加总
        'max_profit': round(max(profits), 2),
        'max_loss': round(min(profits), 2),
        'avg_hold_days': round(np.mean([t.hold_days for t in trades]), 1),
        'profit_factor': round(total_win / total_loss, 2) if total_loss > 0 else 999.0,
    }

    # 按卖出原因统计
    reason_stats = {}
    for t in trades:
        reason_stats.setdefault(t.sell_reason, {'count': 0, 'total_profit': 0.0})
        reason_stats[t.sell_reason]['count'] += 1
        reason_stats[t.sell_reason]['total_profit'] += t.profit_pct

    summary['reason_stats'] = reason_stats
    return summary


def summarize_portfolio(
    closed_trades: list[dict],
    strategy_name: str = '默认策略',
    start_date: str = None,
    end_date: str = None,
) -> dict[str, Any]:
    """从资金账户实际成交记录生成交易汇总（与 summarize() 输出格式一致）"""
    if not closed_trades:
        return {
            'strategy_name': strategy_name,
            'start_date': start_date,
            'end_date': end_date,
            'total_trades': 0,
            'win_trades': 0,
            'lose_trades': 0,
            'win_rate': 0.0,
            'avg_profit': 0.0,
            'avg_loss': 0.0,
            'total_profit': 0.0,
            'max_profit': 0.0,
            'max_loss': 0.0,
            'avg_hold_days': 0.0,
            'profit_factor': 0.0,
        }

    profits = [t['profit_pct'] for t in closed_trades]
    win_trades = [t for t in closed_trades if t['profit_pct'] > 0]
    lose_trades = [t for t in closed_trades if t['profit_pct'] <= 0]

    total_win = sum(t['profit_pct'] for t in win_trades)
    total_loss = abs(sum(t['profit_pct'] for t in lose_trades))

    summary = {
        'strategy_name': strategy_name,
        'start_date': start_date,
        'end_date': end_date,
        'total_trades': len(closed_trades),
        'win_trades': len(win_trades),
        'lose_trades': len(lose_trades),
        'win_rate': round(len(win_trades) / len(closed_trades) * 100, 2) if closed_trades else 0.0,
        'avg_profit': round(np.mean([t['profit_pct'] for t in win_trades]), 2) if win_trades else 0.0,
        'avg_loss': round(np.mean([t['profit_pct'] for t in lose_trades]), 2) if lose_trades else 0.0,
        'total_profit': round(sum(profits), 2),
        'max_profit': round(max(profits), 2),
        'max_loss': round(min(profits), 2),
        'avg_hold_days': round(np.mean([t['hold_days'] for t in closed_trades]), 1),
        'profit_factor': round(total_win / total_loss, 2) if total_loss > 0 else 999.0,
    }

    # 按卖出原因统计
    reason_stats = {}
    for t in closed_trades:
        reason_stats.setdefault(t['sell_reason'], {'count': 0, 'total_profit': 0.0})
        reason_stats[t['sell_reason']]['count'] += 1
        reason_stats[t['sell_reason']]['total_profit'] += t['profit_pct']

    summary['reason_stats'] = reason_stats
    return summary


def print_portfolio_trades(closed_trades: list[dict], top_n: int = 30):
    """打印资金账户实际成交明细（与 print_trades 格式一致）"""
    if not closed_trades:
        logging.info('  无交易记录')
        return

    sorted_trades = sorted(closed_trades, key=lambda t: t['profit_pct'], reverse=True)
    top_winners = [t for t in sorted_trades if t['profit_pct'] > 0][:top_n]
    top_losers = [t for t in sorted_trades if t['profit_pct'] <= 0][:top_n]
    top_losers = sorted(top_losers, key=lambda t: t['profit_pct'])

    if top_winners:
        logging.info(f'\n  盈利 TOP {len(top_winners)}:')
        logging.info(f'  {"代码":<10} {"名称":<10} {"买入日":>12} {"卖出日":>12} {"持仓":>4}天 {"盈亏":>8} {"原因"}')
        logging.info('  ' + '-' * 70)
        for t in top_winners:
            logging.info(
                f'  {t["code"]:<10} {t["name"]:<10} {t["buy_date"]:>12} {t["sell_date"]:>12} '
                f'{t["hold_days"]:>4}天 {t["profit_pct"]:>+7.2f}% {t["sell_reason"]}'
            )

    if top_losers:
        logging.info(f'\n  亏损 TOP {len(top_losers)}:')
        logging.info(f'  {"代码":<10} {"名称":<10} {"买入日":>12} {"卖出日":>12} {"持仓":>4}天 {"盈亏":>8} {"原因"}')
        logging.info('  ' + '-' * 70)
        for t in top_losers:
            logging.info(
                f'  {t["code"]:<10} {t["name"]:<10} {t["buy_date"]:>12} {t["sell_date"]:>12} '
                f'{t["hold_days"]:>4}天 {t["profit_pct"]:>+7.2f}% {t["sell_reason"]}'
            )


def print_summary(summary: dict[str, Any]):
    """格式化打印汇总统计（同时输出到控制台和日志文件）"""
    start_date = summary.get('start_date', '')
    end_date = summary.get('end_date', '')
    date_str = f' ({start_date} ~ {end_date})' if start_date and end_date else ''

    lines = [
        f'\n{"=" * 60}',
        f'  📊 回测汇总报告 - {summary["strategy_name"]}{date_str}',
        f'{"=" * 60}',
        f'  总交易次数:     {summary["total_trades"]}',
        f'  盈利次数:       {summary["win_trades"]}',
        f'  亏损次数:       {summary["lose_trades"]}',
        f'  胜率:           {summary["win_rate"]}%',
        f'  平均盈利:       {summary["avg_profit"]}%',
        f'  平均亏损:       {summary["avg_loss"]}%',
        f'  最大单笔盈利:   {summary["max_profit"]}%',
        f'  最大单笔亏损:   {summary["max_loss"]}%',
        f'  平均持仓天数:   {summary["avg_hold_days"]}天',
        f'  盈亏比:         {summary["profit_factor"]}',
        f'{"-" * 60}',
    ]
    if 'reason_stats' in summary:
        lines.append('  按卖出原因统计:')
        reason_map = {
            'stop_loss': '止损',
            'stop_profit': '止盈',
            'trailing_stop': '移动止损',
            'max_hold': '最大持仓',
            'data_end': '数据结束',
        }
        for reason, stats in summary['reason_stats'].items():
            label = reason_map.get(reason, reason)
            lines.append(
                f'    {label:8s}: {stats["count"]:5d}笔  总收益: {stats["total_profit"]:+.2f}%'
            )

    for line in lines:
        logging.info(line)


def print_trades(trades: list[Trade], top_n: int = 30):
    """打印交易明细表（同时输出到控制台和日志文件）

    参数:
        trades: Trade 对象列表
        top_n: 默认打印前30笔和前30笔亏损
    """
    if not trades:
        logging.info('  无交易记录')
        return

    # 按盈亏排序
    sorted_trades = sorted(trades, key=lambda t: t.profit_pct, reverse=True)

    # 盈利 TOP N
    top_winners = [t for t in sorted_trades if t.profit_pct > 0][:top_n]
    # 亏损 TOP N
    top_losers = [t for t in sorted_trades if t.profit_pct < 0][-top_n:]
    top_losers.reverse()

    lines = [
        f'\n{"=" * 110}',
        f'  📋 交易明细',
        f'{"=" * 110}',
    ]

    if top_winners:
        lines.append(f'\n  🟢 盈利 TOP {len(top_winners)}:')
        lines.append(
            f'  {"代码":<8s} {"名称":<10s} {"买入日":<12s} {"买入价":>8s} {"卖出日":<12s} {"卖出价":>8s} {"收益":>8s} {"持仓":>6s} {"卖出原因":<8s}'
        )
        lines.append(f'  {"-" * 106}')
        for t in top_winners:
            reason_map = {
                'stop_loss': '止损',
                'stop_profit': '止盈',
                'trailing_stop': '移动止损',
                'max_hold': '最大持仓',
                'data_end': '数据结束',
            }
            reason = reason_map.get(t.sell_reason, t.sell_reason)
            lines.append(
                f'  {t.code:<8s} {t.name:<10s} {t.buy_date:<12s} {t.buy_price:>8.2f} '
                f'{t.sell_date or "N/A":<12s} {t.sell_price:>8.2f} {t.profit_pct:>+7.2f}% '
                f'{t.hold_days:>5d}天 {reason:<8s}'
            )

    if top_losers:
        lines.append(f'\n  🔴 亏损 TOP {len(top_losers)}:')
        lines.append(
            f'  {"代码":<8s} {"名称":<10s} {"买入日":<12s} {"买入价":>8s} {"卖出日":<12s} {"卖出价":>8s} {"收益":>8s} {"持仓":>6s} {"卖出原因":<8s}'
        )
        lines.append(f'  {"-" * 106}')
        for t in top_losers:
            reason_map = {
                'stop_loss': '止损',
                'stop_profit': '止盈',
                'trailing_stop': '移动止损',
                'max_hold': '最大持仓',
                'data_end': '数据结束',
            }
            reason = reason_map.get(t.sell_reason, t.sell_reason)
            lines.append(
                f'  {t.code:<8s} {t.name:<10s} {t.buy_date:<12s} {t.buy_price:>8.2f} '
                f'{t.sell_date or "N/A":<12s} {t.sell_price:>8.2f} {t.profit_pct:>+7.2f}% '
                f'{t.hold_days:>5d}天 {reason:<8s}'
            )

    # 交易费用估算（印花税 0.05% 卖出单边 + 佣金 0.025% 买卖双边 + 过户费 0.001%）
    total_buy_amount = sum(t.buy_price * 100 for t in trades)  # 假设每笔100股
    total_sell_amount = sum(t.sell_price * 100 for t in trades if t.sell_price > 0)
    stamp_tax = total_sell_amount * 0.0005  # 印花税 0.05%（卖出单边）
    commission = (
        total_buy_amount + total_sell_amount
    ) * 0.00025  # 佣金 0.025%（买卖双边）
    transfer_fee = (total_buy_amount + total_sell_amount) * 0.00001  # 过户费 0.001%
    total_fee = stamp_tax + commission + transfer_fee
    total_profit_amount = sum(t.profit_pct * t.buy_price for t in trades)  # 假设100股

    lines.append(f'\n  💰 交易费用估算（假设每笔100股）:')
    lines.append(f'  总买入金额:    {total_buy_amount:>12,.2f} 元')
    lines.append(f'  总卖出金额:    {total_sell_amount:>12,.2f} 元')
    lines.append(f'  印花税(0.05%): {stamp_tax:>12,.2f} 元')
    lines.append(f'  佣金(0.025%):  {commission:>12,.2f} 元')
    lines.append(f'  过户费(0.001%):{transfer_fee:>12,.2f} 元')
    lines.append(f'  {"─" * 30}')
    lines.append(f'  总费用:        {total_fee:>12,.2f} 元')
    lines.append(f'{"=" * 110}')

    for line in lines:
        logging.info(line)


def print_portfolio_summary(portfolio_result: dict[str, Any]):
    """打印资金账户回测结果"""
    if not portfolio_result:
        return
    initial = portfolio_result['initial_cash']
    final = portfolio_result['final_value']
    ret_pct = portfolio_result['total_return_pct']
    ret_amt = round(final - initial, 2)  # 直接计算，不依赖字典键名
    n_trades = portfolio_result['n_trades']
    n_win = portfolio_result['n_win']
    n_lose = portfolio_result['n_lose']
    win_rate = portfolio_result['win_rate']
    start_date = portfolio_result.get('start_date', 'N/A')
    end_date = portfolio_result.get('end_date', 'N/A')

    lines = [
        f'\n{"=" * 60}',
        f'  💰 资金账户回测结果（初始资金: {initial:,.0f} 元）',
        f'{"=" * 60}',
        f'  回测时间段:    {start_date} ~ {end_date}',
        f'  最终资产:      {final:>12,.2f} 元',
        f'  总收益:        {ret_amt:>+12,.2f} 元',
        f'  总收益率:      {ret_pct:>+7.2f}%',
        f'  最大回撤:      {portfolio_result.get("max_drawdown_pct", 0):>+7.2f}%',
        f'  交易次数:      {n_trades:>6d}',
        f'  盈利次数:      {n_win:>6d}',
        f'  亏损次数:      {n_lose:>6d}',
        f'  胜率:          {win_rate:>7.1f}%',
        f'{"=" * 60}',
    ]
    for line in lines:
        logging.info(line)


def print_comparison(summaries: Dict[str, dict[str, Any]]):
    """打印多策略对比表（同时输出到控制台和日志文件）"""
    # 获取回测时间段（从第一个策略的summary中获取）
    start_date = ''
    end_date = ''
    for s in summaries.values():
        start_date = s.get('start_date', '')
        end_date = s.get('end_date', '')
        if start_date and end_date:
            break

    date_str = f' ({start_date} ~ {end_date})' if start_date and end_date else ''

    lines = [
        f'\n{"=" * 100}',
        f'  📊 多策略回测对比报告{date_str}',
        f'{"=" * 100}',
        f'  {"策略":<16s} {"交易数":>6s} {"胜率":>8s} {"均盈利":>8s} {"均亏损":>8s} {"总收益":>10s} {"盈亏比":>8s} {"均持仓":>6s} {"市场过滤":>6s}',
        f'{"-" * 100}',
    ]
    for sid, s in summaries.items():
        name = STRATEGY_CONFIGS.get(sid, {}).get('name', sid)
        filter_on = STRATEGY_CONFIGS.get(sid, {}).get('use_market_filter', True)
        filter_tag = '✓' if filter_on else '—'
        lines.append(
            f'  {name:<16s} {s["total_trades"]:>6d} {s["win_rate"]:>7.1f}% '
            f'{s["avg_profit"]:>7.2f}% {s["avg_loss"]:>7.2f}% '
            f'{s["total_profit"]:>9.2f}% {s["profit_factor"]:>7.2f} '
            f'{s["avg_hold_days"]:>5.1f}天  {filter_tag:>6s}'
        )
    lines.append(f'{"=" * 100}')

    for line in lines:
        logging.info(line)
