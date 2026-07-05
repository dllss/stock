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

    def __init__(self, code: str, name: str, buy_date: str, buy_price: float):
        self.code = code
        self.name = name
        self.buy_date = buy_date
        self.buy_price = buy_price
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
        """计算当前总资产 = 现金 + 持仓市值"""
        position_value = 0.0
        for code, pos in self.positions.items():
            price = price_dict.get(code, pos['cost'])
            position_value += pos['shares'] * price
        return self.cash + position_value

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

        # 打印买入信息（显示可用现金 + 延迟到账资金）
        position_value = sum(p['shares'] * p['cost'] for p in self.positions.values())
        pending_total = sum(self.pending_cash.values())
        total_cash = self.cash + pending_total
        logging.info(
            f'  📈 买入 {date} {code}({name}) '
            f'价格:{price:.2f} 股数:{shares} 成本:{total_cost:.0f}元 | '
            f'持仓:{len(self.positions)}只 持仓市值:{position_value:.0f}元 '
            f'现金:{self.cash:.0f}元(可用) + {pending_total:.0f}元(延迟) = {total_cash:.0f}元(总)'
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
        profit_pct = (price - buy_price) / buy_price * 100

        # T+1规则：卖出资金第二个交易日才能使用
        # 计算下一个交易日，记录到pending_cash
        try:
            from datetime import datetime as dt, timedelta

            d = dt.strptime(date, '%Y-%m-%d')
            next_date = (d + timedelta(days=1)).strftime('%Y-%m-%d')
            self.pending_cash[next_date] = (
                self.pending_cash.get(next_date, 0) + total_revenue
            )
        except Exception:
            # 如果日期解析失败，直接加到cash（降级处理）
            self.cash += total_revenue

        # 计算持仓天数
        hold_days = 0
        try:
            d1 = datetime.strptime(pos['buy_date'], '%Y-%m-%d')
            d2 = datetime.strptime(date, '%Y-%m-%d')
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

        # 打印卖出信息（显示可用现金 + 延迟到账资金）
        # 注意：此时self.positions还未删除当前持仓，需要排除
        position_value = sum(p['shares'] * p['cost'] for k, p in self.positions.items() if k != code)
        pending_total = sum(self.pending_cash.values())
        total_cash = self.cash + pending_total
        logging.info(
            f'  📉 卖出 {date} {code}({pos["name"]}) '
            f'价格:{price:.2f} 股数:{shares} '
            f'收益:{profit:+.0f}元({profit_pct:+.1f}%) 原因:{reason} | '
            f'持仓:{len(self.positions)-1}只 持仓市值:{position_value:.0f}元 '
            f'现金:{self.cash:.0f}元(可用) + {pending_total:.0f}元(延迟) = {total_cash:.0f}元(总)'
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

    # 关键修复：生成完整的交易日历
    # 问题：T+1资金到账可能发生在没有交易的日期
    # 例如：今天卖出，明天资金到账，但明天可能没有买入/卖出
    # 如果明天不在sorted_dates中，update_pending_cash就不会被调用
    #
    # 解决方案：
    # 1. 如果有start_date和end_date，使用完整的回测区间
    # 2. 否则，使用交易日期范围，并扩展结束日期以容纳T+1（最多7天）
    from datetime import datetime as dt, timedelta

    if start_date and end_date:
        # 使用完整的回测区间，并扩展结束日期以容纳T+1（最多7天，考虑周末）
        range_start = start_date
        end_dt = dt.strptime(end_date, '%Y-%m-%d')
        range_end = (end_dt + timedelta(days=7)).strftime('%Y-%m-%d')
    elif all_trade_dates:
        # 使用交易日期的最小和最大值
        range_start = min(all_trade_dates)
        range_end = max(all_trade_dates)
        # 扩展结束日期以容纳T+1（最多7天，考虑周末）
        end_dt = dt.strptime(range_end, '%Y-%m-%d')
        range_end = (end_dt + timedelta(days=7)).strftime('%Y-%m-%d')
    else:
        sorted_dates = []
        range_start = None
        range_end = None

    if range_start and range_end:
        # 生成日期范围（只保留工作日：周一=0 到 周五=4）
        all_dates = []
        d = dt.strptime(range_start, '%Y-%m-%d')
        end = dt.strptime(range_end, '%Y-%m-%d')
        while d <= end:
            if d.weekday() < 5:  # 0-4 = 周一到周五
                all_dates.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)
        sorted_dates = sorted(set(all_dates))  # 去重并排序
    else:
        sorted_dates = []

    # 按日期顺序模拟
    for date in sorted_dates:
        # 更新T+1延迟到账的资金
        portfolio.update_pending_cash(date)

        # 先处理卖出（使用Trade对象中记录的正确价格）
        for code, tl in trade_map.items():
            for t in tl:
                if t.sell_date == date and code in portfolio.positions:
                    portfolio.try_sell(code, date, t.sell_price, t.sell_reason)

        # 再处理买入（使用Trade对象中记录的正确价格）
        # 注意：必须先检查持仓数量，再尝试买入
        current_positions = len(portfolio.positions)
        for code, tl in trade_map.items():
            if current_positions >= portfolio.max_positions:
                break  # 已满仓，跳过买入
            for t in tl:
                if t.buy_date == date and code not in portfolio.positions:
                    result = portfolio.try_buy(code, t.name, date, t.buy_price)
                    if result:
                        current_positions += 1  # 买入成功，更新持仓数量

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
            # 使用持仓成本价作为平仓价（保守估计，避免未来函数）
            # 注意：真实场景中应该用最后一天的开盘价或收盘价
            portfolio.try_sell(
                code, last_date, portfolio.positions[code]['cost'], 'data_end'
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
        'n_trades': n_trades,
        'n_win': n_win,
        'n_lose': n_lose,
        'win_rate': win_rate,
        'start_date': start_date,
        'end_date': end_date,
        'closed_trades': portfolio.closed_trades,
    }


# ==================== 策略参数定义 ====================

STRATEGY_CONFIGS = {
    'turtle_trade': {
        'name': '海龟交易法则',
        'description': '收盘价创60日新高时买入',
        'min_data_days': 60,
        'params': {},
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
    },
    'breakthrough_platform': {
        'name': '突破平台',
        'description': '横盘于MA60附近→放量突破MA60',
        'min_data_days': 120,
        'params': {},
    },
    'backtrace_ma250': {
        'name': '回踩年线',
        'description': '突破MA250→回踩不破→缩量',
        'min_data_days': 250,
        'params': {},
    },
    'keep_increasing': {
        'name': '均线多头',
        'description': 'MA30持续递增+月涨>20%',
        'min_data_days': 60,
        'params': {},
    },
    'low_atr': {
        'name': '低ATR成长',
        'description': '低波动(日波动<10%)+10天涨>10%',
        'min_data_days': 250,
        'params': {},
    },
    'low_backtrace': {
        'name': '无大幅回撤',
        'description': '60天涨60%+无单日跌>7%/连续跌>10%',
        'min_data_days': 60,
        'params': {},
    },
    'parking_apron': {
        'name': '停机坪',
        'description': '涨停后高位横盘3天',
        'min_data_days': 20,
        'params': {},
    },
    'high_tight_flag': {
        'name': '高而窄旗形',
        'description': '短期涨90%+连续涨停（回测模式忽略机构条件）',
        'min_data_days': 60,
        'params': {},
    },
    'climax_limitdown': {
        'name': '放量跌停',
        'description': '跌停+成交额>=2亿+量比>=4（高风险抄底）',
        'min_data_days': 61,
        'params': {},
    },
}

# 默认卖出参数
DEFAULT_SELL_PARAMS = {
    'stop_loss': -0.08,
    'stop_profit': 0.20,
    'trailing_stop': -0.05,
    'max_hold_days': 60,
}


# ==================== 选股信号函数（内嵌版） ====================


def _prepare_data(data: pd.DataFrame, end_idx: int) -> pd.DataFrame:
    """从历史数据中截取到 end_idx 为止的部分，方便策略计算"""
    return data.iloc[: end_idx + 1].copy()


# ---- 1. 海龟交易法则 ----
def _signal_turtle_trade(data: pd.DataFrame, end_idx: int, **kwargs) -> bool:
    """收盘价 >= 60日最高收盘价"""
    need = 60
    if end_idx < need - 1:
        return False
    window = data.iloc[end_idx - need + 1 : end_idx + 1]
    max_close = window['close'].max()
    last_close = data.iloc[end_idx]['close']
    return last_close >= max_close


# ---- 2. 量能突破 ----
def _signal_breakthrough_volume(data: pd.DataFrame, end_idx: int, **kwargs) -> bool:
    """收盘价突破N日高点 + 放量 + MA过滤"""
    break_days = kwargs.get('break_days', 20)
    use_volume = kwargs.get('use_volume', True)
    vol_ratio = kwargs.get('vol_ratio', 1.5)
    vol_days = kwargs.get('vol_days', 5)
    use_ma_filter = kwargs.get('use_ma_filter', True)
    ma_days = kwargs.get('ma_days', 60)

    min_need = max(break_days, vol_days, ma_days) + 1
    if end_idx < min_need:
        return False

    last_close = data.iloc[end_idx]['close']
    last_vol = data.iloc[end_idx]['volume']

    # 突破条件
    prev_high = data['high'].iloc[end_idx - break_days : end_idx].max()
    if last_close < prev_high:
        return False

    # 均线过滤
    if use_ma_filter:
        ma = data['close'].iloc[end_idx - ma_days + 1 : end_idx + 1].mean()
        if last_close < ma:
            return False

    # 放量确认
    if use_volume:
        vol_ma = data['volume'].iloc[end_idx - vol_days : end_idx].mean()
        if vol_ma <= 0 or last_vol / vol_ma < vol_ratio:
            return False

    return True


# ---- 3. 突破平台 ----
def _signal_breakthrough_platform(data: pd.DataFrame, end_idx: int, **kwargs) -> bool:
    """横盘于MA60附近 → 放量突破MA60"""
    threshold = 60
    if end_idx < threshold + threshold - 1:
        return False

    # 计算60日均线
    close_arr = data['close'].values
    ma60_arr = np.full(len(close_arr), np.nan)
    for i in range(threshold - 1, len(close_arr)):
        ma60_arr[i] = close_arr[i - threshold + 1 : i + 1].mean()

    # 取最后60天
    start = max(0, end_idx - threshold + 1)
    window = data.iloc[start : end_idx + 1].copy()
    window['ma60'] = ma60_arr[start : end_idx + 1]

    # 寻找突破点：open < ma60 <= close
    breakthrough_idx = None
    for j in range(len(window)):
        row = window.iloc[j]
        if pd.notna(row['ma60']) and row['open'] < row['ma60'] <= row['close']:
            # 检查放量（量比>=2）
            vol_5 = window['volume'].iloc[max(0, j - 4) : j + 1].mean()
            if vol_5 > 0 and row['volume'] / vol_5 >= 2:
                breakthrough_idx = j
                break

    if breakthrough_idx is None:
        return False

    # 突破前所有天：close与ma60偏离在 -5% ~ 20%
    for j in range(breakthrough_idx):
        row = window.iloc[j]
        if pd.isna(row['ma60']) or row['ma60'] <= 0:
            continue
        deviation = (row['ma60'] - row['close']) / row['ma60']
        if not (-0.05 < deviation < 0.2):
            return False

    return True


# ---- 4. 回踩年线 ----
def _signal_backtrace_ma250(data: pd.DataFrame, end_idx: int, **kwargs) -> bool:
    """突破MA250 → 回踩不破 → 缩量"""
    threshold = 60
    if end_idx < 250 - 1:
        return False

    # 计算MA250
    close_arr = data['close'].values
    ma250_arr = np.full(len(close_arr), np.nan)
    for i in range(249, len(close_arr)):
        ma250_arr[i] = close_arr[i - 249 : i + 1].mean()

    # 取最后60天
    start = max(0, end_idx - threshold + 1)
    subset = data.iloc[start : end_idx + 1].copy()
    subset['ma250'] = ma250_arr[start : end_idx + 1]

    if len(subset) < threshold:
        return False

    # 找最高价和最低价
    highest_close = 0
    highest_vol = 0
    highest_date = ''
    lowest_close = 1e9
    lowest_vol = 0
    lowest_date = ''

    for _, row in subset.iterrows():
        if row['close'] > highest_close:
            highest_close = row['close']
            highest_vol = row['volume']
            highest_date = row['date']
        if row['close'] < lowest_close:
            lowest_close = row['close']
            lowest_vol = row['volume']
            lowest_date = row['date']

    if highest_vol == 0 or lowest_vol == 0:
        return False

    # 前段：最高价日之前
    front = subset[subset['date'] < highest_date]
    if front.empty:
        return False

    # 前段开始close < ma250, 前段结束close > ma250
    first_row = front.iloc[0]
    last_row = front.iloc[-1]
    if pd.isna(first_row['ma250']) or pd.isna(last_row['ma250']):
        return False
    if not (
        first_row['close'] < first_row['ma250']
        and last_row['close'] > last_row['ma250']
    ):
        return False

    # 后段：最高价日及之后
    back = subset[subset['date'] >= highest_date]
    recent_lowest_close = 1e9
    recent_lowest_vol = 0
    recent_lowest_date = ''

    for _, row in back.iterrows():
        if pd.isna(row['ma250']):
            continue
        if row['close'] < row['ma250']:
            return False  # 跌破年线
        if row['close'] < recent_lowest_close:
            recent_lowest_close = row['close']
            recent_lowest_vol = row['volume']
            recent_lowest_date = row['date']

    # 检查回调时间：10~50天
    try:
        hd = pd.to_datetime(highest_date)
        ld = pd.to_datetime(recent_lowest_date)
        date_diff = (ld - hd).days
    except Exception:
        return False
    if not (10 <= date_diff <= 50):
        return False

    # 缩量回调
    vol_ratio_val = highest_vol / recent_lowest_vol
    back_ratio_val = recent_lowest_close / highest_close
    if not (vol_ratio_val > 2 and back_ratio_val < 0.8):
        return False

    return True


# ---- 5. 均线多头 ----
def _signal_keep_increasing(data: pd.DataFrame, end_idx: int, **kwargs) -> bool:
    """MA30持续递增 + 月涨>20%"""
    threshold = 30
    if end_idx < threshold - 1:
        return False

    # 计算MA30
    close_arr = data['close'].values
    ma30_arr = np.full(len(close_arr), np.nan)
    for i in range(29, len(close_arr)):
        ma30_arr[i] = close_arr[i - 29 : i + 1].mean()

    start = max(0, end_idx - threshold + 1)
    ma_values = ma30_arr[start : end_idx + 1]
    if len(ma_values) < threshold or np.any(pd.isna(ma_values)):
        return False

    step1 = round(threshold / 3)
    step2 = round(threshold * 2 / 3)

    if (
        ma_values[0] < ma_values[step1] < ma_values[step2] < ma_values[-1]
        and ma_values[-1] > 1.2 * ma_values[0]
    ):
        return True
    return False


# ---- 6. 低ATR成长 ----
def _signal_low_atr(data: pd.DataFrame, end_idx: int, **kwargs) -> bool:
    """低波动 + 10天涨>10%（修正ratio bug）"""
    threshold = 10
    if end_idx < 250 - 1:
        return False

    # 取最后10天
    start = end_idx - threshold + 1
    subset = data.iloc[start : end_idx + 1]
    if len(subset) < threshold:
        return False

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
        return False

    ratio = (highest_close - lowest_close) / lowest_close if lowest_close > 0 else 0
    # 修正原策略bug：ratio > 1.1 改为 ratio > 0.1
    if ratio > 0.1:
        return True
    return False


# ---- 7. 无大幅回撤 ----
def _signal_low_backtrace(data: pd.DataFrame, end_idx: int, **kwargs) -> bool:
    """60天涨60% + 无单日跌>7%/连续跌>10%"""
    threshold = 60
    if end_idx < threshold - 1:
        return False

    start = end_idx - threshold + 1
    subset = data.iloc[start : end_idx + 1]
    if len(subset) < threshold:
        return False

    # 60日涨幅 >= 60%
    ratio_increase = (subset.iloc[-1]['close'] - subset.iloc[0]['close']) / subset.iloc[
        0
    ]['close']
    if ratio_increase < 0.6:
        return False

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
            return False
        # 高开低走 > 7%
        if open_p > 0 and (close_p - open_p) / open_p * 100 < -7:
            return False
        # 两日累计跌 > 10%
        if prev_p_change + pchg < -10:
            return False
        # 两日高开低走累计 > 10%
        if prev_open > 0 and (close_p - prev_open) / prev_open * 100 < -10:
            return False

        prev_p_change = pchg
        prev_open = open_p

    return True


# ---- 8. 停机坪 ----
def _signal_parking_apron(data: pd.DataFrame, end_idx: int, **kwargs) -> bool:
    """涨停后高位横盘3天"""
    threshold = 15
    if end_idx < threshold:
        return False

    start = max(0, end_idx - threshold + 1)
    subset = data.iloc[start : end_idx + 1]
    if len(subset) < threshold:
        return False

    # 需要 p_change 列
    if 'p_change' not in subset.columns:
        return False

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
            return True

    return False


# ---- 9. 高而窄旗形 ----
def _signal_high_tight_flag(data: pd.DataFrame, end_idx: int, **kwargs) -> bool:
    """短期涨90%+连续涨停（回测模式忽略机构条件）"""
    threshold = 60
    if end_idx < threshold - 1:
        return False

    if 'p_change' not in data.columns:
        return False

    start = max(0, end_idx - threshold + 1)
    subset = data.iloc[start : end_idx + 1]
    if len(subset) < threshold:
        return False

    # 取倒数24~10天（共14天）
    tail24 = subset.tail(24)
    head14 = tail24.head(14)
    if len(head14) < 14:
        return False

    low = head14['low'].min()
    if low <= 0:
        return False
    ratio_increase = head14.iloc[-1]['high'] / low
    if ratio_increase < 1.9:
        return False

    # 连续涨停
    prev_pchg = 0.0
    for _, row in head14.iterrows():
        pchg = row.get('p_change', 0)
        if pd.isna(pchg):
            pchg = 0
        if pchg >= 9.5:
            if prev_pchg >= 9.5:
                return True
            else:
                prev_pchg = pchg
        else:
            prev_pchg = 0.0

    return False


# ---- 10. 放量跌停 ----
def _signal_climax_limitdown(data: pd.DataFrame, end_idx: int, **kwargs) -> bool:
    """跌停 + 成交额>=2亿 + 量比>=4"""
    if end_idx < 5:
        return False

    if 'p_change' not in data.columns:
        return False

    row = data.iloc[end_idx]
    pchg = row.get('p_change', 0)
    if pd.isna(pchg) or pchg > -9.5:
        return False

    # 成交额 >= 2亿
    amount = row['close'] * row['volume']
    if amount < 200000000:
        return False

    # 量比 >= 4（当日量 / 5日均量）
    if end_idx < 5:
        return False
    vol_5 = data['volume'].iloc[end_idx - 4 : end_idx + 1].mean()
    if vol_5 <= 0:
        return False
    if row['volume'] / vol_5 < 4:
        return False

    return True


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
    max_hold_days: int = 60,
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
    if max_high_since_buy > buy_price * (1 + stop_profit * 0.3):  # 至少浮盈过6%
        if max_profit_from_high <= trailing_stop:
            return True, 'trailing_stop'

    # 4. 最大持仓天数
    if hold_days >= max_hold_days:
        return True, 'max_hold'

    return False, ''


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

    # 统一 date 格式
    if hasattr(data['date'].iloc[0], 'strftime'):
        data['date'] = data['date'].apply(
            lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else str(x)[:10]
        )

    # 过滤回测时间范围
    mask = (data['date'] >= start_date) & (data['date'] <= end_date)
    data = data.loc[mask].reset_index(drop=True)

    if len(data) < min_data:
        return trades

    current_trade: Optional[Trade] = None
    max_high_since_buy: float = 0.0
    hold_day_counter: int = 0
    pending_buy: bool = False  # 标记是否需要延迟一天买入
    pending_sell: bool = False  # 标记是否需要延迟一天卖出
    pending_sell_reason: str = ''

    scan_start = min_data  # 从第 min_data 天开始扫描

    for i in range(scan_start, len(data)):
        today_date = data.iloc[i]['date']
        today_open = data.iloc[i]['open']
        today_high = data.iloc[i]['high']
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
            current_trade.max_loss_pct = current_trade.profit_pct

            trades.append(current_trade)
            current_trade = None
            max_high_since_buy = 0.0
            hold_day_counter = 0
            pending_sell = False
            pending_sell_reason = ''
            continue  # 卖出后不再检查买入

        # ========== 2. 执行待买入订单（延迟一天）==========
        if pending_buy and current_trade is None:
            # 在前一天触发买入信号，今天以开盘价买入
            buy_price = today_open
            current_trade = Trade(code, name, today_date, buy_price)
            max_high_since_buy = today_high
            hold_day_counter = 0
            pending_buy = False
            continue  # 买入后不再检查卖出

        # ========== 3. 持仓状态：检查卖出信号 ==========
        if current_trade is not None:
            hold_day_counter += 1
            max_high_since_buy = max(max_high_since_buy, today_high)

            # 使用今天的数据检查卖出信号（实际卖出在明天）
            should_sell, reason = check_sell_signal(
                buy_price=current_trade.buy_price,
                current_close=today_close,
                current_high=today_high,
                hold_days=hold_day_counter,
                max_high_since_buy=max_high_since_buy,
                **sell_params,
            )

            if should_sell:
                pending_sell = True
                pending_sell_reason = reason
                continue  # 延迟到明天卖出

        # ========== 4. 空仓状态：检查买入信号 ==========
        if current_trade is None and not pending_buy and not pending_sell:
            # 使用今天的数据计算信号，明天以开盘价买入
            if signal_func(data, i, **signal_params):
                pending_buy = True
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
        current_trade.max_loss_pct = current_trade.profit_pct

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
        current_trade.max_loss_pct = current_trade.profit_pct

        trades.append(current_trade)

    # 如果还有待买入的，取消买入（回测结束）
    if pending_buy:
        pending_buy = False

    return trades


# ==================== 批量回测 ====================


def _backtest_single_stock(args: Tuple) -> List[Trade]:
    """
    单只股票回测的包装函数（用于并行处理）

    参数:
        args: (code, kline_dict[code], strategy_id, signal_params, sell_params, start_date, end_date)

    返回:
        该股票的回测交易列表
    """
    code, kline, strategy_id, signal_params, sell_params, start_date, end_date = args

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

    # 准备并行处理的参数
    tasks = []
    for code, kline in kline_dict.items():
        tasks.append(
            (code, kline, strategy_id, signal_params, sell_params, start_date, end_date)
        )

    all_trades: List[Trade] = []
    total = len(tasks)

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
                    logging.debug(f'  ⚠️ 并行回测异常: {e}')

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

    results = {}
    for sid in strategy_ids:
        logging.info(f'\n{"=" * 60}')
        logging.info(f'🚀 运行策略: {STRATEGY_CONFIGS[sid]["name"]}')
        logging.info(f'{"=" * 60}')
        trades = run_backtest(kline_dict, start_date, end_date, sid, sell_params)
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
        'avg_profit': round(np.mean(profits), 2),
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
        f'  平均收益率:     {summary["avg_profit"]}%',
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
        f'  {"策略":<16s} {"交易数":>6s} {"胜率":>8s} {"均收益":>8s} {"均亏损":>8s} {"总收益":>10s} {"盈亏比":>8s} {"均持仓":>6s}',
        f'{"-" * 100}',
    ]
    for sid, s in summaries.items():
        name = STRATEGY_CONFIGS.get(sid, {}).get('name', sid)
        lines.append(
            f'  {name:<16s} {s["total_trades"]:>6d} {s["win_rate"]:>7.1f}% '
            f'{s["avg_profit"]:>7.2f}% {s["avg_loss"]:>7.2f}% '
            f'{s["total_profit"]:>9.2f}% {s["profit_factor"]:>7.2f} '
            f'{s["avg_hold_days"]:>5.1f}天'
        )
    lines.append(f'{"=" * 100}')

    for line in lines:
        logging.info(line)
