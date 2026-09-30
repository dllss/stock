#!/usr/bin/env python3
"""
回测引擎：状态机驱动 + 现金账户 + portfolio 模拟 + 整手取整再平衡。
支持 V22.1 双资产再平衡和 V22.3 单一 TQQQ 常态仓位；版本差异由 settings 控制。
portfolio_value = 现金 + 持仓市值（卖出变现进入现金，买入消耗现金）。
独立实现，不依赖主工程。
"""

import types

import pandas as pd

from config.settings import S


def _round_lot(diff_shares: float, price: float) -> float:
    """整手取整：按股数向下取整买卖。"""
    if not S.ROUND_LOTS or price <= 0:
        return diff_shares
    return int(diff_shares)


def run(df: pd.DataFrame) -> "types.SimpleNamespace":
    """
    输入：含 date, QQQ_Close, TQQQ_Close, state, w_QQQ, w_TQQQ 的 DataFrame
    输出：逐日 portfolio 记录（含现金、持仓、总净值）
    通用交易规则:
      - 首日 state_label='INIT'，不交易（等首次状态切换）
      - 状态切换当日只卖出（变现到现金），pending_buy=True
      - 次日（pending_buy）才按目标权重买入（T+1 防融资）
    """
    capital = S.INIT_CAPITAL
    cash = capital
    shares_qqq = 0.0
    shares_tqqq = 0.0
    prev_state = "INIT"  # 首日不建仓
    pending_buy = False
    records = []
    trades = []  # 调仓记录（用于日志输出）

    def add_trade(action, ticker, qty, price, amount, state):
        # 每笔交易附带成交后的持仓快照与市值，便于日志核对
        qqq_val = shares_qqq * row["QQQ_Close"]
        tqqq_val = shares_tqqq * row["TQQQ_Close"]
        total = cash + qqq_val + tqqq_val
        trades.append(
            {
                "date": row["date"],
                "action": action,
                "ticker": ticker,
                "shares": qty,
                "price": price,
                "value": amount,
                "state": state,
                "shares_qqq_after": shares_qqq,
                "shares_tqqq_after": shares_tqqq,
                "cash_after": cash,
                "qqq_val_after": qqq_val,
                "tqqq_val_after": tqqq_val,
                "value_after": total,
            }
        )

    for _, row in df.iterrows():
        qqq_price = row["QQQ_Close"]
        tqqq_price = row["TQQQ_Close"]
        state = row["state"]
        w_q = row["w_QQQ"]
        w_t = row["w_TQQQ"]

        hold_val = shares_qqq * qqq_price + shares_tqqq * tqqq_price
        value = cash + hold_val

        # --- T+1 买入：上一日切换留下的 pending，今日执行买入 ---
        if pending_buy:
            target_qqq_val = value * w_q
            target_tqqq_val = value * w_t
            buy_q_val = max(0.0, target_qqq_val - shares_qqq * qqq_price)
            buy_t_val = max(0.0, target_tqqq_val - shares_tqqq * tqqq_price)
            if buy_q_val >= S.MIN_TRADE_VAL and qqq_price > 0 and cash > 0:
                qty = _round_lot(min(buy_q_val, cash) / qqq_price, qqq_price)
                if qty > 0:
                    cost = qty * qqq_price
                    cash -= cost
                    shares_qqq += qty
                    add_trade("BUY", "QQQ", qty, qqq_price, cost, state)
            if buy_t_val >= S.MIN_TRADE_VAL and tqqq_price > 0 and cash > 0:
                qty = _round_lot(min(buy_t_val, cash) / tqqq_price, tqqq_price)
                if qty > 0:
                    cost = qty * tqqq_price
                    cash -= cost
                    shares_tqqq += qty
                    add_trade("BUY", "TQQQ", qty, tqqq_price, cost, state)
            pending_buy = False

        # --- 首日直接按目标权重建仓（初始全现金，用自己的资金当日买满，不等待状态切换）---
        if prev_state == "INIT":
            target_qqq_val = value * w_q
            target_tqqq_val = value * w_t
            buy_q_val = max(0.0, target_qqq_val - shares_qqq * qqq_price)
            buy_t_val = max(0.0, target_tqqq_val - shares_tqqq * tqqq_price)
            if buy_q_val >= S.MIN_TRADE_VAL and qqq_price > 0 and cash > 0:
                qty = _round_lot(min(buy_q_val, cash) / qqq_price, qqq_price)
                if qty > 0:
                    cost = qty * qqq_price
                    cash -= cost
                    shares_qqq += qty
                    add_trade("BUY", "QQQ", qty, qqq_price, cost, state)
            if buy_t_val >= S.MIN_TRADE_VAL and tqqq_price > 0 and cash > 0:
                qty = _round_lot(min(buy_t_val, cash) / tqqq_price, tqqq_price)
                if qty > 0:
                    cost = qty * tqqq_price
                    cash -= cost
                    shares_tqqq += qty
                    add_trade("BUY", "TQQQ", qty, tqqq_price, cost, state)

        # --- 当日调仓判定（仅状态切换触发；首日 INIT 不交易）---
        if prev_state != "INIT" and state != prev_state:
            target_qqq_val = value * w_q
            target_tqqq_val = value * w_t
            # 当日只卖出（把超出目标的部分变现），买入推迟到次日
            sell_q = max(0.0, shares_qqq * qqq_price - target_qqq_val)
            sell_t = max(0.0, shares_tqqq * tqqq_price - target_tqqq_val)
            if sell_q >= S.MIN_TRADE_VAL and qqq_price > 0:
                qty = _round_lot(sell_q / qqq_price, qqq_price)
                if qty > 0:
                    proceeds = qty * qqq_price
                    cash += proceeds
                    shares_qqq -= qty
                    add_trade("SELL", "QQQ", qty, qqq_price, proceeds, state)
            if sell_t >= S.MIN_TRADE_VAL and tqqq_price > 0:
                qty = _round_lot(sell_t / tqqq_price, tqqq_price)
                if qty > 0:
                    proceeds = qty * tqqq_price
                    cash += proceeds
                    shares_tqqq -= qty
                    add_trade("SELL", "TQQQ", qty, tqqq_price, proceeds, state)
            pending_buy = True

        # --- NORMAL 状态内偏离再平衡（仅双资产版本启用）---
        # 仅在非切换日（切换日已通过 pending_buy 处理）且当前持仓已建立后执行。
        if (
            prev_state != "INIT"
            and state == "NORMAL"
            and not pending_buy
            and value > 0
            and getattr(S, "NORMAL_REBAL_ENABLED", True)
            and w_q > 0
            and w_t > 0
        ):
            val_q = shares_qqq * qqq_price
            val_t = shares_tqqq * tqqq_price
            # 与 base 一致：偏差 = |市值_q - 市值_t| / 总市值
            deviation = abs(val_q - val_t) / (value + 1e-6)
            # 偏离超过阈值时调回（卖出偏离过大的一侧，补另一侧）
            if deviation > S.NORMAL_REBAL_DEV:
                target_qqq_val = value * S.NORMAL_REBAL_W
                target_tqqq_val = value * S.NORMAL_REBAL_W
                # 先卖后买：把超出目标的部分变现
                sell_q = max(0.0, val_q - target_qqq_val)
                sell_t = max(0.0, val_t - target_tqqq_val)
                if sell_q >= S.MIN_TRADE_VAL and qqq_price > 0:
                    qty = _round_lot(sell_q / qqq_price, qqq_price)
                    if qty > 0:
                        proceeds = qty * qqq_price
                        cash += proceeds
                        shares_qqq -= qty
                        add_trade("SELL", "QQQ", qty, qqq_price, proceeds, state)
                if sell_t >= S.MIN_TRADE_VAL and tqqq_price > 0:
                    qty = _round_lot(sell_t / tqqq_price, tqqq_price)
                    if qty > 0:
                        proceeds = qty * tqqq_price
                        cash += proceeds
                        shares_tqqq -= qty
                        add_trade("SELL", "TQQQ", qty, tqqq_price, proceeds, state)
                # 用当前现金补回目标（买入不跨日，直接当日完成）
                hold_val = shares_qqq * qqq_price + shares_tqqq * tqqq_price
                value = cash + hold_val
                buy_q_val = max(0.0, target_qqq_val - shares_qqq * qqq_price)
                buy_t_val = max(0.0, target_tqqq_val - shares_tqqq * tqqq_price)
                if buy_q_val >= S.MIN_TRADE_VAL and qqq_price > 0 and cash > 0:
                    qty = _round_lot(min(buy_q_val, cash) / qqq_price, qqq_price)
                    if qty > 0:
                        cost = qty * qqq_price
                        cash -= cost
                        shares_qqq += qty
                        add_trade("BUY", "QQQ", qty, qqq_price, cost, state)
                if buy_t_val >= S.MIN_TRADE_VAL and tqqq_price > 0 and cash > 0:
                    qty = _round_lot(min(buy_t_val, cash) / tqqq_price, tqqq_price)
                    if qty > 0:
                        cost = qty * tqqq_price
                        cash -= cost
                        shares_tqqq += qty
                        add_trade("BUY", "TQQQ", qty, tqqq_price, cost, state)

        prev_state = state
        hold_val = shares_qqq * qqq_price + shares_tqqq * tqqq_price
        value = cash + hold_val

        records.append(
            {
                "date": row["date"],
                "state": state,
                "QQQ_Close": qqq_price,
                "TQQQ_Close": tqqq_price,
                "QQQ_Volume": row.get("QQQ_Volume"),
                "cash": cash,
                "shares_QQQ": shares_qqq,
                "shares_TQQQ": shares_tqqq,
                "portfolio_value": value,
                "ret": value / S.INIT_CAPITAL - 1.0,
            }
        )

    out = types.SimpleNamespace()
    out.portfolio = pd.DataFrame(records)
    out.trades = (
        pd.DataFrame(trades)
        if trades
        else pd.DataFrame(
            columns=[
                "date",
                "action",
                "ticker",
                "shares",
                "price",
                "value",
                "state",
                "shares_qqq_after",
                "shares_tqqq_after",
                "cash_after",
                "qqq_val_after",
                "tqqq_val_after",
                "value_after",
            ]
        )
    )
    return out


def summary(out: pd.DataFrame) -> dict:
    if out.empty:
        return {}
    final = out["portfolio_value"].iloc[-1]
    total_ret = final / S.INIT_CAPITAL - 1.0
    curve = out["portfolio_value"]
    peak = curve.cummax()
    dd = (curve - peak) / peak
    max_dd = dd.min()
    n_days = max(len(out) - 1, 1)
    years = n_days / 252.0
    cagr = (final / S.INIT_CAPITAL) ** (1 / years) - 1 if years > 0 else 0.0
    # 夏普比率：组合净值日收益率均值/标准差（年化 ×√252），无风险利率取 0
    daily_ret = curve.pct_change(fill_method=None).dropna()
    if len(daily_ret) > 1 and daily_ret.std() > 0:
        sharpe = daily_ret.mean() / daily_ret.std() * (252**0.5)
    else:
        sharpe = 0.0
    return {
        "final_value": final,
        "total_return": total_ret,
        "max_drawdown": max_dd,
        "cagr": cagr,
        "sharpe": sharpe,
        "trading_days": n_days,
    }


def buy_and_hold(df: pd.DataFrame, ticker: str = "QQQ") -> dict:
    """
    买入持有基准：在交易起点全仓买入 ticker 并持有至结束。
    返回与 summary 同口径的指标，便于直接对比。
    ticker 可传 "QQQ" 或列名 "QQQ_Close"（自动兼容）。
    """
    if df.empty:
        return {}
    col = ticker if ticker in df.columns else f"{ticker}_Close"
    if col not in df.columns:
        return {}
    price = df[col].dropna()
    if price.empty:
        return {}
    p0 = price.iloc[0]
    p1 = price.iloc[-1]
    shares = S.INIT_CAPITAL / p0
    final = shares * p1
    total_ret = final / S.INIT_CAPITAL - 1.0
    curve = (price / p0) * S.INIT_CAPITAL
    peak = curve.cummax()
    dd = (curve - peak) / peak
    max_dd = dd.min()
    n_days = max(len(price) - 1, 1)
    years = n_days / 252.0
    cagr = (final / S.INIT_CAPITAL) ** (1 / years) - 1 if years > 0 else 0.0
    daily_ret = curve.pct_change(fill_method=None).dropna()
    sharpe = (
        daily_ret.mean() / daily_ret.std() * (252**0.5)
        if len(daily_ret) > 1 and daily_ret.std() > 0
        else 0.0
    )
    return {
        "ticker": ticker,
        "final_value": final,
        "total_return": total_ret,
        "max_drawdown": max_dd,
        "cagr": cagr,
        "sharpe": sharpe,
        "trading_days": n_days,
        "curve": curve,  # 逐日净值(归一化到初始资金)，供 HTML 报告画对比曲线
    }
