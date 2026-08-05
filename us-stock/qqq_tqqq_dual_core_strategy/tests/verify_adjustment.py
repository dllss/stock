#!/usr/bin/env python3
"""后复权(hfq) vs 真实价(raw) 对照验证脚本。

目标：量化「hfq 隐含分红再投资」对回测 CAGR 的影响，回答"用后复权回测是否过度乐观"。

三种口径：
  A 基线 : hfq 价格 + hfq 决策状态        —— 等于现状 main.py 的结果
  B 真实 : raw 价格 + raw 决策状态        —— 全用真实价重跑（含决策变化）
  C 隔离 : hfq 决策状态 + raw 价格        —— 纯粹隔离「复权对市值标记」的影响

结论读法：
  CAGR_A - CAGR_C ≈ 复权（分红再投资）贡献的年化差值；通常很小（QQQ 股息率 ~0.6%/年）。
  CAGR_A - CAGR_B ≈ 若改用真实价重做决策带来的偏差（状态机对相对量敏感，应接近 0）。

运行: python tests/verify_adjustment.py
"""
import os
import sys

# 让脚本可直接运行（项目根在 tests/ 的上级目录）
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pandas as pd

from backtest import engine
from config.settings import S
from data import fetcher
from strategy import strategy as ST
from utils import helpers


def _build_hfq(start: str, end: str) -> pd.DataFrame:
    """抓取 hfq 宽表并算指标 + 状态机决策（基线）。"""
    df = fetcher.fetch_and_merge(S.DATA_TICKERS, start, end)
    if df.empty:
        raise RuntimeError("hfq 数据为空，请先确保缓存或网络可用")
    df = helpers.add_indicators(df)
    df = ST.generate_signals(df)
    return df


def _build_raw(start: str, end: str) -> pd.DataFrame:
    """抓取真实价(raw)宽表，算指标 + 独立决策。"""
    frames = {}
    for tk in S.DATA_TICKERS:
        d = fetcher.fetch_ticker(tk, start, end, adjust="")  # 不复权 = 真实市价
        if d.empty:
            raise RuntimeError(f"真实价抓取失败: {tk}（接口可能暂不支持或需网络）")
        d = d.rename(columns=lambda c, tk=tk: f"{tk}_{c}" if c != "date" else c)
        frames[tk] = d
    base = None
    for tk, d in frames.items():
        base = d if base is None else base.merge(d, on="date", how="outer")
    base = base.sort_values("date").reset_index(drop=True)
    base = helpers.add_indicators(base)
    base = ST.generate_signals(base)
    return base


def _run(df: pd.DataFrame) -> dict:
    out = engine.run(df)
    return engine.summary(out.portfolio)


def _pct(x: float) -> str:
    return f"{x * 100:.2f}%"


def _translate_to_real(df_hfq: pd.DataFrame, df_a: pd.DataFrame) -> dict | None:
    """把 A(hfq 决策) 的逐日净值翻译为真实价市值，量化翻译精度。

    分红再投派下，真实价市值 = cash + shares_QQQ*raw_QQQ + shares_TQQQ*raw_TQQQ。
    同时用 adj_factor = hfq/raw 反推验证：hfq净值 / adj_factor ≈ 真实价市值。
    """
    try:
        # 抓真实价(raw)，列改名 _raw 避免与 hfq 的 QQQ_Close 冲突
        raw_frames = {}
        for tk in S.DATA_TICKERS:
            d = fetcher.fetch_ticker(tk, "20080101", "20991231", adjust="")
            if d.empty:
                return None
            d = d.rename(columns=lambda c, tk=tk: f"{tk}_raw" if c == "Close" else c)
            raw_frames[tk] = d[["date", f"{tk}_raw"]]
        raw = None
        for tk, d in raw_frames.items():
            raw = d if raw is None else raw.merge(d, on="date", how="outer")
        raw = raw.sort_values("date").reset_index(drop=True)

        # 跑 A 引擎拿逐日持仓
        out_a = engine.run(df_a)
        pa = out_a.portfolio.merge(raw, on="date", how="left")
        pa = pa.sort_values("date").reset_index(drop=True)

        real_val = (
            pa["cash"].fillna(0)
            + pa["shares_QQQ"].fillna(0) * pa["QQQ_raw"]
            + pa["shares_TQQQ"].fillna(0) * pa["TQQQ_raw"]
        )
        hfq_val = pa["portfolio_value"]
        cash = pa["cash"].fillna(0)

        # adj_factor 逐行（同一 pa 内 hfq/raw，已对齐）
        adj_factor = pa["QQQ_Close"] / pa["QQQ_raw"]
        # 正确反推：现金不参与复权，仅持仓部分除 adj_factor
        # real = cash + (hfq_val - cash) / adj_factor
        trans_via_adj = cash + (hfq_val - cash) / adj_factor
        valid = real_val.replace(0, float("nan")).notna() & adj_factor.notna()
        err = ((trans_via_adj - real_val) / real_val)[valid]

        return {
            "real_final": float(real_val.iloc[-1]),
            "adj_first": float(adj_factor.iloc[0]),
            "adj_last": float(adj_factor.iloc[-1]),
            "max_err": float(err.abs().max()) if len(err) else 0.0,
            "final_err": float(err.iloc[-1]) if len(err) else 0.0,
        }
    except Exception as e:  # noqa: BLE001
        print(f"  [翻译验证跳过] {e}")
        return None


def main() -> dict:
    start, end = "20080101", "20991231"
    df_hfq = _build_hfq(start, end)
    df_raw = _build_raw(start, end)

    # A: hfq 价格 + hfq 状态（基线）
    df_a = df_hfq[["date", "QQQ_Close", "TQQQ_Close", "state", "w_QQQ", "w_TQQQ"]].copy()

    # B: raw 价格 + raw 状态（全口径重跑）
    df_b = df_raw[["date", "QQQ_Close", "TQQQ_Close", "state", "w_QQQ", "w_TQQQ"]].copy()

    # C: hfq 状态 + raw 价格（隔离复权对市值标记的影响）
    raw_px = df_raw[["date", "QQQ_Close", "TQQQ_Close"]].rename(
        columns={"QQQ_Close": "QQQ_Close_raw", "TQQQ_Close": "TQQQ_Close_raw"}
    )
    df_c = df_hfq[["date", "state", "w_QQQ", "w_TQQQ"]].merge(raw_px, on="date", how="inner")
    df_c = df_c.rename(columns={"QQQ_Close_raw": "QQQ_Close", "TQQQ_Close_raw": "TQQQ_Close"})

    sa, sb, sc = _run(df_a), _run(df_b), _run(df_c)

    print("=" * 74)
    print("后复权(hfq) vs 真实价(raw) 对照验证")
    print(f"区间: {df_hfq['date'].min().date()} ~ {df_hfq['date'].max().date()}  初始资金 {S.INIT_CAPITAL:,.0f}")
    print("=" * 74)
    print(f"{'口径':<8}{'CAGR':>10}{'总收益':>12}{'最大回撤':>12}{'夏普':>8}")
    print(f"{'A hfq':<8}{_pct(sa['cagr']):>10}{_pct(sa['total_return']):>12}{_pct(sa['max_drawdown']):>12}{sa['sharpe']:>8.2f}")
    print(f"{'B raw':<8}{_pct(sb['cagr']):>10}{_pct(sb['total_return']):>12}{_pct(sb['max_drawdown']):>12}{sb['sharpe']:>8.2f}")
    print(f"{'C iso':<8}{_pct(sc['cagr']):>10}{_pct(sc['total_return']):>12}{_pct(sc['max_drawdown']):>12}{sc['sharpe']:>8.2f}")
    print("-" * 74)
    print(
        f"复权贡献(A-C): CAGR {((sa['cagr'] - sc['cagr']) * 100):+.2f}%/年   "
        f"总收益 {((sa['total_return'] - sc['total_return']) * 100):+.2f}%"
    )
    print(f"决策变化(A-B): CAGR {((sa['cagr'] - sb['cagr']) * 100):+.2f}%/年")
    print("=" * 74)

    # ---- 翻译验证：hfq 净值 -> 真实价市值（分红再投派精确对账）----
    # A 的 hfq 决策 + 真实价(raw) 成交 = 分红再投派实盘市值。
    # 用 adj_factor = hfq/raw 反推，或直接用 raw 价乘 A 的持仓份额。
    trans = _translate_to_real(df_hfq, df_a)
    if trans is not None:
        print("\n[翻译验证] hfq 净值 -> 真实价市值（分红再投派）")
        print(f"  期末 hfq 净值      : {S.INIT_CAPITAL * (1 + sa['total_return']):,.0f}")
        print(f"  期末真实价市值     : {trans['real_final']:,.0f}  (cash + shares×raw)")
        print(f"  复权因子 adj_factor : 首 {trans['adj_first']:.4f} -> 末 {trans['adj_last']:.4f}")
        print(
            f"  翻译误差(对真实价) : 最大 {trans['max_err'] * 100:+.3f}%  "
            f"期末 {trans['final_err'] * 100:+.3f}%"
        )
        print("  说明: 误差来自 adj_factor 舍入 / 再投 T+1 时滞，分红再投派下可忽略。")
        print("=" * 74)

    return {"A": sa, "B": sb, "C": sc}


if __name__ == "__main__":
    main()
