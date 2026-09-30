#!/usr/bin/env python3
"""
QQQ & TQQQ 双核策略 - 统一入口。
一条命令同时跑两部分：
  回测：前复权(qfq)历史回测，验证策略长期有效性。
  实盘跟踪：跟踪真实账户收益 + 次日操作指令（复用回测 logger，
      实盘历史表嵌入 HTML 报告，实盘日志合并进 backtest_full_{ts}.log）。

运行: python main.py [--end YYYY-MM-DD]
"""

import datetime
import logging
import os
import sys
import time
import unicodedata

import pandas as pd

# 保证可 import 同级包
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backtest import engine
from config.settings import S
from data import fetcher
from strategy import strategy as ST
from utils import helpers

# ---- 中文显示映射（仅输出层，不影响策略逻辑与 base 对齐）----
STATE_CN = {
    "INIT": "INIT(初始)",
    "NORMAL": "NORMAL(常态)",
    "TOP_ESCAPE": "TOP_ESCAPE(逃顶)",
    "ZONE_DESPAIR_TQQQ": "ZONE_DESPAIR_TQQQ(绝望区TQQQ)",
    "ZONE_BATTLE_ATTACK": "ZONE_BATTLE_ATTACK(交战进攻)",
    "ZONE_BATTLE_DEFEND": "ZONE_BATTLE_DEFEND(交战防守)",
    "BEAR_CASH": "BEAR_CASH(熊市空仓)",
}
ACTION_CN = {"BUY": "BUY(买入)", "SELL": "SELL(卖出)"}

# 英文表头 -> 中文(英文) 显示表头
PORTFOLIO_COLS_CN = {
    "date": "date(日期)",
    "state": "state(状态)",
    "QQQ_Close": "QQQ_Close(QQQ收盘价)",
    "TQQQ_Close": "TQQQ_Close(TQQQ收盘价)",
    "cash": "cash(现金)",
    "shares_QQQ": "shares_QQQ(QQQ持仓股数)",
    "shares_TQQQ": "shares_TQQQ(TQQQ持仓股数)",
    "portfolio_value": "portfolio_value(总资产)",
    "ret": "ret(累计收益率%)",
}
TRADE_COLS_CN = {
    "date": "date(日期)",
    "action": "action(动作)",
    "ticker": "ticker(标的)",
    "shares": "shares(股数)",
    "price": "price(价格)",
    "value": "value(金额)",
    "state": "state(状态)",
}


def _localize_portfolio(out: "pd.DataFrame") -> "pd.DataFrame":
    df = out.copy()
    df["state"] = df["state"].map(lambda s: STATE_CN.get(s, s))
    # 总资产保留两位小数（数值），累计收益率改为百分比字符串
    df["portfolio_value"] = df["portfolio_value"].round(2)
    df["ret"] = (df["ret"] * 100).round(2).astype(str) + "%"
    df = df.rename(columns=PORTFOLIO_COLS_CN)
    return df


def _localize_trades(trades: "pd.DataFrame") -> "pd.DataFrame":
    df = trades.copy()
    df["action"] = df["action"].map(lambda a: ACTION_CN.get(a, a))
    df["state"] = df["state"].map(lambda s: STATE_CN.get(s, s))
    df = df.rename(columns=TRADE_COLS_CN)
    return df


def _merge_portfolio_trades(portfolio: "pd.DataFrame", trades: "pd.DataFrame") -> "pd.DataFrame":
    """
    合并逐日净值与逐笔调仓为单一文件。
    逐日为主表；交易动作按当日序号展开为多列（最多 MAX_TRADE_PER_DAY 笔）。
    无交易日对应动作为空。
    """
    MAX_TRADE_PER_DAY = 2
    port = _localize_portfolio(portfolio)
    trades = _localize_trades(trades)

    # 把每笔交易整合成一列（动作/标的/股数/价格/金额）
    def _fmt_trade(r):
        return pd.Series(
            {
                "trade_action(动作)": r["action(动作)"],
                "trade_ticker(标的)": r["ticker(标的)"],
                "trade_shares(股数)": r["shares(股数)"],
                "trade_price(价格)": r["price(价格)"],
                "trade_value(金额)": r["value(金额)"],
            }
        )

    TRADE_FIELDS = [
        "trade_action(动作)",
        "trade_ticker(标的)",
        "trade_shares(股数)",
        "trade_price(价格)",
        "trade_value(金额)",
    ]
    if not trades.empty:
        t = trades.copy()
        t[TRADE_FIELDS] = t.apply(_fmt_trade, axis=1)
        t = t[["date(日期)"] + TRADE_FIELDS]
        t = t.sort_values(["date(日期)"])
        t["_seq"] = t.groupby("date(日期)").cumcount()  # 0,1,... 当日第几笔
        # 透视：每行一笔交易 -> 每列一组(序号+字段)
        wide = t.pivot(index="date(日期)", columns="_seq")
        wide.columns = [f"{col[0]}{col[1]+1}" for col in wide.columns]  # 动作1/标的1...
        # 补齐缺失序号列
        for i in range(1, MAX_TRADE_PER_DAY + 1):
            for fld in TRADE_FIELDS:
                cname = f"{fld}{i}"
                if cname not in wide.columns:
                    wide[cname] = ""
        wide = wide.reset_index()
    else:
        wide = pd.DataFrame(columns=["date(日期)"])

    merged = port.merge(wide, on="date(日期)", how="left")
    return merged


def setup_logger(run_log_path=None):
    os.makedirs(S.OUTPUT_DIR, exist_ok=True)
    logger = logging.getLogger("backtest")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    # 仅保留带时间戳的本次运行 log（与 backtest_full_{ts}.csv 配对），
    # 不再写固定名 backtest.log（与带时间戳 log 内容重复，属冗余文件）。
    if run_log_path:
        rfh = logging.FileHandler(run_log_path, encoding="utf-8")
        rfh.setFormatter(fmt)
        logger.addHandler(rfh)
    # 同时打到终端
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


# ---- 实盘跟踪逻辑已迁移至 utils/live_signal.py，由 live_track.py 调用 ----
# main.py 是统一入口：回测 + 实盘跟踪（live_track.run 在 main() 内部调用），
# 实盘历史表与次日指令嵌入 HTML 报告的「实盘跟踪」章节。


def main(start=None, warmup_start=None, end=None):
    # 命令行 / 调用参数覆盖配置默认值
    trade_start = start or S.WARMUP_END
    warm_start = warmup_start or S.WARMUP_START
    end_date = end or pd_today()

    # 运行时时间戳：CSV 与同名 log 共用，保证配对
    run_ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(S.OUTPUT_DIR, f"backtest_full_{run_ts}.csv")
    log_path = os.path.join(S.OUTPUT_DIR, f"backtest_full_{run_ts}.log")

    logger = setup_logger(run_log_path=log_path)
    logger.info("=" * 60)
    logger.info("QQQ & TQQQ 双核策略回测开始")
    logger.info(
        "配置: 复权=%s, 交易起点=%s, 预热起点=%s, 终点=%s, 初始资金=%.0f, ATH窗口=%d, VolMA窗口=%d",
        S.ADJUST,
        trade_start,
        warm_start,
        end_date,
        S.INIT_CAPITAL,
        S.ATH_WINDOW,
        S.VOL_WINDOW,
    )

    # 1. 下载 / 读取数据（含预热区间，用于指标计算）
    logger.info("步骤1: 获取数据")
    raw = fetcher.fetch_and_merge(
        S.DATA_TICKERS,
        start=warm_start,
        end=end_date,
    )
    if raw.empty:
        logger.error("无数据，退出")
        return
    logger.info(
        "数据区间: %s ~ %s, 共 %d 行", raw["date"].min().date(), raw["date"].max().date(), len(raw)
    )

    # 2. 计算指标
    logger.info("步骤2: 计算指标 (ATH窗口=%d, VolMA窗口=%d)", S.ATH_WINDOW, S.VOL_WINDOW)
    df = helpers.add_indicators(raw)

    # 4. 仅在预热结束后开始交易（供主流程与对比方案共用）
    trade_df = df[df["date"] >= pd_T(trade_start)].reset_index(drop=True)
    logger.info("步骤4: 回测 (交易起点 %s)", trade_start)

    # 3+4. 跑一次双核策略（临时覆盖 VOL_FACTOR，不改 settings 原值）
    def _run_dual_core(vol_factor):
        orig = S.VOL_FACTOR
        try:
            S.VOL_FACTOR = vol_factor
            # generate_signals 需完整 df（含预热区间指标），重算信号后切交易段
            sig = ST.generate_signals(df)
        finally:
            S.VOL_FACTOR = orig  # 始终恢复 settings 当前值（1.5）
        td = sig[sig["date"] >= pd_T(trade_start)].reset_index(drop=True)
        return engine.run(td)

    # 主流程：使用 settings 正式值 VOL_FACTOR (现已改 1.5)
    logger.info("步骤3: 双核状态机 (VOL_FACTOR=%.1f, 正式值)", S.VOL_FACTOR)
    out = _run_dual_core(S.VOL_FACTOR)

    # 5. 输出（合并为单一文件，文件名带运行时时间戳，不覆盖历史）
    merged = _merge_portfolio_trades(out.portfolio, out.trades)
    # out_path / log_path 已在 main 开头生成（与 CSV 同名的 log 已挂载 handler）
    merged.to_csv(out_path, index=False)
    # 删除历史分开的两个文件，只保留合并版
    for _f in ("portfolio_history.csv", "trade_history.csv"):
        _p = os.path.join(S.OUTPUT_DIR, _f)
        if os.path.exists(_p):
            os.remove(_p)
    summ = engine.summary(out.portfolio)

    # 6. 买入持有基准对比（同区间、同复权、同初始资金）
    bh_qqq = engine.buy_and_hold(trade_df, ticker="QQQ")
    bh_tqqq = engine.buy_and_hold(trade_df, ticker="TQQQ")

    # 6.1 对比基准：临时覆盖 VOL_FACTOR=2.0 (改动前的原值) 再跑一次，用于并排对比
    out_vol20 = _run_dual_core(2.0)
    summ_vol20 = engine.summary(out_vol20.portfolio)
    logger.info(
        "对比基准 (VOL_FACTOR=2.0, 原值): 最终资产 %.0f, 年化 %.2f%%, 夏普 %.2f",
        summ_vol20["final_value"],
        summ_vol20["cagr"] * 100,
        summ_vol20["sharpe"],
    )

    # 6.2 复权对比已移除：回测全程统一使用 qfq 前复权，不再重跑 hfq 对照。

    logger.info("-" * 60)
    logger.info("回测结果 (双核策略):")
    for k, v in summ.items():
        if isinstance(v, float):
            logger.info("  %s: %.4f", k, v)
        else:
            logger.info("  %s: %s", k, v)

    for _bh in (bh_qqq, bh_tqqq):
        if _bh:
            logger.info("-" * 60)
            logger.info("买入持有基准 (全仓%s):", _bh["ticker"])
            for k in ("final_value", "total_return", "max_drawdown", "cagr", "trading_days"):
                v = _bh[k]
                logger.info("  %s: %.4f" if isinstance(v, float) else "  %s: %s", k, v)

    # 状态分布
    dist = out.portfolio["state"].value_counts()
    logger.info("状态分布:")
    for st, cnt in dist.items():
        logger.info("  %s: %d 天", st, cnt)

    # 调仓记录（含成交后持仓快照与市值）
    logger.info("调仓记录 (%d 笔):", len(out.trades))
    for _, t in out.trades.iterrows():
        logger.info(
            "  %s | %s %s | x%.0f @ %.2f | 金额 %.0f | 状态 %s | "
            "持仓 QQQ %.0f/TQQQ %.0f | QQQ市值 %.0f TQQQ市值 %.0f | 现金 %.0f | 总资产 %.0f",
            t["date"].date() if hasattr(t["date"], "date") else t["date"],
            t["action"],
            t["ticker"],
            t["shares"],
            t["price"],
            t["value"],
            t["state"],
            t["shares_qqq_after"],
            t["shares_tqqq_after"],
            t["qqq_val_after"],
            t["tqqq_val_after"],
            t["cash_after"],
            t["value_after"],
        )

    logger.info("完成: 结果已写入 %s", out_path)

    # 7. 终端汇总（起始/最终时间、资金、收益率、年化），同时写入同名 log
    start_date = trade_df["date"].iloc[0]
    end_date = trade_df["date"].iloc[-1]
    start_cap = S.INIT_CAPITAL
    end_cap = summ["final_value"]
    ret = summ["total_return"]
    cagr = summ["cagr"]
    sd = start_date.date() if hasattr(start_date, "date") else start_date
    ed = end_date.date() if hasattr(end_date, "date") else end_date
    logger.info("=" * 56)
    logger.info("回测汇总 (双核策略)")
    logger.info("-" * 56)
    logger.info(f"起始时间 : {sd}")
    logger.info(f"起始资金 : {start_cap:,.2f}")
    logger.info(f"最终时间 : {ed}")
    logger.info(f"最终资金 : {end_cap:,.2f}")
    logger.info(f"累计收益率: {ret * 100:.2f}%")
    logger.info(f"年化收益率: {cagr * 100:.2f}%")
    logger.info(f"最大回撤  : {summ['max_drawdown'] * 100:.2f}%")
    logger.info(f"夏普比率  : {summ['sharpe']:.2f}")
    logger.info("=" * 56)

    # 四栏对比表格：双核(正式1.5) / 双核(原值2.0) / 全仓QQQ / 全仓TQQQ
    def _disp_width(text):
        # 中文/全角字符按 2 宽，英文按 1 宽，保证终端对齐
        return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1 for ch in str(text))

    def _pad(text, width, align="<"):
        text = str(text)
        gap = width - _disp_width(text)
        if gap < 0:
            gap = 0
        return text + " " * gap if align == "<" else " " * gap + text

    # 列宽（显示宽度）
    W_LBL, W_VAL, W_PCT, W_DD, W_SH = 12, 16, 12, 12, 8
    _sep = "-" * (W_LBL + 1 + W_VAL + 1 + W_PCT + 1 + W_PCT + 1 + W_DD + 1 + W_SH)

    def _row2(label, s):
        if not s:
            return " | ".join(
                [
                    _pad(label, W_LBL),
                    _pad("-", W_VAL, ">"),
                    _pad("-", W_PCT, ">"),
                    _pad("-", W_PCT, ">"),
                    _pad("-", W_DD, ">"),
                    _pad("-", W_SH, ">"),
                ]
            )
        return " | ".join(
            [
                _pad(label, W_LBL),
                _pad(f"{s['final_value']:,.0f}", W_VAL, ">"),
                _pad(f"{s['total_return'] * 100:,.2f}%", W_PCT, ">"),
                _pad(f"{s['cagr'] * 100:,.2f}%", W_PCT, ">"),
                _pad(f"{s['max_drawdown'] * 100:,.2f}%", W_DD, ">"),
                _pad(f"{s['sharpe']:.2f}", W_SH, ">"),
            ]
        )

    logger.info("策略对比表 (区间 %s ~ %s, 起始资金 %.0f):", sd, ed, start_cap)
    logger.info(_sep)
    logger.info(
        " | ".join(
            [
                _pad("策略", W_LBL),
                _pad("最终资产", W_VAL, ">"),
                _pad("累计收益", W_PCT, ">"),
                _pad("年化", W_PCT, ">"),
                _pad("最大回撤", W_DD, ">"),
                _pad("夏普", W_SH, ">"),
            ]
        )
    )
    logger.info(_sep)
    logger.info(_row2("双核(正式1.5)", summ))
    logger.info(_row2("双核(原值2.0)", summ_vol20))
    logger.info(_row2(f"全仓{bh_qqq['ticker']}", bh_qqq))
    logger.info(_row2(f"全仓{bh_tqqq['ticker']}", bh_tqqq))
    logger.info(_sep)
    logger.info("=" * 56)
    logger.info(f"结果文件: {out_path}")
    logger.info(f"日志文件: {log_path}")
    logger.info("=" * 60)

    # 9. 生成 HTML 报告（与 CSV 同目录、同时间戳配对，方便直观查看）
    try:
        from backtest.report_html import build_report
        import live_track

        # 实盘跟踪线：复用当前回测 logger（日志合并进 backtest_full_{ts}.log），
        # 历史表转 HTML 嵌入报告「实盘跟踪」章节。
        live_hist, guide_text = live_track.run(logger)
        live_hist_html = live_track._hist_to_html(live_hist) if live_hist is not None else ""

        run_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        html_path = build_report(
            out_path,
            summ,
            summ_vol20,
            bh_qqq,
            bh_tqqq,
            out.portfolio["state"].value_counts(),
            out.portfolio,
            start_cap,
            trade_df=trade_df,
            run_ts=run_ts,
            live_hist_html=live_hist_html,
            live_text=guide_text or None,
        )
        logger.info(f"HTML报告: {html_path}")
    except Exception as e:
        import traceback as _tb

        logger.warning("HTML 报告生成失败: %s\n%s", e, _tb.format_exc())
        html_path = None

    return html_path


def pd_today():
    import datetime

    return datetime.date.today().isoformat()


def pd_T(s):
    return s


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="QQQ & TQQQ 双核策略：回测 + 实盘跟踪 统一入口")
    parser.add_argument("--start", default=None, help="交易起点日期 (默认读 settings.WARMUP_END)")
    parser.add_argument(
        "--warmup-start",
        default=None,
        help="预热起点日期，用于指标计算 (默认读 settings.WARMUP_START)",
    )
    parser.add_argument("--end", default=None, help="回测终点日期 (默认今天)，同时透传给回测与实盘跟踪")
    args = parser.parse_args()
    html_path = main(start=args.start, warmup_start=args.warmup_start, end=args.end)

    print("\n" + "#" * 70)
    if html_path:
        print("# 回测 HTML 报告已生成（含实盘跟踪章节）:")
        print(f"#   {html_path}")
    else:
        print("# 回测 HTML 报告本次未生成（见上方 warning）。")
    print("#" * 70)
