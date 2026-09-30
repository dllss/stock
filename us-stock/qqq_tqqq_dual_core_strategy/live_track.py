#!/usr/bin/env python3
"""实盘跟踪：独立跟踪你在真实建仓日的实盘收益率，并输出次日操作指令。

与 main.py 的回测（前复权 qfq 历史回测）协同：
- 回测跑前复权(qfq)回测（python main.py），看策略历史表现。
- 实盘跟踪只跟踪你 2026-08-06 实际建仓的仓位（QQQ 1股 / TQQQ 11股 / 现金），
  用真实价（不复权）逐日计算你的组合市值、累计收益率、当前占比。
- 次日操作指令：复用回测引擎算出的策略状态（NORMAL/BEAR 等），
  套用 TRACK_* 真实账户数据，给出状态切换地图 + 再平衡建议。
  该逻辑已迁移到 utils/live_signal.py（原 main.py --live）。

文件合并策略（与用户约定）：
- 实盘跟踪日志合并进回测的 backtest_full_{ts}.log（run(logger=回测logger) 时）。
- 实盘历史表（live_track_history）不再单独写 CSV，而是嵌入回测 HTML 报告的
  「实盘跟踪」章节（由 main.py 调用 build_report(live_hist_html=...) 实现）。
- 独立运行 python live_track.py 时，日志仍写回 live_track.log（兼容），并返回 hist。

运行: python live_track.py  （日常统一入口请用 python main.py）
"""

import logging
import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import S
from data import fetcher
from strategy import strategy as ST
from utils import helpers
from backtest import engine
from utils.live_signal import emit_next_state_guide, emit_live_signal


def _load_real_prices(ticker: str, start: str, end: str) -> pd.Series:
    """抓真实价（不复权）日线，返回 date->Close 的 Series。"""
    df = fetcher.fetch_ticker(ticker, start=start, end=end, adjust="")
    if df.empty:
        return pd.Series(dtype=float)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df.set_index("date")["Close"]


def _setup_logger(logger=None):
    """返回用于实盘跟踪的 logger。

    - 若传入外部 logger（回测线的 logger），直接复用：实盘日志会合并进
      回测的 backtest_full_{ts}.log，不再单独写文件。
    - 若未传（独立运行 python live_track.py），自建 logger 并写 live_track.log。
    """
    if logger is not None:
        return logger
    os.makedirs(S.OUTPUT_DIR, exist_ok=True)
    logger = logging.getLogger("live_track")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(S.LIVE_LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


def _build_ret_svg(hist: pd.DataFrame) -> str:
    """基于实盘历史表，画累计收益率% 的内联 SVG 曲线（与回测净值图同款风格，无外部依赖）。

    纵轴=累计收益率%，横轴=交易日，含 0% 基准线。
    """
    if hist is None or len(hist) == 0:
        return "<p>暂无实盘数据</p>"

    ret = hist["ret_pct"].astype(float).tolist()
    dates = hist["date"].astype(str).tolist()
    n = len(ret)

    max_v = max(ret)
    min_v = min(ret)
    # 对称留白，保证 0% 线可见
    top = max(max_v, 0.0)
    bot = min(min_v, 0.0)
    if top - bot < 1e-9:
        top += 1.0
        bot -= 1.0
    span = (top - bot) or 1.0

    W, H = 900, 300
    pad_l, pad_r, pad_t, pad_b = 60, 20, 20, 40
    plot_w = W - pad_l - pad_r
    plot_h = H - pad_t - pad_b

    def x(i):
        return pad_l + (plot_w * i / (n - 1)) if n > 1 else pad_l + plot_w / 2

    def y(v):
        return pad_t + plot_h * (1 - (v - bot) / span)

    # 网格 + Y 轴刻度（5 档）
    grid = []
    for g in range(6):
        vv = bot + span * g / 5
        yy = y(vv)
        grid.append(
            '<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#eee" stroke-width="1"/>'
            % (pad_l, yy, W - pad_r, yy)
        )
        grid.append(
            '<text x="%d" y="%.1f" fill="#888" font-size="11" text-anchor="end">%.2f%%</text>'
            % (pad_l - 6, yy + 4, vv)
        )

    # 0% 基准线
    y0 = y(0.0)
    grid.append(
        '<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#999" stroke-width="1" stroke-dasharray="4 4"/>'
        % (pad_l, y0, W - pad_r, y0)
    )

    # 曲线
    pts = " ".join("%.1f,%.1f" % (x(i), y(v)) for i, v in enumerate(ret))
    line = '<polyline fill="none" stroke="#2563eb" stroke-width="2" points="%s"/>' % pts
    # 末端点高亮
    last_x, last_y = x(n - 1), y(ret[-1])
    dot = '<circle cx="%.1f" cy="%.1f" r="3.5" fill="#2563eb"/>' % (last_x, last_y)
    last_lbl = (
        '<text x="%.1f" y="%.1f" fill="#2563eb" font-size="11" text-anchor="end">最新 %.2f%%</text>'
        % (last_x - 6, last_y - 8, ret[-1])
    )

    # X 轴首尾日期
    xlabels = []
    if n > 0:
        xlabels.append(
            '<text x="%d" y="%d" fill="#888" font-size="10" text-anchor="start">%s</text>'
            % (pad_l, H - pad_b + 28, dates[0])
        )
        if n > 1:
            xlabels.append(
                '<text x="%d" y="%d" fill="#888" font-size="10" text-anchor="end">%s</text>'
                % (W - pad_r, H - pad_b + 28, dates[-1])
            )

    svg = (
        '<svg viewBox="0 0 %d %d" width="100%%" preserveAspectRatio="xMidYMid meet" '
        'xmlns="http://www.w3.org/2000/svg">' % (W, H)
        + "".join(grid)
        + line
        + dot
        + last_lbl
        + "".join(xlabels)
        + "</svg>"
    )
    return svg


def _hist_to_html(hist: pd.DataFrame) -> str:
    """把实盘历史表转成 HTML（含收益率曲线 + 表格），供嵌入回测报告「实盘跟踪」章节。"""
    cols = [
        ("date", "日期"),
        ("QQQ_close", "QQQ价"),
        ("TQQQ_close", "TQQQ价"),
        ("shares_q", "QQQ股数"),
        ("shares_t", "TQQQ股数"),
        ("QQQ_mkt", "QQQ市值"),
        ("TQQQ_mkt", "TQQQ市值"),
        ("cash", "现金"),
        ("total", "总资产"),
        ("QQQ_volume", "QQQ成交量"),
        ("ret_pct", "收益率%"),
        ("w_q_pct", "QQQ%"),
        ("w_t_pct", "TQQQ%"),
        ("w_c_pct", "现金%"),
    ]
    int_cols = {"shares_q", "shares_t", "QQQ_volume"}
    thead = "".join(
        f'<th class="cw">{label}</th>' if key != "date" else f"<th>{label}</th>"
        for key, label in cols
    )
    body_rows = []
    for _, r in hist.iloc[::-1].iterrows():
        tds = "".join(
            (f'<td class="cw">{r[key]:,.0f}</td>'
             if key in int_cols
             else (f"<td>{r[key]}</td>" if key == "date" else f'<td class="cw">{r[key]:,.2f}</td>'))
            for key, _ in cols
        )
        # 收益率正负着色
        ret = r["ret_pct"]
        tr_cls = ' class="pos"' if ret >= 0 else ' class="neg"'
        body_rows.append(f"<tr{tr_cls}>{tds}</tr>")
    chart = _build_ret_svg(hist)
    ret_pct = float(hist["ret_pct"].iloc[-1]) if len(hist) else 0.0
    cur_total = float(hist["total"].iloc[-1]) if len(hist) else 0.0
    return (
        '<h2>实盘跟踪（真实价口径）</h2>'
        '<p class="note">以下为真实账户逐日跟踪，与上方回测（前复权 qfq 虚拟资金）相互独立。</p>'
        '<div class="live-section">'
        '<div class="chart">' + chart + '</div>'
        '<p class="note">纵轴=累计收益率%，横轴=交易日，虚线为 0% 基准。</p>'
        '<h3>建仓成本（真实价 · 固定）</h3>'
        '<table class="tbl">'
        "<thead><tr><th>标的</th><th>建仓价</th><th>股数</th><th>成本</th></tr></thead>"
        "<tbody>"
        f'<tr><td>QQQ</td><td>{S.TRACK_PRICE_QQQ:.2f}</td><td>{S.TRACK_SHARES_QQQ:.0f}</td>'
        f'<td>{S.TRACK_PRICE_QQQ * S.TRACK_SHARES_QQQ:.2f}</td></tr>'
        f'<tr><td>TQQQ</td><td>{S.TRACK_PRICE_TQQQ:.2f}</td><td>{S.TRACK_SHARES_TQQQ:.0f}</td>'
        f'<td>{S.TRACK_PRICE_TQQQ * S.TRACK_SHARES_TQQQ:.2f}</td></tr>'
        f'<tr><td>现金</td><td>-</td><td>-</td><td>{S.TRACK_CASH:.2f}</td></tr>'
        f'<tr class="pos"><td>起始本金合计</td><td>-</td><td>-</td><td>{S.TRACK_INIT_CAPITAL:.2f}</td></tr>'
        f'<tr class="pos"><td>当前市值合计</td><td>-</td><td>-</td><td>{cur_total:.2f}</td></tr>'
        f'<tr class="{"pos" if ret_pct >= 0 else "neg"}"><td>累计收益率</td><td>-</td><td>-</td>' +
        f'<td>{ret_pct:.2f}%</td></tr>'
        "</tbody></table>"
        '<h3>每日净值明细（真实价口径）</h3>'
        '<div class="scroll"><table class="tbl">'
        f"<thead><tr>{thead}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table></div>"
        "</div>"
    )


def _run_backtest_for_state(end_date: str):
    """跑一次轻量回测，只为拿到最新策略状态（df）与组合（out），供次日指令使用。

    与 main.py 回测管线一致：前复权(qfq)数据 + 指标 + 状态机 + engine.run。
    """
    raw = fetcher.fetch_and_merge(
        S.DATA_TICKERS,
        start=S.WARMUP_START,
        end=end_date,
    )
    if raw.empty:
        return None, None
    df = helpers.add_indicators(raw)
    sig = ST.generate_signals(df)
    td = sig[sig["date"] >= S.WARMUP_END].reset_index(drop=True)
    out = engine.run(td)
    return df, out


def run(logger=None) -> pd.DataFrame:
    """运行实盘跟踪线，返回实盘历史表 DataFrame（hist）。

    logger: 传入回测线的 logger 时，实盘日志合并进 backtest_full_{ts}.log；
            为 None 时独立写 live_track.log。
    不再单独写 live_track_history.csv —— 历史表由调用方嵌入回测 HTML 报告。
    """
    logger = _setup_logger(logger)
    buy_date = pd.to_datetime(S.TRACK_BUY_DATE).strftime("%Y%m%d")
    end_date = pd.Timestamp.now().strftime("%Y%m%d")

    # 建仓成本（真实价）
    cost_q = S.TRACK_SHARES_QQQ * S.TRACK_PRICE_QQQ
    cost_t = S.TRACK_SHARES_TQQQ * S.TRACK_PRICE_TQQQ
    init_capital = S.TRACK_INIT_CAPITAL

    print("=" * 64)
    print("实盘跟踪 — 独立账户真实收益率")
    print("=" * 64)
    print(f"  建仓日: {S.TRACK_BUY_DATE}")
    print(f"  建仓: QQQ {S.TRACK_SHARES_QQQ:.0f} 股 @ {S.TRACK_PRICE_QQQ:.2f}"
          f" = {cost_q:.2f} 美元")
    print(f"         TQQQ {S.TRACK_SHARES_TQQQ:.0f} 股 @ {S.TRACK_PRICE_TQQQ:.2f}"
          f" = {cost_t:.2f} 美元")
    print(f"         现金: {S.TRACK_CASH:.2f} 美元")
    print(f"  投入本金: {init_capital:.2f} 美元")
    print(f"  目标比例: QQQ/TQQQ 各 {(1 - S.TRACK_CASH_PCT) / 2 * 100:.1f}% |"
          f" 现金 {S.TRACK_CASH_PCT * 100:.1f}%")
    print()

    # 拉建仓日起的真实价（含建仓日当天）
    px_q = _load_real_prices(S.DATA_TICKERS[0], buy_date, end_date)
    px_t = _load_real_prices(S.DATA_TICKERS[1], buy_date, end_date)
    if px_q.empty or px_t.empty:
        print("[错误] 无法获取真实价数据，请检查网络/数据源。")
        return

    # 对齐交易日
    dates = px_q.index.intersection(px_t.index)
    px_q = px_q.reindex(dates)
    px_t = px_t.reindex(dates)

    # QQQ 当日成交量（不复权，与逃顶量能口径一致）
    vol_q = fetcher.fetch_ticker(S.DATA_TICKERS[0], start=buy_date, end=end_date, adjust="")
    if not vol_q.empty:
        vol_q = vol_q.copy()
        vol_q["date"] = pd.to_datetime(vol_q["date"]).dt.normalize()
        vol_q = vol_q.set_index("date")["Volume"].reindex(dates)

    # 逐日计算组合市值与收益率
    rows = []
    for d in dates:
        mkt_q = S.TRACK_SHARES_QQQ * px_q[d]
        mkt_t = S.TRACK_SHARES_TQQQ * px_t[d]
        total = mkt_q + mkt_t + S.TRACK_CASH
        ret = total / init_capital - 1
        w_q = mkt_q / total if total > 0 else 0
        w_t = mkt_t / total if total > 0 else 0
        w_c = S.TRACK_CASH / total if total > 0 else 0
        rows.append({
            "date": d.strftime("%Y-%m-%d"),
            "QQQ_close": px_q[d],
            "TQQQ_close": px_t[d],
            "QQQ_volume": vol_q[d] if (not vol_q.empty and d in vol_q.index) else 0.0,
            "shares_q": S.TRACK_SHARES_QQQ,
            "shares_t": S.TRACK_SHARES_TQQQ,
            "QQQ_mkt": mkt_q,
            "TQQQ_mkt": mkt_t,
            "cash": S.TRACK_CASH,
            "total": total,
            "ret_pct": ret * 100,
            "w_q_pct": w_q * 100,
            "w_t_pct": w_t * 100,
            "w_c_pct": w_c * 100,
        })
    hist = pd.DataFrame(rows)

    # 打印每日表
    print("逐日市值 / 收益率:")
    print("-" * 64)
    print(f"{'日期':12} {'QQQ价':>9} {'TQQQ价':>9} {'总资产':>11} {'收益率%':>9}")
    print("-" * 64)
    for _, r in hist.iterrows():
        print(f"{r['date']:12} {r['QQQ_close']:>9.2f} {r['TQQQ_close']:>9.2f}"
              f" {r['total']:>11.2f} {r['ret_pct']:>8.2f}%")

    # 最新状态
    last = hist.iloc[-1]
    target_w = (1 - S.TRACK_CASH_PCT) / 2
    off_q = abs(last["w_q_pct"] / 100 - target_w)
    off_t = abs(last["w_t_pct"] / 100 - target_w)

    print()
    print("=" * 64)
    print("当前状态（最新交易日）")
    print("=" * 64)
    print(f"  最新交易日: {last['date']}")  # last['date'] 已是 'YYYY-MM-DD' 字符串
    print(f"  QQQ 真实价: {last['QQQ_close']:.2f} | TQQQ 真实价: {last['TQQQ_close']:.2f}")
    print(f"  当前市值: {last['total']:.2f} 美元 | 累计收益率: {last['ret_pct']:+.2f}%")
    print(f"  当前持仓占比: QQQ {last['w_q_pct']:.1f}% + TQQQ {last['w_t_pct']:.1f}%"
          f" + 现金 {last['w_c_pct']:.1f}%")
    print(f"  目标占比: QQQ {target_w * 100:.1f}% + TQQQ {target_w * 100:.1f}%"
          f" + 现金 {S.TRACK_CASH_PCT * 100:.1f}%")

    # 调仓判定（轻量，仅基于真实价占比）
    print()
    if off_q <= S.NORMAL_REBAL_DEV and off_t <= S.NORMAL_REBAL_DEV:
        print(f"  调仓建议: 无需再平衡（QQQ/TQQQ 偏离均 <= {S.NORMAL_REBAL_DEV * 100:.0f}%）")
    else:
        print(f"  调仓建议: 触发再平衡（偏离 > {S.NORMAL_REBAL_DEV * 100:.0f}%）")
        target_total = last["total"]
        tgt_q_val = target_total * target_w
        tgt_t_val = target_total * target_w
        tgt_q_shares = tgt_q_val / last["QQQ_close"]
        tgt_t_shares = tgt_t_val / last["TQQQ_close"]
        dq = tgt_q_shares - S.TRACK_SHARES_QQQ
        dt = tgt_t_shares - S.TRACK_SHARES_TQQQ
        if abs(dq) >= 1e-6:
            act = "买入" if dq > 0 else "卖出"
            print(f"    {act} QQQ: {abs(dq):+.2f} 股"
                  f" (目标 {tgt_q_shares:.2f} 股, 当前 {S.TRACK_SHARES_QQQ:.0f} 股)")
        if abs(dt) >= 1e-6:
            act = "买入" if dt > 0 else "卖出"
            print(f"    {act} TQQQ: {abs(dt):+.2f} 股"
                  f" (目标 {tgt_t_shares:.2f} 股, 当前 {S.TRACK_SHARES_TQQQ:.0f} 股)")
        print(f"    （按最新真实价估算，实际下单请以实时价微调）")

    # ---- 次日操作指令（复用回测引擎状态 + TRACK_* 真实账户）----
    print()
    print("#" * 64)
    print("# 次日操作指令（基于回测策略状态 + 实盘跟踪线 TRACK_*）")
    print("#" * 64)
    df, out = _run_backtest_for_state(end_date)
    if df is None or out is None:
        print("[错误] 回测引擎数据获取失败，无法生成次日指令。")
        return hist, ""
    # 捕获次日切换地图文本，供回测报告生成思维导图
    import io
    import logging
    _buf = io.StringIO()
    _cap_h = logging.StreamHandler(_buf)
    _cap_h.setLevel(logging.INFO)
    _orig_propagate = logger.propagate
    logger.addHandler(_cap_h)
    try:
        emit_next_state_guide(logger, df, S.WARMUP_END)
    finally:
        logger.removeHandler(_cap_h)
        logger.propagate = _orig_propagate
    _guide_text = _buf.getvalue()
    emit_live_signal(logger, df, S.WARMUP_END, out)

    return hist, _guide_text


if __name__ == "__main__":
    run()
