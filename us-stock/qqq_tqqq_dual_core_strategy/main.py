#!/usr/bin/env python3
"""
QQQ & TQQQ 双核策略 - 标准回测入口。
运行: python main.py
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
    # 通用日志（固定文件名，保留历史不易混淆）
    fh = logging.FileHandler(S.LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    # 本次运行的同名 log（与 backtest_full_{ts}.csv 配对）
    if run_log_path:
        rfh = logging.FileHandler(run_log_path, encoding="utf-8")
        rfh.setFormatter(fmt)
        logger.addHandler(rfh)
    # 同时打到终端
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


# ---- 实盘跟踪：状态 -> 实盘动作描述 ----
LIVE_ACTION = {
    "NORMAL": "持有 QQQ+TQQQ (各45%, 留10%现金)",
    "ZONE_BATTLE_ATTACK": "持有 TQQQ (99%)",
    "ZONE_DESPAIR_TQQQ": "持有 TQQQ (99%)",
    "ZONE_BATTLE_DEFEND": "切换为 QQQ (90%), 清空 TQQQ",
    "TOP_ESCAPE": "切换为 QQQ (90%), 清空 TQQQ (逃顶)",
    "BEAR_CASH": "清空全部, 持有现金 (100%)",
}


def _round_lot(qty):
    # 与 backtest/engine.py 一致：ROUND_LOTS=True 时取整股（向下取整），
    # 否则保留 2 位小数（碎股）。默认 settings.ROUND_LOTS=True -> 整股。
    if getattr(S, "ROUND_LOTS", True):
        return int(qty)
    return round(qty, 2)


def _emit_next_state_guide(logger, df, trade_start):
    """基于已算好的 df，输出次日状态切换地图与限制说明。"""
    sig = ST.generate_signals(df)
    s = sig.iloc[-1]
    last_date = pd.Timestamp(s["date"]).date()

    close = float(s["QQQ_Close"])
    ath = float(s["QQQ_ATH"])
    ma200 = float(s["QQQ_MA200"])
    ma20 = float(s["QQQ_MA20"])
    volma = float(s["QQQ_VolMA"])
    drawdown = (close / ath - 1.0) if ath > 0 else 0.0

    # 后复权价 -> 真实市价 换算系数（用于切换地图里标注真实盯盘价，避免与同花顺 724 混淆）
    real_q = fetcher.latest_raw_close(S.DATA_TICKERS[0])
    k = (real_q / close) if close > 0 else 1.0

    logger.info("")
    logger.info("=" * 68)
    logger.info("实盘跟踪 - 次日状态切换指导")
    logger.info("=" * 68)
    logger.info("[最新交易日] %s" % last_date)
    logger.info(
        "  QQQ 收盘: %.2f | ATH: %.2f | MA200: %.2f | MA20: %.2f" % (close, ath, ma200, ma20)
    )
    logger.info(
        "  QQQ 真实市价(不复权, 盯盘用): %.2f (后复权≈%.2f 的约 %.1f%%)" % (real_q, close, k * 100)
    )
    logger.info("  当前回撤: %.2f%%" % (drawdown * 100))
    logger.info("  当前状态: %s  ->  当前动作: %s" % (s["state"], LIVE_ACTION.get(s["state"], "?")))
    logger.info(
        "  QQQ 成交量均线 VolMA(前%d日): %.0f  ->  逃顶量能阈值 = VolMA×%.1f = %.0f"
        % (S.VOL_WINDOW, volma, S.VOL_FACTOR, volma * S.VOL_FACTOR)
    )

    logger.info("\n[次日切换地图] 只要 QQQ 价格满足下列价能条件，即切换到对应状态：")
    logger.info("-" * 68)

    high_zone_line = ath * S.HIGH_ZONE  # 逃顶价线
    dd10_line = ath * (1 - 0.10)  # 回撤 -10% 线
    dd30_line = ath * (1 - 0.30)  # 回撤 -30% 线

    lines = []
    lines.append(
        (
            "逃顶 TOP_ESCAPE",
            "QQQ 收盘 >= %.2f (后复权≈%.2f) (ATH×%.2f)"
            % (high_zone_line * k, high_zone_line, S.HIGH_ZONE),
            "还需: 当日成交量>%.0f(VolMA×%.1f) 且 收阴(close<open)"
            % (volma * S.VOL_FACTOR, S.VOL_FACTOR),
            LIVE_ACTION["TOP_ESCAPE"],
        )
    )
    if close >= ma200:
        lines.append(
            (
                "跌破 MA200 -> 风险区",
                "QQQ 收盘 < %.2f (后复权≈%.2f) (MA200)" % (ma200 * k, ma200),
                "进入后按回撤分三档(见下)",
                LIVE_ACTION["BEAR_CASH"],
            )
        )
        lines.append(
            (
                "  其中 回撤>-10%%",
                "  且 %.2f <= QQQ收盘 < %.2f (后复权≈%.2f)" % (dd10_line * k, ma200 * k, ma200),
                "  -> BEAR_CASH",
                LIVE_ACTION["BEAR_CASH"],
            )
        )
        lines.append(
            (
                "  其中 -30%%<=回撤<=-10%% 且 close<MA20",
                "  QQQ收盘<=%s 且 QQQ收盘<%.2f(MA20) (后复权≈%.2f)"
                % (dd10_line * k, ma20, dd10_line),
                "  -> ZONE_BATTLE_DEFEND",
                LIVE_ACTION["ZONE_BATTLE_DEFEND"],
            )
        )
        lines.append(
            (
                "  其中 -30%%<=回撤<=-10%% 且 close>MA20",
                "  %s<=QQQ收盘<%s 且 QQQ收盘>%.2f(MA20) (后复权≈%.2f)"
                % (dd10_line * k, ma200 * k, ma20, ma200),
                "  -> ZONE_BATTLE_ATTACK",
                LIVE_ACTION["ZONE_BATTLE_ATTACK"],
            )
        )
        lines.append(
            (
                "  其中 回撤<=-30%%",
                "  QQQ收盘 < %s (后复权≈%.2f)" % (dd30_line * k, dd30_line),
                "  -> ZONE_DESPAIR_TQQQ",
                LIVE_ACTION["ZONE_DESPAIR_TQQQ"],
            )
        )
    else:
        lines.append(
            (
                "站回 MA200 -> 回 NORMAL/ATTACK",
                "QQQ 收盘 > %s (后复权≈%.2f) (MA200)" % (ma200 * k, ma200),
                "回撤<-10%%则 ATTACK(TQQQ), 否则 NORMAL(QQQ+TQQQ)",
                LIVE_ACTION["NORMAL"],
            )
        )
        lines.append(
            (
                "回撤 -10%% 线",
                "QQQ 收盘 %s (后复权≈%.2f) (ATH×0.90)" % (dd10_line * k, dd10_line),
                "上方且>MA200 -> NORMAL; 下方且<MA200 进入风险区分档",
                LIVE_ACTION["NORMAL"],
            )
        )
        lines.append(
            (
                "回撤 -30%% 线",
                "QQQ 收盘 %s (后复权≈%.2f) (ATH×0.70)" % (dd30_line * k, dd30_line),
                "下方 -> ZONE_DESPAIR_TQQQ",
                LIVE_ACTION["ZONE_DESPAIR_TQQQ"],
            )
        )
    lines.append(
        (
            "NORMAL 日内再平衡",
            "状态不变, 但 QQQ/TQQQ 任一侧偏离目标>%.0f%%" % (S.NORMAL_REBAL_DEV * 100),
            "当日卖出偏离侧、买入补回 45/45",
            LIVE_ACTION["NORMAL"],
        )
    )

    for title, price_cond, extra, act in lines:
        logger.info("  • %s" % title)
        logger.info("      价能条件: %s" % price_cond)
        logger.info("      附加条件: %s" % extra)
        logger.info("      动作    : %s" % act)
        logger.info("")

    # ---- 限制说明（明确打印，供复盘）----
    logger.info("-" * 68)
    logger.info("[限制与注意事项]")
    logger.info("  1) 逃顶 TOP_ESCAPE 的量能条件需次日确认，但可盘中估算：")
    logger.info(
        "     已知 VolMA=%.0f，故量能阈值=%.0f（VolMA×%.1f）。"
        % (volma, volma * S.VOL_FACTOR, S.VOL_FACTOR)
    )
    logger.info("     「次日实际成交量」未知，但可在盘中自行估算：")
    logger.info("       估算全天量 ≈ 当前时点成交量 ÷ 当日已过交易时间占比")
    logger.info("                  （例如已过半天，则 ×2 粗略外推；美股本日共6.5小时）")
    logger.info(
        "     若估算全天量 > %.0f 且 QQQ 收盘价>=%s (后复权≈%.2f) 且 收阴(close<open)，"
        % (volma * S.VOL_FACTOR, high_zone_line * k, high_zone_line)
    )
    logger.info("     则触发逃顶，状态切为 TOP_ESCAPE；否则为假突破，不切换。")
    logger.info(
        "     （状态切换当日只卖变现，买入按 T+1 规则推迟到再下一个交易日执行；请勿切换当日满仓买入）"
    )
    logger.info("     临近收盘时估算最准，请勿仅凭价格到线就提前逃顶。")
    logger.info("  2) 所有切换判定均基于 QQQ 价格（基准标的），盯 QQQ 即可，")
    logger.info("     TQQQ 为杠杆跟随，按对应权重比例调仓，无需单独看 TQQQ 价位。")
    logger.info("  3) 价能临界点为基于「最新交易日」指标的推算，次日指标(MA20/MA200/ATH)")
    logger.info("     会随新收盘价微调，临界线以当日实际收盘后重算为准。")
    logger.info("  4) 本指导使用正式参数 VOL_FACTOR=%.1f。" % S.VOL_FACTOR)
    logger.info("=" * 68)


def _emit_live_signal(logger, df, trade_start, out):
    """基于回测输出 out，输出次日再平衡指令（股数按最后一天收盘价估算）。"""
    sig = ST.generate_signals(df)
    s_last = sig.iloc[-1]
    state = s_last["state"]
    w_q = s_last["w_QQQ"]
    w_t = s_last["w_TQQQ"]

    p_last = out.portfolio.iloc[-1]
    total = float(p_last["portfolio_value"])
    px_q = float(p_last["QQQ_Close"])  # 回测价（后复权 hfq，用于策略/净值）
    px_t = float(p_last["TQQQ_Close"])  # 回测价（后复权 hfq，用于策略/净值）
    cur_q = float(p_last["shares_QQQ"])
    cur_t = float(p_last["shares_TQQQ"])
    cur_cash = float(p_last["cash"])

    # 实盘股数用「最新真实市价」估算（券商挂单价），与回测后复权净值口径分离。
    # 回测必须用后复权(hfq)：东财 TQQQ 不复权序列有拆股拼接 bug（首行167为拆股前高价），
    # 直接算历史涨幅会错成“跌60%”，后复权才正确给出 ~180 倍累计涨幅。
    real_q = fetcher.latest_raw_close(S.DATA_TICKERS[0])
    real_t = fetcher.latest_raw_close(S.DATA_TICKERS[1])
    if real_q <= 0:
        real_q = px_q
    if real_t <= 0:
        real_t = px_t

    # 实盘账户总资产按真实市价重算（你的券商账户真金白银），用于估算可下单股数。
    # 注意：偏离判断必须用回测同口径（后复权 px + total），不能用真实市价。
    # 原因：现金不随价格口径缩放，真实市价下现金占比被放大，会扭曲 QQQ/TQQQ 占比，
    # 导致“回测不动、实盘要动”的分裂。故偏离判定复刻 engine 的后复权口径，
    # 而股数估算用真实市价（real_total），两者分离、各自自洽。
    mkt_q = cur_q * real_q
    mkt_t = cur_t * real_t
    real_total = mkt_q + mkt_t + cur_cash
    # 回测同口径占比（后复权持仓市值 / 后复权总资产）
    back_w_q = (cur_q * px_q) / total if total > 0 else 0
    back_w_t = (cur_t * px_t) / total if total > 0 else 0

    target_q_val = real_total * w_q
    target_t_val = real_total * w_t
    target_q_shares = _round_lot(target_q_val / real_q) if real_q > 0 else 0.0
    target_t_shares = _round_lot(target_t_val / real_t) if real_t > 0 else 0.0
    delta_q = _round_lot(target_q_shares - cur_q)
    delta_t = _round_lot(target_t_shares - cur_t)

    logger.info("")
    logger.info("=" * 64)
    logger.info("实盘跟踪 - 次日操作指令")
    logger.info("=" * 64)
    logger.info("[最新交易日] %s" % pd.Timestamp(s_last["date"]).date())
    logger.info("  最新价(不复权真实市价): QQQ %.2f | TQQQ %.2f" % (real_q, real_t))
    logger.info("  最后一天状态: %s | 目标权重 QQQ=%.2f  TQQQ=%.2f" % (state, w_q, w_t))
    logger.info("  账户总资产(真实市价): %.2f 美元（持仓市值+现金，非回测后复权口径）" % real_total)
    logger.info(
        "  当前持仓(按真实市价): QQQ %.2f 股(市值 %.2f) | TQQQ %.2f 股(市值 %.2f) | 现金 %.2f"
        % (cur_q, mkt_q, cur_t, mkt_t, cur_cash)
    )
    logger.info(
        "  当前占比(回测后复权口径, 用于再平衡判定): QQQ %.2f%% | TQQQ %.2f%%"
        % (back_w_q * 100, back_w_t * 100)
    )

    logger.info("次日操作指令（股数按真实市价估算，实际下单请以次日实时价微调）:")
    logger.info("-" * 64)
    logger.info(
        "  当前状态: [%s] | 目标权重: QQQ=%.0f%% / TQQQ=%.0f%%" % (state, w_q * 100, w_t * 100)
    )

    # 金额单位统一为「美元」，避免与人民币「元」混淆（标的为美股 QQQ/TQQQ）
    def _act(delta, ticker, px):
        if abs(delta) < 1e-6:
            return "    维持 %s 不变" % ticker
        if delta > 0:
            return "    买入 %s: +%s 股 (约 %.2f 美元，按真实市价 %.2f 估算)" % (
                ticker,
                delta,
                delta * px,
                px,
            )
        return "    卖出 %s: %s 股 (约 %.2f 美元，按真实市价 %.2f 估算)" % (
            ticker,
            delta,
            -delta * px,
            px,
        )

    if w_q == 0 and w_t == 0:
        # 目标全现金：仅当仍有持仓时才需卖出，已空仓则无需操作
        logger.info("  次日动作: 清仓为现金（BEAR_CASH）")
        if cur_q > 0 or cur_t > 0:
            logger.info("    次日开盘卖出全部持仓，持有现金：")
            if cur_q > 0:
                logger.info("    卖出 QQQ: %.2f 股" % cur_q)
            if cur_t > 0:
                logger.info("    卖出 TQQQ: %.2f 股" % cur_t)
        else:
            logger.info("    当前已为全现金，无需操作。")
    else:
        logger.info("  目标持仓（按真实市价估算，总资产 %.2f 美元）:" % real_total)
        logger.info("    QQQ 目标: %.2f 股 (目标市值 %.2f 美元)" % (target_q_shares, target_q_val))
        logger.info("    TQQQ 目标: %.2f 股 (目标市值 %.2f 美元)" % (target_t_shares, target_t_val))
        # 与回测 engine 对齐：NORMAL 内仅当 QQQ/TQQQ 市值偏离超过阈值(20%)才再平衡；
        # 状态切换导致的买卖已在上方「次日状态切换指导」中说明，此处不重复。
        off_q = abs(back_w_q - w_q)
        off_t = abs(back_w_t - w_t)
        if off_q <= S.NORMAL_REBAL_DEV and off_t <= S.NORMAL_REBAL_DEV:
            logger.info("  次日动作: 无需再平衡")
            logger.info(
                "    当前 QQQ/TQQQ 市值占比偏离均 <= %.0f%%，未达再平衡阈值。"
                % (S.NORMAL_REBAL_DEV * 100)
            )
            logger.info(
                "    若上方「状态切换指导」判定次日需切换状态，请按其指示执行"
                "(切换当日只卖、次交易日再买)；否则持仓不动。"
            )
        else:
            logger.info(
                "  次日动作: 触发再平衡（QQQ/TQQQ 市值偏离 > %.0f%%）" % (S.NORMAL_REBAL_DEV * 100)
            )
            if w_q > 0:
                logger.info(_act(delta_q, "QQQ", real_q))
            else:
                if cur_q > 0:
                    logger.info("    卖出 QQQ: %.2f 股 (清空)" % cur_q)
                else:
                    logger.info("    QQQ 目标权重 0，已空，无需操作。")
            if w_t > 0:
                logger.info(_act(delta_t, "TQQQ", real_t))
            else:
                if cur_t > 0:
                    logger.info("    卖出 TQQQ: %.2f 股 (清空)" % cur_t)
                else:
                    logger.info("    TQQQ 目标权重 0，已空，无需操作。")
    logger.info("-" * 64)
    logger.info(
        "说明: 上述股数按最新真实市价(不复权)估算，实际下单请以次日实时价微调；金额单位均为美元。"
    )
    logger.info(
        "      回测净值用后复权价(hfq，正确处理 TQQQ 拆股，收益更准)；本指令的总资产/市值/股数"
    )
    logger.info("      均按真实市价计算（券商账户实际计价），与后复权净值口径分离、互不污染。")
    logger.info("      关于T+1防融资规则: 若上方「状态切换指导」判定次日需切换状态，")
    logger.info(
        "      切换当日只执行卖出变现，买入推迟到再下一个交易日(资金结算后)按目标权重执行；"
    )
    logger.info("      故状态切换当日先卖、次交易日再买，请勿在切换当日满仓买入。")


def main(start=None, warmup_start=None, end=None, live=False):
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

    # 8. 实盘跟踪指导（默认关闭，--live 开启）：复用已算好的 df / out，不重复拉数据
    live_text = ""
    if live:
        import io

        _buf = io.StringIO()
        _cap = logging.StreamHandler(_buf)
        _cap.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(_cap)
        try:
            _emit_next_state_guide(logger, df, trade_start)
            _emit_live_signal(logger, df, trade_start, out)
        finally:
            logger.removeHandler(_cap)
        live_text = _buf.getvalue()

    # 9. 生成 HTML 报告（与 CSV 同目录、同时间戳配对，方便直观查看）
    try:
        from backtest.report_html import build_report

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
            run_ts,
            live,
            live_text,
        )
        logger.info(f"HTML报告: {html_path}")
    except Exception as e:
        import traceback as _tb

        logger.warning("HTML 报告生成失败: %s\n%s", e, _tb.format_exc())


def pd_today():
    import datetime

    return datetime.date.today().isoformat()


def pd_T(s):
    return s


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="QQQ & TQQQ 双核策略回测")
    parser.add_argument("--start", default=None, help="交易起点日期 (默认读 settings.WARMUP_END)")
    parser.add_argument(
        "--warmup-start",
        default=None,
        help="预热起点日期，用于指标计算 (默认读 settings.WARMUP_START)",
    )
    parser.add_argument("--end", default=None, help="回测终点日期 (默认今天)")
    parser.add_argument(
        "--live", action="store_true", help="额外输出实盘跟踪指导（次日状态切换地图 + 再平衡指令）"
    )
    args = parser.parse_args()
    main(start=args.start, warmup_start=args.warmup_start, end=args.end, live=args.live)
