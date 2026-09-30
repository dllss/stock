"""实盘跟踪线的次日操作指令（状态切换指导 + 再平衡指令）。

从 main.py 抽取而来，供 live_track.py 单独调用；
main.py 现在只负责回测，不再输出实盘指导。
"""

import logging

import pandas as pd

from config.settings import S
from data import fetcher
from strategy import strategy as ST

# ---- 实盘跟踪：状态 -> 实盘动作描述 ----
def _action_for_state(state):
    weights = ST.target_weights(state)
    q_pct = int(weights["QQQ"] * 100)
    t_pct = int(weights["TQQQ"] * 100)
    cash_pct = 100 - q_pct - t_pct
    if q_pct == 0 and t_pct == 0:
        return "清空全部, 持有现金 (100%)"
    if q_pct == 0:
        return "持有 TQQQ (%d%%), 现金 %d%%" % (t_pct, cash_pct)
    if t_pct == 0:
        return "持有 QQQ (%d%%), 现金 %d%%" % (q_pct, cash_pct)
    return "持有 QQQ (%d%%) + TQQQ (%d%%), 现金 %d%%" % (q_pct, t_pct, cash_pct)


LIVE_ACTION = {
    state: _action_for_state(state)
    for state in (
        "NORMAL",
        "HI",
        "HI_CASH",
        "ZONE_BATTLE_ATTACK",
        "ZONE_DESPAIR_TQQQ",
        "ZONE_BATTLE_DEFEND",
        "TOP_ESCAPE",
        "BEAR_CASH",
    )
}


def _round_lot(qty):
    # 与 backtest/engine.py 一致：ROUND_LOTS=True 时取整股（向下取整），
    # 否则保留 2 位小数（碎股）。默认 settings.ROUND_LOTS=True -> 整股。
    if getattr(S, "ROUND_LOTS", True):
        return int(qty)
    return round(qty, 2)


def emit_next_state_guide(logger, df, trade_start):
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

    # 回测价(qfq) -> 真实市价 换算系数（用于切换地图里标注真实盯盘价，避免与同花顺 724 混淆）
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
        "  QQQ 真实市价(不复权, 盯盘用): %.2f (前复权~%.2f 的约 %.1f%%)" % (real_q, close, k * 100)
    )
    logger.info("  当前回撤: %.2f%%" % (drawdown * 100))
    logger.info("  当前状态: %s  ->  当前动作: %s" % (s["state"], LIVE_ACTION.get(s["state"], "?")))
    logger.info("  当日QQQ成交量: %.0f" % float(s.get("QQQ_Volume", 0.0) or 0.0))
    logger.info(
        "  QQQ 成交量均线 VolMA(前%d日): %.0f  ->  逃顶量能阈值 = VolMA*%.1f = %.0f"
        % (S.VOL_WINDOW, volma, S.VOL_FACTOR, volma * S.VOL_FACTOR)
    )

    logger.info("\n[次日切换地图] 只要 QQQ 价格满足下列价能条件，即切换到对应状态：")
    logger.info("-" * 68)

    high_zone_line = ath * S.HIGH_ZONE  # 逃顶价线（前复权 qfq）
    dd10_line = ath * (1 - 0.10)  # 回撤 -10% 线（前复权 qfq）
    dd30_line = ath * (1 - 0.30)  # 回撤 -30% 线（前复权 qfq）

    def _price(real, hfq, tag=""):
        return "%.2f (含义: %s，前复权: %.2f)" % (real, tag, hfq)

    r_ma200 = ma200 * k
    r_ma20 = ma20 * k
    r_hz = high_zone_line * k
    r_dd10 = dd10_line * k
    r_dd30 = dd30_line * k

    lines = []
    lines.append(
        (
            "逃顶 TOP_ESCAPE",
            "QQQ 收盘 >= %s" % _price(r_hz, high_zone_line, "ATH*%.2f" % S.HIGH_ZONE),
            "还需: 当日QQQ成交量>%.0f(VolMA*%.1f) 且 收阴(close<open)"
            % (volma * S.VOL_FACTOR, S.VOL_FACTOR),
            LIVE_ACTION["TOP_ESCAPE"],
        )
    )
    if close >= ma200:
        lines.append(
            (
                "跌破 MA200 -> 风险区",
                "QQQ 收盘 < %s" % _price(r_ma200, ma200, "MA200"),
                "进入后按回撤分两档(见下)",
                LIVE_ACTION["BEAR_CASH"],
            )
        )
        lines.append(
            (
                "  -> ZONE_BATTLE_DEFEND",
                "%s <= QQQ收盘 < %s"
                % (_price(r_dd30, dd30_line, "ATH*0.70"), _price(r_dd10, dd10_line, "ATH*0.90")),
                "",
                LIVE_ACTION["ZONE_BATTLE_DEFEND"],
                "-30%<=回撤<=-10% 且 close<MA20",
            )
        )
        lines.append(
            (
                "  -> ZONE_DESPAIR_TQQQ",
                "QQQ收盘 < %s" % _price(r_dd30, dd30_line, "ATH*0.70"),
                "",
                LIVE_ACTION["ZONE_DESPAIR_TQQQ"],
                "回撤<=-30%",
            )
        )
    else:
        lines.append(
            (
                "站回 MA200 -> 回 NORMAL/ATTACK",
                "QQQ 收盘 > %s" % _price(r_ma200, ma200, "MA200"),
                "回撤<-10%%则 ATTACK(TQQQ), 否则 NORMAL(%s)" % LIVE_ACTION["NORMAL"],
                LIVE_ACTION["NORMAL"],
            )
        )
        lines.append(
            (
                "回撤 -10%% 线",
                "QQQ 收盘 %s" % _price(r_dd10, dd10_line, "ATH*0.90"),
                "上方且>MA200 -> NORMAL(%s); 下方且<MA200 进入风险区分档"
                % LIVE_ACTION["NORMAL"],
                LIVE_ACTION["NORMAL"],
            )
        )
        lines.append(
            (
                "回撤 -30%% 线",
                "QQQ 收盘 %s" % _price(r_dd30, dd30_line, "ATH*0.70"),
                "下方 -> ZONE_DESPAIR_TQQQ",
                LIVE_ACTION["ZONE_DESPAIR_TQQQ"],
            )
        )
    normal_weights = ST.target_weights("NORMAL")
    if (
        getattr(S, "NORMAL_REBAL_ENABLED", True)
        and normal_weights["QQQ"] > 0
        and normal_weights["TQQQ"] > 0
    ):
        lines.append(
            (
                "NORMAL 日内再平衡",
                "状态不变, 但 QQQ/TQQQ 任一侧偏离目标>%.0f%%"
                % (S.NORMAL_REBAL_DEV * 100),
                "当日卖出偏离侧、买入补回目标权重",
                LIVE_ACTION["NORMAL"],
            )
        )

    for row in lines:
        title, price_cond, extra, act = row[0], row[1], row[2], row[3]
        cond_note = row[4] if len(row) > 4 else ""
        logger.info("  - %s" % title)
        if cond_note:
            logger.info("      条件说明: %s" % cond_note)
        logger.info("      价能条件: %s" % price_cond)
        if extra:
            logger.info("      附加条件: %s" % extra)
        logger.info("      执行动作: %s" % act)
        logger.info("")

    logger.info("-" * 68)
    logger.info("[限制与注意事项]")
    logger.info("  1) 逃顶 TOP_ESCAPE 的量能条件需次日确认，但可盘中估算：")
    logger.info(
        "     已知 VolMA=%.0f，故量能阈值=%.0f（VolMA*%.1f）。"
        % (volma, volma * S.VOL_FACTOR, S.VOL_FACTOR)
    )
    logger.info("     「次日实际成交量」未知，但可在盘中自行估算：")
    logger.info("       估算全天量 ~ 当前时点成交量 ÷ 当日已过交易时间占比")
    logger.info("                  （例如已过半天，则 *2 粗略外推；美股本日共6.5小时）")
    logger.info(
        "     若估算全天QQQ成交量 > %.0f 且 QQQ 收盘价>=%.2f (前复权~%.2f) 且 收阴(close<open)，"
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


def emit_live_signal(logger, df, trade_start, out):
    """基于回测输出 out，输出次日再平衡指令（股数按最后一天收盘价估算）。"""
    sig = ST.generate_signals(df)
    s_last = sig.iloc[-1]
    state = s_last["state"]
    w_q = s_last["w_QQQ"]
    w_t = s_last["w_TQQQ"]

    p_last = out.portfolio.iloc[-1]
    total = float(p_last["portfolio_value"])
    px_q = float(p_last["QQQ_Close"])  # 回测价（前复权 qfq，用于策略/净值）
    px_t = float(p_last["TQQQ_Close"])  # 回测价（前复权 qfq，用于策略/净值）

    # 真实账户数据源来自实盘跟踪线配置 TRACK_*（与 live_track.py 同源）
    _use_real = True  # 真实账户固定以 TRACK_* 为准
    cur_q = S.TRACK_SHARES_QQQ
    cur_t = S.TRACK_SHARES_TQQQ
    cur_cash = S.TRACK_CASH

    # 实盘股数用「最新真实市价」估算（券商挂单价），与回测前复权(qfq)净值口径分离。
    real_q = fetcher.latest_raw_close(S.DATA_TICKERS[0])
    real_t = fetcher.latest_raw_close(S.DATA_TICKERS[1])
    if real_q <= 0:
        real_q = px_q
    if real_t <= 0:
        real_t = px_t

    mkt_q = cur_q * real_q
    mkt_t = cur_t * real_t
    real_total = mkt_q + mkt_t + cur_cash
    # 回测同口径占比（前复权 qfq 持仓市值 / 前复权 qfq 总资产）
    back_w_q = (cur_q * px_q) / total if total > 0 else 0
    back_w_t = (cur_t * px_t) / total if total > 0 else 0

    # live 用真实账户时，目标权重按 TRACK_CASH_PCT 重算（QQQ/TQQQ 各 (1-cash)/2）
    if _use_real and w_q > 0 and w_t > 0:
        w_q = w_t = (1.0 - S.TRACK_CASH_PCT) / 2.0

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
    logger.info(
        "  账户总资产(真实市价): %.2f 美元（持仓市值+现金，非回测前复权 qfq 口径）" % real_total
    )
    if _use_real:
        _w_q_real = (mkt_q / real_total * 100) if real_total > 0 else 0
        _w_t_real = (mkt_t / real_total * 100) if real_total > 0 else 0
        logger.info(
            "  [实盘跟踪线] 持仓取自 TRACK_* 配置: QQQ %.2f 股 / TQQQ %.2f 股 / 现金 %.2f 美元"
            % (cur_q, cur_t, cur_cash)
        )
    logger.info(
        "  当前持仓(按真实市价): QQQ %.2f 股(市值 %.2f) | TQQQ %.2f 股(市值 %.2f) | 现金 %.2f"
        % (cur_q, mkt_q, cur_t, mkt_t, cur_cash)
    )
    if _use_real:
        logger.info(
            "  当前占比(真实价口径, 用于再平衡判定): QQQ %.2f%% | TQQQ %.2f%%"
            % (_w_q_real, _w_t_real)
        )
    else:
        logger.info(
            "  当前占比(回测前复权 qfq 口径, 用于再平衡判定): QQQ %.2f%% | TQQQ %.2f%%"
            % (back_w_q * 100, back_w_t * 100)
        )
    _w_q_real = (mkt_q / real_total * 100) if real_total > 0 else 0
    _w_t_real = (mkt_t / real_total * 100) if real_total > 0 else 0
    _w_cash_real = (cur_cash / real_total * 100) if real_total > 0 else 0
    logger.info(
        "  当前实际持仓状态(真实价口径)：QQQ %.1f%% + TQQQ %.1f%% + 现金 %.1f%%"
        % (_w_q_real, _w_t_real, _w_cash_real)
    )

    logger.info("次日操作指令（股数按真实市价估算，实际下单请以次日实时价微调）:")
    logger.info("-" * 64)
    logger.info(
        "  当前状态: [%s] | 目标权重: QQQ=%.0f%% / TQQQ=%.0f%%" % (state, w_q * 100, w_t * 100)
    )

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
        off_q = abs(_w_q_real / 100 - w_q) if _use_real else abs(back_w_q - w_q)
        off_t = abs(_w_t_real / 100 - w_t) if _use_real else abs(back_w_t - w_t)
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
        "      回测净值用前复权价(qfq，锚定最新日=实盘真实价，且正确规避 TQQQ 拆股跳空)；本指令的总资产/市值/股数"
    )
    logger.info("      均按真实市价计算（券商账户实际计价），与回测净值口径分离、互不污染。")
    logger.info("      关于T+1防融资规则(与 base 自动执行层一致): 若上方「状态切换指导」判定次日需切换状态，")
    logger.info(
        "      切换当日只执行卖出变现，买入推迟到再下一个交易日(资金结算后)按目标权重执行；"
    )
    logger.info("      故状态切换当日先卖、次交易日再买，请勿在切换当日满仓买入。")
    logger.info("      三保险(手动操作须同样遵守):")
    logger.info("        1) 禁止融资: 次交易日买入只能用卖出所得现金，绝不使用融资/保证金；")
    logger.info("        2) Anti-V冷却: 风险解除后需连续 %d 个交易日保持在场才允许切回 risk-on，" % S.MIN_RISK_OFF_DAYS)
    logger.info("           防止刚卖就V型反转踏空；(base自动层更严: %d 天)" % 2)
    logger.info("        3) 小单阈值: 买卖差额 <= %d 美元时视为无需调仓，跳过操作避免碎单。" % S.MIN_TRADE_VAL)
