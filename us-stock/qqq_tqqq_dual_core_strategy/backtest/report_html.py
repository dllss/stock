#!/usr/bin/env python3
"""
回测结果 -> 自包含 HTML 报告（内联 SVG 图表，无外部依赖）。

输出内容：
  1. 关键指标卡片（累计收益 / 年化 / 最大回撤 / 夏普 / 最终资产）
  2. 策略对比表（双核正式1.5 / 双核原值2.0 / 全仓QQQ / 全仓TQQQ）
  3. 净值曲线 SVG 图（双核 vs QQQ vs TQQQ，归一化到初始资金）
  4. 状态分布
  5. 每日净值明细表（可滚动）
"""

import html
import os
import re
import string

import pandas as pd

# ---- 中文状态全称 ----
STATE_CN = {
    "INIT": "INIT(初始)",
    "NORMAL": "NORMAL(常态)",
    "TOP_ESCAPE": "TOP_ESCAPE(逃顶)",
    "ZONE_DESPAIR_TQQQ": "ZONE_DESPAIR_TQQQ(绝望区TQQQ)",
    "ZONE_BATTLE_ATTACK": "ZONE_BATTLE_ATTACK(交战进攻)",
    "ZONE_BATTLE_DEFEND": "ZONE_BATTLE_DEFEND(交战防守)",
    "BEAR_CASH": "BEAR_CASH(熊市空仓)",
}


def _pct(x):
    return "%.2f%%" % (x * 100)


def _money(x):
    return format(x, ",.0f")


def _fmt(v):
    return "%.2f" % v


def _esc(s):
    return html.escape(str(s))


def _build_price_svg(trade_df: pd.DataFrame):
    """生成 QQQ / TQQQ 前复权(qfq)股价走势 SVG（双 Y 轴，真实价格数值）。

    左轴 = QQQ 真实价（绿），右轴 = TQQQ 真实价（红）。
    两量级差近 2000 倍（QQQ 末价 ~700，TQQQ 首价 ~0.4），故用独立双轴，
    各自标注真实数值，避免把 TQQQ 压成贴底直线。
    """
    if trade_df is None or trade_df.empty:
        return "<p>无价格数据</p>"

    qqq = trade_df["QQQ_Close"].dropna().tolist()
    tqqq = trade_df["TQQQ_Close"].dropna().tolist()
    if not qqq and not tqqq:
        return "<p>无价格数据</p>"

    W, H = 900, 360
    legend_h = 30
    pad_l, pad_r, pad_t, pad_b = 64, 64, 20, 40
    plot_w = W - pad_l - pad_r
    plot_h = H - pad_t - pad_b

    def x(ser, i):
        return pad_l + (plot_w * i / (len(ser) - 1)) if len(ser) > 1 else pad_l

    def y_axis(val, lo, hi):
        span = (hi - lo) or 1.0
        return pad_t + plot_h * (1 - (val - lo) / span)

    # 各自量纲范围
    qqq_lo, qqq_hi = (min(qqq), max(qqq)) if qqq else (0, 1)
    tqqq_lo, tqqq_hi = (min(tqqq), max(tqqq)) if tqqq else (0, 1)
    if qqq_lo > 0:
        qqq_lo = 0.0
    if tqqq_lo > 0:
        tqqq_lo = 0.0

    grid = []
    # 共享网格线（以 6 等分左轴刻度），左右各标真实数值
    for g in range(6):
        frac = g / 5
        yy = pad_t + plot_h * (1 - frac)
        qv = qqq_lo + (qqq_hi - qqq_lo) * frac
        tv = tqqq_lo + (tqqq_hi - tqqq_lo) * frac
        grid.append('<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#eee" stroke-width="1"/>'
                    % (pad_l, yy, W - pad_r, yy))
        grid.append('<text x="%d" y="%.1f" fill="#16a34a" font-size="11" text-anchor="end">%.1f</text>'
                    % (pad_l - 6, yy + 4, qv))
        grid.append('<text x="%d" y="%.1f" fill="#dc2626" font-size="11" text-anchor="start">%.2f</text>'
                    % (W - pad_r + 6, yy + 4, tv))

    def path(ser, lo, hi, color):
        pts = " ".join("%.1f,%.1f" % (x(ser, i), y_axis(v, lo, hi)) for i, v in enumerate(ser))
        return '<polyline fill="none" stroke="%s" stroke-width="2" points="%s"/>' % (color, pts)

    lines = []
    if qqq:
        lines.append(path(qqq, qqq_lo, qqq_hi, "#16a34a"))
    if tqqq:
        lines.append(path(tqqq, tqqq_lo, tqqq_hi, "#dc2626"))

    legend = []
    lx = pad_l
    ly = legend_h / 2 - 6
    for name, col, unit in [("QQQ（前复权，左轴）", "#16a34a", ""),
                            ("TQQQ（前复权，右轴）", "#dc2626", "")]:
        legend.append('<rect x="%d" y="%.1f" width="10" height="10" fill="%s"/>' % (lx, ly + 1, col))
        legend.append('<text x="%d" y="%.1f" fill="#333" font-size="10">%s</text>'
                      % (lx + 14, ly + 9, name))
        lx += 230

    return (
        '<svg viewBox="0 0 %d %d" width="100%%" preserveAspectRatio="xMidYMid meet" '
        'xmlns="http://www.w3.org/2000/svg">' % (W, H + legend_h)
        + '<g transform="translate(0,%d)">' % legend_h
        + "".join(grid)
        + "".join(lines)
        + "</g>"
        + "".join(legend)
        + "</svg>"
    )


def _build_svg(port_dual: pd.DataFrame, bh_qqq: dict, bh_tqqq: dict):
    """生成三策略归一化净值曲线 SVG（纵轴=净值/初始资金，横轴=交易日序号）。"""
    # 双核逐日净值（已为绝对值，用 INIT_CAPITAL 归一化）
    from config.settings import S

    cap = S.INIT_CAPITAL

    def series(ser, init):
        arr = ser.dropna().values.astype(float)
        if len(arr) == 0 or arr[0] == 0:
            return []
        return (arr / init).tolist()

    dual = series(port_dual["portfolio_value"], cap)
    qqq = series(bh_qqq.get("curve"), cap) if bh_qqq else []
    tqqq = series(bh_tqqq.get("curve"), cap) if bh_tqqq else []

    all_series = [s for s in (dual, qqq, tqqq) if s]
    if not all_series:
        return "<p>无净值数据</p>"

    max_val = max(max(s) for s in all_series)
    min_val = min(min(s) for s in all_series)
    if min_val > 1:
        min_val = 0.0  # 起点附近留白更直观
    span = (max_val - min_val) or 1.0

    W, H = 900, 360
    legend_h = 30  # 顶部空白行，用于放置图例标签（坐标图尺寸保持不变，整体下移到其下方）
    pad_l, pad_r, pad_t, pad_b = 60, 20, 20, 40
    plot_w = W - pad_l - pad_r
    plot_h = H - pad_t - pad_b

    def x(i, n):
        return pad_l + (plot_w * i / (n - 1)) if n > 1 else pad_l

    def y(v):
        return pad_t + plot_h * (1 - (v - min_val) / span)

    # 网格 + Y 轴刻度（5 档）
    grid = []
    for g in range(6):
        vv = min_val + span * g / 5
        yy = y(vv)
        grid.append(
            '<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#eee" stroke-width="1"/>'
            % (pad_l, yy, W - pad_r, yy)
        )
        grid.append(
            '<text x="%d" y="%.1f" fill="#888" font-size="11" text-anchor="end">%.2fx</text>'
            % (pad_l - 6, yy + 4, vv)
        )

    def path(ser, color):
        pts = " ".join("%.1f,%.1f" % (x(i, len(ser)), y(v)) for i, v in enumerate(ser))
        return '<polyline fill="none" stroke="%s" stroke-width="2" points="%s"/>' % (color, pts)

    colors = {"双核": "#2563eb", "QQQ": "#16a34a", "TQQQ": "#dc2626"}
    lines = []
    if dual:
        lines.append(path(dual, colors["双核"]))
    if qqq:
        lines.append(path(qqq, colors["QQQ"]))
    if tqqq:
        lines.append(path(tqqq, colors["TQQQ"]))

    # 图例：放在顶部空白行（legend_h 内），不压在坐标图上方
    legend = []
    lx = pad_l
    ly = legend_h / 2 - 6
    for name, col in colors.items():
        legend.append('<rect x="%d" y="%.1f" width="10" height="10" fill="%s"/>' % (lx, ly + 1, col))
        legend.append(
            '<text x="%d" y="%.1f" fill="#333" font-size="10">%s</text>'
            % (lx + 14, ly + 9, name)
        )
        lx += 80

    svg = (
        '<svg viewBox="0 0 %d %d" width="100%%" preserveAspectRatio="xMidYMid meet" '
        'xmlns="http://www.w3.org/2000/svg">' % (W, H + legend_h)
        # 绘图主内容整体下移 legend_h，坐标图尺寸/比例完全不变
        + '<g transform="translate(0,%d)">' % legend_h
        + "".join(grid)
        + "".join(lines)
        + "</g>"
        + "".join(legend)
        + "</svg>"
    )
    return svg


def _state_dist_html(dist: pd.Series):
    if dist is None or len(dist) == 0:
        return "<p>无</p>"
    total = dist.sum()
    rows = []
    for st, cnt in dist.items():
        pct = cnt / total * 100 if total else 0
        rows.append(
            "<tr><td>%s</td><td>%d</td><td>%.1f%%</td></tr>"
            % (_esc(STATE_CN.get(st, st)), cnt, pct)
        )
    return (
        "<table class='tbl'><thead><tr><th>状态</th><th>天数</th><th>占比</th></tr></thead>"
        "<tbody>" + "".join(rows) + "</tbody></table>"
    )


def _attach_real_value(port: pd.DataFrame) -> pd.DataFrame:
    """附加真实价市值列：real_value = cash + shares_QQQ*raw_QQQ + shares_TQQQ*raw_TQQQ。

    直接用 raw 价乘份额，对账最精确（raw 即交易所真实成交价）。无网络/无 raw 数据时返回原表（不报错）。
    """
    if port is None or len(port) == 0:
        return port
    try:
        from config.settings import S
        from data import fetcher

        frames = {}
        for tk in S.DATA_TICKERS:
            d = fetcher.fetch_ticker(tk, start="20080101", end="20991231", adjust="")
            if d.empty:
                return port
            # raw 价列改名避免与 port 的 qfq 价(QQQ_Close)冲突
            d = d.rename(
                columns=lambda c, tk=tk: f"{tk}_raw" if c == "Close" else c
            )
            frames[tk] = d[["date", f"{tk}_raw"]]
        raw = None
        for tk, d in frames.items():
            raw = d if raw is None else raw.merge(d, on="date", how="outer")
        raw = raw.sort_values("date").reset_index(drop=True)
        merged = port.merge(raw, on="date", how="left")
        rv = (
            merged["cash"].fillna(0)
            + merged["shares_QQQ"].fillna(0) * merged["QQQ_raw"]
            + merged["shares_TQQQ"].fillna(0) * merged["TQQQ_raw"]
        )
        port = port.copy()
        port["real_value"] = rv.reindex(port.index).values
        return port
    except Exception:
        # 静默降级：无网络/接口失败时不附加真实价列
        return port


def _detail_table(port: pd.DataFrame):
    cols = [
        "date",
        "state",
        "QQQ_Close",
        "TQQQ_Close",
        "cash",
        "shares_QQQ",
        "shares_TQQQ",
        "portfolio_value",
        "real_value",
        "QQQ_Volume",
        "ret",
    ]
    # real_value 缺失时（无 raw 价）静默跳过该列
    if "real_value" not in port.columns:
        cols = [c for c in cols if c != "real_value"]
    if "ret" not in port.columns:
        port = port.copy()
        port["ret"] = port["portfolio_value"] / port["portfolio_value"].iloc[0] - 1
    head_label = {
        "date": "日期",
        "state": "状态",
        "QQQ_Close": "QQQ(前复权)",
        "TQQQ_Close": "TQQQ(前复权)",
        "cash": "现金",
        "shares_QQQ": "QQQ份额",
        "shares_TQQQ": "TQQQ份额",
        "portfolio_value": "qfq净值",
        "real_value": "真实价市值",
        "QQQ_Volume": "QQQ成交量",
        "ret": "累计收益",
    }
    head = "".join(
        "<th%s>%s</th>" % (' class="cw"' if c not in ("date", "state") else "", _esc(head_label.get(c, c)))
        for c in cols
    )
    rows = []
    for _, r in port[cols].iloc[::-1].iterrows():
        vals = []
        for c in cols:
            v = r[c]
            if c == "date":
                vals.append(_esc(str(v)[:10]))
            elif c == "state":
                vals.append(_esc(STATE_CN.get(v, v)))
            elif c == "ret":
                vals.append(_pct(float(v)))
            elif c in ("QQQ_Close", "TQQQ_Close", "portfolio_value", "real_value"):
                if c == "real_value":
                    vals.append(_money(float(v)))  # 真实价市值（真实成交价 × 份额，实盘对账用）
                else:
                    vals.append(_money(float(v)) if c == "portfolio_value" else _fmt(float(v)))
            elif c == "QQQ_Volume":
                vals.append(_money(float(v)))  # 当日 QQQ 成交量（整数千分位）
            else:
                vals.append(_fmt(float(v)))
        cells = "".join(
            "<td%s>%s</td>" % (' class="cw"' if c not in ("date", "state") else "", v)
            for c, v in zip(cols, vals)
        )
        rows.append("<tr>" + cells + "</tr>")
    return "<table class='tbl'><thead><tr>%s</tr></thead><tbody>%s</tbody></table>" % (
        head,
        "".join(rows),
    )


def _cards(summ, init_cap):
    final = summ.get("final_value", 0)
    cards = [
        ("初始资金", _money(init_cap), ""),
        ("最终资产", _money(final), ""),
        (
            "累计收益",
            _pct(summ.get("total_return", 0)),
            "pos" if summ.get("total_return", 0) >= 0 else "neg",
        ),
        ("年化收益", _pct(summ.get("cagr", 0)), "pos" if summ.get("cagr", 0) >= 0 else "neg"),
        ("最大回撤", _pct(summ.get("max_drawdown", 0)), "neg"),
        ("夏普比率", _fmt(summ.get("sharpe", 0)), ""),
    ]
    out = []
    for name, val, cls in cards:
        out.append(
            "<div class='card %s'><div class='card-name'>%s</div><div class='card-val'>%s</div></div>"
            % (cls, _esc(name), _esc(val))
        )
    return "<div class='cards'>" + "".join(out) + "</div>"


def _compare_table(summ, summ_vol20, bh_qqq, bh_tqqq):
    def row(label, s):
        if not s:
            return "<tr><td>%s</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td></tr>" % _esc(
                label
            )
        return "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
            _esc(label),
            _money(s["final_value"]),
            _pct(s["total_return"]),
            _pct(s["cagr"]),
            _pct(s["max_drawdown"]),
            _fmt(s["sharpe"]),
        )

    return (
        "<table class='tbl'>"
        "<thead><tr><th>策略</th><th>最终资产</th><th>累计收益</th><th>年化</th><th>最大回撤</th><th>夏普</th></tr></thead>"
        "<tbody>"
        + row("双核(正式1.5)", summ)
        + row("双核(原值2.0)", summ_vol20)
        + row("全仓QQQ", bh_qqq)
        + row("全仓TQQQ", bh_tqqq)
        + "</tbody></table>"
    )


def _parse_mindmap(live_text):
    """从 --live 的明日操作指引文本中解析「次日切换地图」，构建思维导图树。

    返回:
        root: dict = {"state": "NORMAL", "children": [ {...}, ... ]}
    解析失败（无切换地图）时返回 None。
    """
    if not live_text:
        return None
    text = html.unescape(live_text)
    text = text.replace("%%", "%")  # 还原日志中 % 格式化的转义残留

    # 截取切换地图段：[次日切换地图] ... [限制与注意事项]
    start = text.find("[次日切换地图]")
    end = text.find("[限制与注意事项]")
    if start == -1:
        return None
    seg = text[start : end if end != -1 else len(text)]

    # 当前状态（根）
    m = re.search(r"当前状态:\s*(\w+)", text)
    root_state = m.group(1) if m else "NORMAL"

    # 节点行格式（来自 --live 输出）：
    #   主节点:  "  - 逃顶 TOP_ESCAPE"            (行首 "- "，无 "其中")
    #   子节点:  "  -   其中 回撤>-10%%"          (行内含 "其中"，缩进更深)
    # 价能条件/动作 挂到当前子节点（cur_sub），无子节点时挂到当前主节点（cur_main）。
    lines = seg.splitlines()
    # 根节点的「当前动作」来自 [次日切换地图] 之前的独立行：
    #   "当前状态: NORMAL  ->  当前动作: 持有 QQQ+TQQQ (各45%, 留10%现金)"
    act_m = re.search(r"当前动作:\s*(.+)", text)
    root_action = act_m.group(1).strip() if act_m else ""
    vol_m = re.search(r"当日QQQ成交量:\s*([\d.,]+)", text)
    root_qqq_vol = vol_m.group(1).strip() if vol_m else ""
    root = {"state": root_state, "cond": "", "action": root_action, "qqq_vol": root_qqq_vol, "children": []}
    cur_main = None
    cur_sub = None  # 当前子节点（挂在 cur_main 下）

    def translate_states(text):
        # 把条件/动作文本里的英文状态名（如 BEAR_CASH）翻译为中文（BEAR_CASH(熊市空仓)），
        # 避免在思维导图里只显示裸英文。STATE_CN 的 key 均为独立大写 token，不会误伤 MA200 等。
        for k, v in STATE_CN.items():
            if k in text:
                text = text.replace(k, v)
        return text

    def new_node(label):
        # 节点标题直接用清洗后的可读标签：
        # 主节点如「跌破 MA200 -> 风险区」、子节点如「回撤>-10%」（去掉「其中」层级前缀）。
        # 不再用正则抓英文状态名（会从 MA200 误抓 MA），中文描述本身即最佳标题。
        name = re.sub(r"^其中\s*", "", label.strip())
        return {"state": name, "label": name, "cond": "", "action": "", "cond_note": "", "children": []}

    def attach(node):
        # 子节点挂到当前主节点下；主节点挂到 root 下
        if cur_main is not None and node.get("_is_sub"):
            cur_main["children"].append(node)
        else:
            root["children"].append(node)

    for ln in lines:
        s = ln.rstrip()
        if not s.strip():
            continue
        stripped = s.lstrip()
        if stripped.startswith("-"):  # 节点行（主/子）
            # stripped[1:] 是 "-" 之后的内容，保留 title 自身的前导缩进（lstrip 会丢失缩进，故用 title_content 判断）
            title_content = stripped[1:]
            after = title_content.lstrip()
            # 跳过分隔线（纯 "-"/"=" 等）和空内容
            if not after or set(after) <= set("-="):
                continue
            # 按 title 自身缩进判断层级：日志固定为 "  - <title>"，"- " 之间恒有 1 空格，
            # 故 title 自身前导缩进 = (title_content 长度 - after 长度) - 1。
            # 子节点（"  -> BEAR_CASH" / "  -> ZONE_XXX"）title 自带 2 空格缩进 → is_sub=True；
            # 主节点（"逃顶 TOP_ESCAPE" / "跌破 MA200 -> 风险区" 等）title 无缩进 → 恒为 0 → is_sub=False。
            title_indent = (len(title_content) - len(after)) - 1
            is_sub = title_indent > 0
            node = new_node(after)
            node["_is_sub"] = is_sub
            attach(node)
            if is_sub:
                cur_sub = node
                # 子节点归属当前主节点；若前面没有主节点则退化为 root 子节点
                if cur_main is None:
                    cur_main = node
            else:
                cur_main = node
                cur_sub = None
        elif "价能条件" in s:
            val = s.split(":", 1)[1].strip() if ":" in s else s
            target = cur_sub if cur_sub is not None else cur_main
            if target is not None:
                target["cond"] = translate_states(val)
        elif "条件说明" in s:
            val = s.split(":", 1)[1].strip() if ":" in s else s
            target = cur_sub if cur_sub is not None else cur_main
            if target is not None:
                target["cond_note"] = val
        elif "附加条件" in s:
            val = s.split(":", 1)[1].strip() if ":" in s else s
            target = cur_sub if cur_sub is not None else cur_main
            if target is not None:
                # 量能等附加条件合并到条件行（与价能条件用「；」连接）
                target["cond"] = (target.get("cond") or "") + ("；" if target.get("cond") else "") + "附加: " + translate_states(val)
        elif "执行动作" in s and ":" in s or ("动作" in s and ":" in s):
            val = s.split(":", 1)[1].strip()
            target = cur_sub if cur_sub is not None else cur_main
            if target is not None:
                target["action"] = translate_states(val)
    return root


def _tree_to_d3(root):
    """将 _parse_mindmap 的树转为 d3.hierarchy 可用的 JSON（节点含 title/cond/action）。"""

    def cn(name):
        return STATE_CN.get(name, name)

    def conv(node):
        return {
            "name": cn(node.get("state", "")),
            "cond": node.get("cond", "") or "",
            "action": node.get("action", "") or "",
            "cond_note": node.get("cond_note", "") or "",
            "qqq_vol": node.get("qqq_vol", "") or "",
            "children": [conv(c) for c in node.get("children", [])],
        }

    return conv(root)


def _build_mindmap(root):
    """用 D3 (tree layout) 渲染思维导图：根在左，分支向右逐层展开，支持缩放/拖动。

    输出：挂载 <svg> 容器 + 内联渲染脚本（数据以 JSON 注入）。
    d3 库由本页通过 CDN (jsDelivr) 引用 d3@7。
    """
    if root is None:
        return ""

    data = _tree_to_d3(root)
    import json

    payload = json.dumps(data, ensure_ascii=False)

    svg = (
        '<div class="mindmap-wrap">'
        '<div id="mm-container" class="mm-container"></div>'
        '<p class="mindmap-tip">「当前状态 → 次日可能切换的目标状态」思维导图；'
        "完整价能条件与操作指令见下方原文。</p>"
        '<script id="mm-data" type="application/json">%s</script>'
        '<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>'
        '<script>\n'
        "(function(){\n"
        "  const data = JSON.parse(document.getElementById('mm-data').textContent || '{}');\n"
        "  const dx = 140;           // 同层节点垂直间距\n"
        "  const gap = 100;          // 节点间的水平空白\n"
        "  const nodeW = d => (d.depth === 0 ? 300 : (d.depth >= 2 ? 600 : 460));  // 孙节点宽 +20%\n"
        "  const root = d3.hierarchy(data);\n"
        "  // 用 d3.tree 算垂直位置，再按层级自定义水平 x（左边缘），避免加宽后重叠\n"
        "  d3.tree().nodeSize([dx, 1])(root);\n"
        "  root.each(d => { d.y = d.depth === 0 ? 0 : d.parent.y + nodeW(d.parent) + gap; });\n"
        "  let x0 = Infinity, x1 = -Infinity, y0 = Infinity, y1 = -Infinity;\n"
        "  root.each(d => { if (d.x > x1) x1 = d.x; if (d.x < x0) x0 = d.x;\n"
        "                    if (d.y > y1) y1 = d.y; if (d.y < y0) y0 = d.y; });\n"
        "  const colorByDepth = ['#2563eb','#16a34a','#ea580c','#dc2626','#7c3aed'];\n"
        "  // 预计算每个节点的文本行与卡片高度（供布局高度与绘制复用）\n"
        "  const lh = 22, pad = 10;\n"
        "  root.descendants().forEach(d => {\n"
        "    // 思维导图卡片不展示「(含义: X，后复权: Y)」后缀（日志文本块照常展示）\n"
        "    const stripHfq = s => s.replace(/\\s*\\(含义:[^)]*\\)/g, '');\n"
        "    const lines = [d.data.name];\n"
        "    if (d.data.cond_note) lines.push('条件说明 ' + d.data.cond_note);\n"
        "    if (d.data.cond) {\n"
        "      const parts = d.data.cond.split('；附加: ');\n"
        "      lines.push('条件 ' + stripHfq(parts[0]));\n"
        "      if (parts.length > 1) lines.push('附加 ' + stripHfq(parts[1]));\n"
        "    }\n"
    "    if (d.data.action) lines.push((d.depth === 0 ? '当前动作 ' : '执行动作 ') + d.data.action);\n"
    "    if (d.depth === 0 && d.data.qqq_vol) lines.push('当日QQQ成交量 ' + d.data.qqq_vol);\n"
    "    d._lines = lines;\n"
    "    d._h = lines.length * lh + pad * 2;\n"
    "    // 不可达检测：价能条件出现「左界 <= ... < 右界」且左界数值 > 右界数值（区间为空）\n"
    "    d._unreachable = false;\n"
    "    if (d.data.cond) {\n"
    "      const m = d.data.cond.match(/([\\d.]+)\\s*(?:\\([^)]*\\))?\\s*<=\\s*[^<]*?<\\s*([\\d.]+)/);\n"
    "      if (m && parseFloat(m[1]) > parseFloat(m[2])) d._unreachable = true;\n"
    "    }\n"
        "  });\n"
        "  const maxHalf = root.descendants().reduce((m,d)=>Math.max(m, d._h/2), 0);\n"
        "  // 内部固定坐标系：内容自然宽高 + 留白（含卡片半高，避免上下裁切），svg 用 viewBox 由 CSS width:100% 等比缩放\n"
        "  const padX = 40, padY = 24;\n"
        "  const contentH = (x1 - x0) + maxHalf * 2 + padY * 2;\n"
        "  const contentW = (y1 - y0) + nodeW(root.descendants().sort((a,b)=>b.depth-a.depth)[0]) + padX * 2;\n"
        "  const vbH = contentH;\n"
        "  const svg = d3.select('#mm-container').append('svg')\n"
        "      .attr('class', 'mm-svg')\n"
        "      .attr('viewBox', `0 0 ${contentW} ${vbH}`)\n"
        "      .attr('preserveAspectRatio', 'xMidYMid meet')\n"
        "      .style('--mm-w', contentW + 'px');  // 内容原始宽，窄屏(<960px)下用作固定宽，避免等比缩小压小文字\n"
        "  // 纵向平移：最上节点中心 x0 映射到 (padY + maxHalf)，使内容垂直居中且不裁切\n"
        "  const g = svg.append('g')\n"
        "      .attr('transform', `translate(${padX - y0},${padY + maxHalf - x0})`);\n"
        "  // 连线：父节点从右边缘出发，子节点到左边缘（避免从中心穿出）\n"
        "  g.selectAll('path.mm-link').data(root.links()).join('path')\n"
        "    .attr('class','mm-link')\n"
        "    .attr('d', d => {\n"
        "      const sx = d.source.y + nodeW(d.source);  // 父右边缘\n"
        "      const sy = d.source.x;\n"
        "      const tx = d.target.y;                    // 子左边缘\n"
        "      const ty = d.target.x;\n"
        "      const mx = (sx + tx) / 2;\n"
        "      return `M${sx},${sy} C${mx},${sy} ${mx},${ty} ${tx},${ty}`;\n"
        "    });\n"
        "  // 节点组\n"
        "  const node = g.selectAll('g.mm-node').data(root.descendants()).join('g')\n"
        "    .attr('class','mm-node')\n"
        "    .attr('transform', d => `translate(${d.y},${d.x})`);\n"
        "  // 节点卡片（复用预计算的 _lines / _h）\n"
        "  node.each(function(d){\n"
        "    const g0 = d3.select(this);\n"
        "    const lines = d._lines, w = nodeW(d), h = d._h;\n"
    "    const col = colorByDepth[Math.min(d.depth, colorByDepth.length-1)];\n"
    "    g0.append('rect').attr('class','mm-rect')\n"
    "      .attr('x', 0).attr('y', -h/2).attr('width', w).attr('height', h)\n"
    "      .attr('rx', 8)\n"
    "      .style('fill', d._unreachable ? '#fee2e2' : '#fff')\n"
    "      .style('stroke', d._unreachable ? '#dc2626' : col)\n"
    "      .style('stroke-width', d._unreachable ? 2 : 1.5);\n"
    "    if (d._unreachable) {\n"
    "      g0.append('text').attr('x', w - 8).attr('y', -h/2 + pad + lh*0.75)\n"
    "        .attr('text-anchor', 'end').attr('class', 'mm-unreach')\n"
    "        .text('⚠ 不可达');\n"
    "    }\n"
        "    lines.forEach((t, i) => {\n"
        "      let cls = 'mm-title';\n"
        "      if (i > 0) {\n"
        "        if (t.startsWith('附加')) cls = 'mm-extra';\n"
        "        else if (t.startsWith('条件')) cls = 'mm-cond';\n"
        "        else cls = 'mm-act';\n"
        "      }\n"
        "      g0.append('text').attr('x', 12).attr('y', -h/2 + pad + lh*(i+0.75))\n"
        "        .attr('class', cls).text(t);\n"
        "    });\n"
        "  });\n"
        "})();\n"
        "</script></div>"
    ).replace("%s", payload)
    return svg


def build_report(
    out_path,
    summ,
    summ_vol20,
    bh_qqq,
    bh_tqqq,
    dist,
    port,
    init_cap,
    run_ts,
    trade_df=None,
    live=False,
    live_text="",
    live_hist_html="",
):
    """生成自包含 HTML 报告，与 CSV 同目录、同时间戳配对。
    live_text: --live 模式下捕获的实盘操作指引纯文本（含明日指令/切换地图）。
    """
    html_path = out_path.rsplit(".", 1)[0] + ".html"

    title = "QQQ & TQQQ 双核策略回测报告"
    cards = _cards(summ, init_cap)
    compare = _compare_table(summ, summ_vol20, bh_qqq, bh_tqqq)
    adjust_compare = ""
    svg = _build_svg(port, bh_qqq, bh_tqqq)
    price_svg = _build_price_svg(trade_df)
    state_html = _state_dist_html(dist)
    # 真实价市值（实盘对账用）：real = cash + shares*raw，raw 即交易所真实成交价；
    # 无网络/无 raw 数据时 _attach_real_value 静默跳过该列。
    port = _attach_real_value(port)
    detail = _detail_table(port)

    # 实盘跟踪段（默认包含，live 有内容时渲染提示）
    live_note = ""
    if live:
        live_note = (
            "<p class='note'>提示：本报告已默认包含实盘跟踪，"
            "下方为明日操作指引（真实市价股数、切换地图）。</p>"
        )
    # 实盘操作指引（明日指令 + 切换地图），与 log 完全一致，原样渲染
    live_html = ""
    mindmap_html = ""
    if live_text:
        live_html = "<pre class='live'>%s</pre>" % _esc(live_text)
        # 思维导图动画：可视化「切换地图」的状态分支
        mm_tree = _parse_mindmap(live_text)
        if mm_tree:
            mindmap_html = _build_mindmap(mm_tree)

    sd = str(port["date"].iloc[0])[:10]
    ed = str(port["date"].iloc[-1])[:10]

    # 实盘跟踪历史表（line2），嵌入报告「实盘跟踪」章节
    livehist_html = live_hist_html if live_hist_html else ""

    # 读取独立 HTML 模板并填充（string.Template：$var 占位，避免与 CSS 的 {} 冲突）
    tpl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "report_template.html")
    with open(tpl_path, encoding="utf-8") as f:
        template = string.Template(f.read())
    doc = template.safe_substitute(
        title=title,
        sd=sd,
        ed=ed,
        init_cap=_money(init_cap),
        run_ts=run_ts,
        cards=cards,
        compare=compare,
        adjustcompare=adjust_compare,
        svg=svg,
        pricesvg=price_svg,
        livenote=live_note,
        state=state_html,
        mindmap=mindmap_html,
        live=live_html,
        livehist=livehist_html,
        detail=detail,
    )

    os.makedirs(os.path.dirname(html_path), exist_ok=True)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(doc)
    return html_path
