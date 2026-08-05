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

    legend = []
    lx = pad_l + 10
    for name, col in colors.items():
        legend.append('<rect x="%d" y="%d" width="12" height="12" fill="%s"/>' % (lx, 8, col))
        legend.append(
            '<text x="%d" y="%d" fill="#333" font-size="12">%s</text>' % (lx + 16, 18, name)
        )
        lx += 90

    svg = (
        '<svg viewBox="0 0 %d %d" width="100%%" preserveAspectRatio="xMidYMid meet" '
        'xmlns="http://www.w3.org/2000/svg">' % (W, H)
        + "".join(grid)
        + "".join(lines)
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

    分红再投派下，hfq 净值 / adj_factor 等价于上式；这里直接用 raw 价乘份额，
    避免 adj_factor 舍入误差，对账最精确。无网络/无 raw 数据时返回原表（不报错）。
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
            # raw 价列改名避免与 port 的 hfq 价(QQQ_Close)冲突
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
        "QQQ_Close": "QQQ(后复权)",
        "TQQQ_Close": "TQQQ(后复权)",
        "cash": "现金",
        "shares_QQQ": "QQQ份额",
        "shares_TQQQ": "TQQQ份额",
        "portfolio_value": "hfq净值",
        "real_value": "真实价市值",
        "ret": "累计收益",
    }
    head = "".join("<th>%s</th>" % _esc(head_label.get(c, c)) for c in cols)
    rows = []
    for _, r in port[cols].iterrows():
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
                    vals.append(_money(float(v)))  # 真实价市值（分红再投派对账用）
                else:
                    vals.append(_money(float(v)) if c == "portfolio_value" else _fmt(float(v)))
            else:
                vals.append(_fmt(float(v)))
        rows.append("<tr>" + "".join("<td>%s</td>" % v for v in vals) + "</tr>")
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

    # 主节点：行首 "• 名称"，子节点：前导空格 + "其中 ..."
    lines = seg.splitlines()
    root = {"state": root_state, "cond": "当前持仓状态", "action": "", "children": []}
    cur_main = None
    cur_sub = None  # 当前子节点（挂在 cur_main 下）

    def new_node(label):
        st = label
        # 提取状态名（去掉 "逃顶 "/"跌破 MA200 -> 风险区" 等前缀词）
        mm = re.search(r"([A-Z_]+)", label)
        state_key = mm.group(1) if mm else label
        return {"state": state_key, "label": label.strip(), "cond": "", "action": "", "children": []}

    for ln in lines:
        s = ln.rstrip()
        if not s.strip():
            continue
        stripped = s.lstrip()
        if stripped.startswith("•"):
            after = stripped[1:].lstrip()
            if after.startswith("其中"):  # 子状态（回落分档），挂在当前主节点下
                node = new_node(after)
                if cur_main is not None:
                    cur_main["children"].append(node)
                else:
                    root["children"].append(node)
                cur_sub = node
            else:  # 主状态节点
                node = new_node(after)
                root["children"].append(node)
                cur_main = node
                cur_sub = None
        elif "其中" in stripped and stripped.startswith("其中"):
            node = new_node(stripped)
            if cur_main is not None:
                cur_main["children"].append(node)
            else:
                root["children"].append(node)
            cur_sub = node
        elif "价能条件" in s:
            val = s.split(":", 1)[1].strip() if ":" in s else s
            target = cur_sub if cur_sub is not None else cur_main
            if target is not None:
                target["cond"] = val
        elif "动作" in s and ":" in s:
            val = s.split(":", 1)[1].strip()
            target = cur_sub if cur_sub is not None else cur_main
            if target is not None:
                target["action"] = val
    return root


def _build_mindmap(root):
    """将思维导图树渲染为内联 SVG（根在左，分支向右逐级展开），带 CSS 动画。"""
    if root is None:
        return ""

    NODE_W, NODE_H = 240, 46
    GAP_Y = 16
    LEVEL_X = [40, 320, 600, 880]  # 各层级 x 起点

    # 计算布局：先展平为带 depth 的节点列表
    flat = []  # (node, depth, parent_screen_y)

    def node_height(node):
        lines = 1 + (1 if node.get("cond") else 0) + (1 if node.get("action") else 0)
        return 30 + lines * 16 + GAP_Y

    def subtree_height(node):
        if not node["children"]:
            return node_height(node)
        return sum(subtree_height(c) for c in node["children"])

    total_h = max(NODE_H + GAP_Y, subtree_height(root))

    # 分配坐标
    def layout(node, depth, y_top):
        x = LEVEL_X[min(depth, len(LEVEL_X) - 1)]
        h = subtree_height(node)
        cy = y_top + h / 2
        node["_x"] = x
        node["_y"] = cy
        node["_cx"] = x + NODE_W
        flat.append((node, depth))
        yy = y_top
        for c in node["children"]:
            layout(c, depth + 1, yy)
            yy += subtree_height(c)
        return node

    layout(root, 0, 10)

    H = total_h + 20
    W = LEVEL_X[-1] + NODE_W + 20

    # 按层级排序，用于动画 delay
    nodes_svg = []
    links_svg = []

    def cn(name):
        return STATE_CN.get(name, name)

    for node, depth in flat:
        x, cy = node["_x"], node["_y"]
        title = cn(node.get("state", ""))
        cond = node.get("cond", "") or ""
        action = node.get("action", "") or ""
        # 节点动态高度：有条件和动作时加高，三行显示
        lines = 1 + (1 if cond else 0) + (1 if action else 0)
        nh = 30 + lines * 16
        # 节点块（带类用于动画，delay 按 depth）
        rect_y = cy - nh / 2
        nodes_svg.append(
            '<g class="mm-node mm-d%d" style="animation-delay:%ds">'
            '<rect x="%d" y="%.1f" width="%d" height="%d" rx="8" '
            'class="mm-rect mm-rect-%d"/>'
            '<text x="%d" y="%.1f" class="mm-title">%s</text>'
            % (
                depth,
                depth * 0.35,
                x,
                rect_y,
                NODE_W,
                nh,
                min(depth, 3),
                x + 10,
                rect_y + 18,
                _esc(title),
            )
        )
        yy = rect_y + 34
        if cond:
            nodes_svg.append(
                '<text x="%d" y="%.1f" class="mm-cond">条件 %s</text>'
                % (x + 10, yy, _esc(cond[:34] + ("…" if len(cond) > 34 else "")))
            )
            yy += 16
        if action:
            nodes_svg.append(
                '<text x="%d" y="%.1f" class="mm-act">动作 %s</text>'
                % (x + 10, yy, _esc(action[:34] + ("…" if len(action) > 34 else "")))
            )
        nodes_svg.append("</g>")
        node["_cx"] = x + NODE_W  # 连线起点用右边缘
        # 连线到子节点（生长动画）
        for c in node["children"]:
            x1 = node["_cx"]
            y1 = cy
            x2 = c["_x"]
            y2 = c["_y"]
            mx = (x1 + x2) / 2
            links_svg.append(
                '<path class="mm-link mm-d%d" style="animation-delay:%.2fs" '
                'd="M%.1f,%.1f C%.1f,%.1f %.1f,%.1f %.1f,%.1f"/>'
                % (
                    depth + 1,
                    (depth + 1) * 0.35,
                    x1,
                    y1,
                    mx,
                    y1,
                    mx,
                    y2,
                    x2,
                    y2,
                )
            )

    svg = (
        '<svg viewBox="0 0 %d %d" width="100%%" preserveAspectRatio="xMidYMid meet" '
        'xmlns="http://www.w3.org/2000/svg" class="mm-svg">' % (W, H)
        + "".join(links_svg)
        + "".join(nodes_svg)
        + "</svg>"
    )
    return (
        '<div class="mindmap-wrap"><h3>明日操作思维导图（状态切换地图）</h3>'
        + svg
        + '<p class="mindmap-tip">动画展示「当前状态 → 次日可能切换的目标状态」；'
        "完整价能条件与操作指令见下方原文。</p></div>"
    )


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
    live=False,
    live_text="",
):
    """生成自包含 HTML 报告，与 CSV 同目录、同时间戳配对。
    live_text: --live 模式下捕获的实盘操作指引纯文本（含明日指令/切换地图）。
    """
    html_path = out_path.rsplit(".", 1)[0] + ".html"

    title = "QQQ & TQQQ 双核策略回测报告"
    cards = _cards(summ, init_cap)
    compare = _compare_table(summ, summ_vol20, bh_qqq, bh_tqqq)
    svg = _build_svg(port, bh_qqq, bh_tqqq)
    state_html = _state_dist_html(dist)
    # 真实价市值（分红再投派对账用）：real = cash + shares*raw
    # 复权因子 adj_factor = hfq/raw，故用 raw 价直接乘持仓份额；无网络时跳过该列。
    port = _attach_real_value(port)
    detail = _detail_table(port)

    # 实盘跟踪段（--live 时才有意义，但仅在 live 时渲染提示）
    live_note = ""
    if live:
        live_note = (
            "<p class='note'>提示：本运行含 --live 实盘跟踪，"
            "下方为明日操作指引（真实市价股数、切换地图）。</p>"
        )
    # 实盘操作指引（明日指令 + 切换地图），与 log 完全一致，原样渲染
    live_html = ""
    mindmap_html = ""
    if live_text:
        live_html = "<h2>明日操作指引（实盘跟踪 --live）</h2>" "<pre class='live'>%s</pre>" % _esc(
            live_text
        )
        # 思维导图动画：可视化「切换地图」的状态分支
        mm_tree = _parse_mindmap(live_text)
        if mm_tree:
            mindmap_html = _build_mindmap(mm_tree)

    sd = str(port["date"].iloc[0])[:10]
    ed = str(port["date"].iloc[-1])[:10]

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
        svg=svg,
        livenote=live_note,
        state=state_html,
        mindmap=mindmap_html,
        live=live_html,
        detail=detail,
    )

    os.makedirs(os.path.dirname(html_path), exist_ok=True)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(doc)
    return html_path
