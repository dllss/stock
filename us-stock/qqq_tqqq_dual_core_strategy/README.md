# QQQ / TQQQ 双核策略 · 本地回测版

基于 `base/ts_reference.py`（base.ts V22.1）精确移植的本地回测工程。
回测逻辑与 base 状态机**完全一致**，仅有一处获准偏离（见下文「与 base 的差异」）。

---

## 目录结构

```
qqq_tqqq_dual_core_strategy/
├── base/
│   └── ts_reference.py             # 🔒 基准实现（只读，禁止修改）
├── config/settings.py              # 阈值参数 + VOL_FACTOR（唯一偏离点）
├── data/fetcher.py                 # 东财美股数据拉取（含缓存，已修复 end 截断）
├── utils/helpers.py                # 指标计算（MA / ATH / VolMA）
├── strategy/strategy.py            # 状态机移植（与 base 逻辑对齐）
├── backtest/engine.py              # 回测撮合引擎（含 T+1 防融资）
├── main.py                         # 回测入口 + 实盘跟踪指导(--live)
└── tests/                          # pytest 正式测试（对齐验证、引擎快照回归）
```

## 运行

```bash
# 标准回测（含双核1.5 vs 原值2.0 对比）
python main.py

# 回测 + 实盘跟踪指导（次日状态切换地图 + 再平衡指令）
python main.py --live
```

Git Bash:
```bash
cd d:/WorkProject/stock/us-stock/qqq_tqqq_dual_core_strategy && python main.py --live
```

日志（含指导，UTF-8）输出到 `output/backtest_full_时间戳.log`，与同名 csv 配对。

---

## 策略逻辑（与 base 对齐）

状态机由 QQQ 价格驱动，6 个状态：

| 状态 | 触发条件 | 目标权重 |
|------|----------|----------|
| NORMAL | 站上 MA200 且回撤 < -10% | QQQ 45% / TQQQ 45% |
| ZONE_BATTLE_ATTACK | 熊市回撤 -10%~-30% 且站上 MA20 | TQQQ 99% |
| ZONE_BATTLE_DEFEND | 熊市回撤 -10%~-30% 且未站上 MA20 | QQQ 90% |
| ZONE_DESPAIR_TQQQ | 回撤 ≤ -30% | TQQQ 99% |
| BEAR_CASH | 跌破 MA200 且回撤 < -10% | 现金 100% |
| TOP_ESCAPE | 触及 ATH×0.95 且放量(Vol>VolMA×VOL_FACTOR)且收阴 | QQQ 90% |

- **逃顶过滤**：`close >= ATH×HIGH_ZONE` 且 `vol > VolMA×VOL_FACTOR` 且 `close < open`。
- **Anti-V 反转过滤**：risk-off → risk-on 需满足冷静期(`MIN_RISK_OFF_DAYS=2` 天) + MA20 斜率向上。
- **T+1 防融资**（与 base 一致）：状态切换日只卖出变现，`pending_buy` 推迟到下一交易日按目标权重买入。
- **NORMAL 再平衡**：QQQ/TQQQ 市值偏离 > 20% 触发再平衡。

---

## 与 base 的差异（透明清单）

> 依据 AGENTS.md：开发须基于 base 最初代码，除 1.5 优化外后续改动不应偏离 base 逻辑。

| 项 | 说明 | 是否偏离 base |
|----|------|---------------|
| `VOL_FACTOR` 2.0 → **1.5** | 唯一获准偏离。经训练段+样本外验证，1.5 年化/夏普更优、回撤不恶化 | ✅ 获准 |
| `strategy/strategy.py` | 状态机逐行移植，含逃顶/Anti-V/冷静期，逻辑与 base 一致 | 否 |
| `backtest/engine.py` | 撮合引擎含 T+1，与 base 下单时序一致 | 否 |
| `data/fetcher.py` | 原 bug：缓存命中忽略 `end` 致前视偏差；已修复为按 `end` 截断 | 否（bug 修复） |
| `main.py --live` | **新增辅助功能**：输出次日状态切换地图 + 再平衡股数估算，仅作跟踪参考，不改变策略逻辑 | 否（附加工具） |
| `config/settings.py` 其余参数 | `HIGH_ZONE=0.95` / `VOL_WINDOW=60` / `MIN_RISK_OFF_DAYS=2` / `NORMAL_REBAL_*` / `MIN_TRADE_VAL=500` 均与 base 一致 | 否 |

### 关于 VOL_FACTOR=1.5 的验证

训练段(2010~2020) / 样本外(2021~2026) 双重验证：

```
训练段:  1.5 → 年化 29.95% / 夏普 1.23 / 回撤 -25.95%
         2.0 → 年化 28.46% / 夏普 1.12 / 回撤 -27.39%
样本外:  1.5 → 年化 46.17% / 夏普 1.14 / 回撤 -32.31%
         2.0 → 年化 44.38% / 夏普 1.08 / 回撤 -32.41%
```

2.5 档样本外回撤恶化到 -34.19%，风险调整后更差，故未采用。

---

## 实盘跟踪指导（`--live`）使用须知

`--live` 在回测日志末尾追加两块内容，**仅供参考，不改变策略逻辑**：

1. **次日状态切换地图**：列出 QQQ 价格临界点 → 目标状态 → 动作，供你盘中盯价。
2. **次日再平衡指令**：按最后一天收盘价估算买卖股数 + 目标权重。

注意事项：
- 指导中的「状态切换」指信号触发时点；**实际下单遵循 base 的 T+1**——切换日先卖变现，买入推迟到下一交易日。
- 逃顶量能条件需次日确认；盘中可用 `估算全天量 ≈ 当前成交量 ÷ 已过交易时间占比` 粗估（美股本日 6.5 小时），临近收盘最准。
- 股数以最后一天收盘价估算，实际下单请以实时价微调。

---

## 数据说明

- 数据源：东财美股（`secid = 105.{ticker}`），复权方式 `hfq`（后复权，含分红再投资）。
- 缓存：`data/` 下按 ticker 缓存 csv，更新需重新拉取（或删除缓存）。
- 回测起点：`WARMUP_START` 预热算指标，`WARMUP_END` 后开始交易。

---

## 免责声明

本工程仅用于策略回测与研究，不构成任何投资建议。杠杆 ETF（TQQQ）波动剧烈，实盘风险自负。
base 策略版权 © 2026 园园AI (aiyuan.ai)，CC BY-NC 4.0。
