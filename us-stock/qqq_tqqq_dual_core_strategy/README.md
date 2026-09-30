# QQQ / TQQQ Dual-Core Strategy

本目录同时维护两个版本，公共数据、回测和报告代码集中在 `common/`。

## 目录结构

```text
qqq_tqqq_dual_core_strategy/
├── common/
│   ├── backtest/       # 回测引擎和 HTML 报告
│   ├── data/           # 东方财富行情抓取和缓存
│   ├── base/           # 策略平台基线参考
│   └── utils/          # 指标和实盘提示
├── v22_1/              # 原双核策略版本
└── v22_3/              # python.py V22.3 本地回测版本
```

## 运行

在仓库根目录执行：

```bash
python us-stock/qqq_tqqq_dual_core_strategy/v22_1/main.py
python us-stock/qqq_tqqq_dual_core_strategy/v22_3/main.py
```

也可以指定交易起点、指标预热起点和回测终点：

```bash
python us-stock/qqq_tqqq_dual_core_strategy/v22_3/main.py \
  --start 2017-01-02 \
  --warmup-start 2015-01-01 \
  --end 2026-08-10
```

## V22.3 规则

- NORMAL：90% TQQQ，10% 现金。
- ZONE_BATTLE_ATTACK / ZONE_DESPAIR_TQQQ：99% TQQQ。
- ZONE_BATTLE_DEFEND：90% QQQ。
- HI：100% QQQ。
- HI_CASH / BEAR_CASH：100% 现金。
- QQQ 高于 MA200 20% 进入 HI；HI/HI_CASH 使用 V22.2 解锁链。
- 回撤达到 -30% 时，只有 MA20 向上才进入深坑 TQQQ 进攻状态。
- NORMAL 为单一 TQQQ 仓位，因此不执行旧版 QQQ/TQQQ 双资产再平衡。
