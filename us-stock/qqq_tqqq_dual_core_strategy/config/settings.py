#!/usr/bin/env python3
"""策略配置（集中管理所有可调参数）。

使用 @dataclass 显式声明字段与类型，便于静态检查与 IDE 补全。
配置实例通过模块级 ``S`` 导出，调用方统一用 ``S.INIT_CAPITAL`` 等属性访问。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@dataclass
class Settings:
    # ---- 账户 / 资金 ----
    INIT_CAPITAL: float = 100_000.0  # 初始资金（美元）
    MIN_TRADE_VAL: float = 500.0  # 单笔最小交易额（过滤零头碎股；与 base 实盘层一致）
    ROUND_LOTS: bool = True  # 是否按整股取整

    # ---- 标的 ----
    DATA_TICKERS: tuple[str, ...] = ("QQQ", "TQQQ")
    ADJUST: str = "hfq"  # 复权口径：hfq 后复权（TQQQ 不复权有拆股拼接 bug）

    # ---- NORMAL 常态 ----
    NORMAL_REBAL_W: float = 0.45  # QQQ / TQQQ 目标权重（各 45%，现金 10% 缓冲）
    NORMAL_REBAL_DEV: float = 0.20  # 市值偏离 > 20% 才触发再平衡

    # ---- 指标窗口 ----
    MA_SHORT: int = 20  # MA20
    MA_LONG: int = 200  # MA200
    VOL_WINDOW: int = 60  # 量能均线窗口（与 base 实盘层一致）
    VOL_FACTOR: float = 1.5  # 逃顶量能阈值倍数（收阴且量 > VolMA*FACTOR）
    ATH_WINDOW: int = 250  # ATH 观察窗口（仅日志展示用；实际 ATH 用 cummax 全局最高）
    RET_WINDOW: int = 60  # 滚动回撤窗口
    HIGH_ZONE: float = 0.95  # ATH 的 95% 以上 + 放量阴线 => 逃顶
    DESPAIR_LINE: float = -0.30  # 回撤 <=-30% => 绝望区（满仓 TQQQ）
    BATTLE_LINE: float = -0.10  # 回撤 <=-10% => 交战区

    # ---- 状态切换冷却 ----
    MIN_RISK_OFF_DAYS: int = 2  # 风险区 -> 风险区最少等待天数（Anti-V 冷静期；与 base 实盘层一致）
    RISK_OFF_LIST: tuple[str, ...] = (
        "ZONE_BATTLE_DEFEND",
        "BEAR_CASH",
        "TOP_ESCAPE",
    )
    RISK_ON_LIST: tuple[str, ...] = ("ZONE_BATTLE_ATTACK", "NORMAL")

    # ---- 预热 / 数据区间 ----
    WARMUP_START: str = "2015-01-01"  # 指标预热起点（MA200 需足够历史）
    WARMUP_END: str = "2018-01-01"  # 交易起点（指标已就绪）

    # ---- 目录 ----
    OUTPUT_DIR: str = field(default_factory=lambda: os.path.join(BASE_DIR, "OUTPUT"))
    CACHE_DIR: str = field(default_factory=lambda: os.path.join(BASE_DIR, "data", "cache"))
    LOG_FILE: str = field(default_factory=lambda: os.path.join(BASE_DIR, "OUTPUT", "backtest.log"))


# 模块级配置实例，全局统一访问点
S = Settings()
