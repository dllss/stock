#!/usr/bin/env python3
"""策略配置（集中管理所有可调参数）。

使用 @dataclass 显式声明字段与类型，便于静态检查与 IDE 补全。
配置实例通过模块级 ``S`` 导出，调用方统一用 ``S.INIT_CAPITAL`` 等属性访问。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# 保持合并前的输出、缓存和日志目录：仍位于 qqq_tqqq_dual_core_strategy 根目录。
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@dataclass
class Settings:
    # ---- 账户 / 资金 ----
    INIT_CAPITAL: float = 100_000.0  # 初始资金（美元）
    MIN_TRADE_VAL: float = 500.0  # 单笔最小交易额（过滤零头碎股；与 base 实盘层一致）
    ROUND_LOTS: bool = True  # 是否按整股取整

    # ---- 标的 ----
    DATA_TICKERS: tuple[str, ...] = ("QQQ", "TQQQ")
    ADJUST: str = "qfq"  # 复权口径：qfq 前复权（锚定最新日，价格尺度贴合实盘真实价，详见 README「数据说明」）

    # ---- NORMAL 常态 ----
    NORMAL_REBAL_W: float = 0.45  # QQQ / TQQQ 目标权重（各 45%，现金 10% 缓冲）
    NORMAL_REBAL_DEV: float = 0.20  # 市值偏离 > 20% 才触发再平衡
    NORMAL_REBAL_ENABLED: bool = True

    # ---- 实盘跟踪线（独立账户，从建仓日起算真实收益率，与回测线分离）----
    # 同时作为 --live 次日操作指令的真实账户数据源（唯一真实账户入口）。
    TRACK_BUY_DATE: str = "2026-08-06"  # 真实建仓日
    TRACK_SHARES_QQQ: float = 1.0  # 建仓 QQQ 股数
    TRACK_SHARES_TQQQ: float = 11.0  # 建仓 TQQQ 股数
    TRACK_PRICE_QQQ: float = 715.0  # 建仓 QQQ 单价（真实价）
    TRACK_PRICE_TQQQ: float = 72.0  # 建仓 TQQQ 单价（真实价）
    TRACK_CASH: float = 150.0  # 建仓后剩余现金
    TRACK_INIT_CAPITAL: float = 1657.0  # 实盘投入本金（= 股票成本 1*715 + 11*72 + 现金 150）
    TRACK_CASH_PCT: float = 0.088  # 跟踪线目标现金比例（QQQ/TQQQ 各 (1-此值)/2）

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
    WARMUP_END: str = "2017-01-02"  # 交易起点

    # ---- 目录 ----
    OUTPUT_DIR: str = field(default_factory=lambda: os.path.join(BASE_DIR, "OUTPUT"))
    CACHE_DIR: str = field(default_factory=lambda: os.path.join(BASE_DIR, "data", "cache"))
    LIVE_LOG_FILE: str = field(default_factory=lambda: os.path.join(BASE_DIR, "OUTPUT", "live_track.log"))


# 模块级配置实例，全局统一访问点
S = Settings()
