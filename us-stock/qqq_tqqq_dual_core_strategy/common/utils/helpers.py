#!/usr/bin/env python3
"""指标计算工具：MA、ATH、Top_Signal 等。独立模块，不依赖主工程。
对齐 base.ts V22：ATH 用全局历史最高（cummax）；Top_Signal 含放量收阴。
"""

import pandas as pd

from config.settings import S


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    输入需含列: date, QQQ_Close, QQQ_Open, QQQ_Volume, TQQQ_Close
    输出追加: QQQ_MA20, QQQ_MA200, QQQ_ATH, QQQ_Top_Signal, QQQ_Bull,
              QQQ_VolMA, QQQ_Drawdown
    对齐 base.ts V22:
      - ATH = 全局历史最高收盘价 (cummax, 等价 base 的 ath_price = max(ath_price, close))
      - VolMA = 前 60 日成交量均值 (不含当日, rolling(60).mean().shift(1))
    """
    df = df.copy()

    df["QQQ_MA20"] = df["QQQ_Close"].rolling(S.MA_SHORT, min_periods=1).mean()
    df["QQQ_MA200"] = df["QQQ_Close"].rolling(S.MA_LONG, min_periods=1).mean()
    # 前一日 MA20（用于斜率判断）
    df["QQQ_MA20_prev"] = df["QQQ_MA20"].shift(1)

    # ATH: 全局历史最高（对齐 base.ts V22 _init_ath_price + 每日 ath_price = max(ath_price, close)）。
    # base 用预热期252根K线最高价初始化，之后每日取累积最高；整段 cummax() 等价于自数据起点起的全局最高。
    df["QQQ_ATH"] = df["QQQ_Close"].cummax()

    # 成交量均线: 前 60 日均值，不含当日（对齐 base.ts vol_ma select=2..61）
    df["QQQ_VolMA"] = df["QQQ_Volume"].rolling(S.VOL_WINDOW, min_periods=1).mean().shift(1)
    df["QQQ_VolMA"] = df["QQQ_VolMA"].fillna(
        df["QQQ_Volume"].rolling(S.VOL_WINDOW, min_periods=1).mean()
    )

    # 回撤（相对 ATH）
    df["QQQ_Drawdown"] = df["QQQ_Close"] / df["QQQ_ATH"] - 1.0

    # 多头市场：价格 > MA200
    df["QQQ_Bull"] = df["QQQ_Close"] > df["QQQ_MA200"]

    return df
