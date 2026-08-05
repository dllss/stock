#!/usr/bin/env python3
"""
独立数据抓取：东方财富美股日线（secid=105.{ticker}，后复权）。
复用主工程 instock.core.eastmoney_fetcher 的持久 session（含 cookie/headers），
避免裸 requests 被服务端 RemoteDisconnected；含本地 CSV 缓存。
"""

import os
import sys
import time

import pandas as pd
import requests

from config.settings import S

# 引入主工程（instock 位于本工程上级目录的同级 instock/）
_INSTOCK_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _INSTOCK_ROOT not in sys.path:
    sys.path.insert(0, _INSTOCK_ROOT)
from instock.core.eastmoney_fetcher import eastmoney_fetcher  # noqa: E402

# 复用主工程验证过的单例 fetcher（自带 cookie / 完整 headers / session）
_em = eastmoney_fetcher()

KLINE_URL = "http://push2his.eastmoney.com/api/qt/stock/kline/get"


def _make_request(params, retries=3):
    """复用主工程 eastmoney_fetcher 的 session 发请求；失败直接上抛，
    交由上层 fetch_and_merge 的缓存降级逻辑处理（不进入交互式 input）。"""
    last_err = None
    for i in range(retries):
        try:
            r = _em.make_request(KLINE_URL, params=params, timeout=15, show_detail_log=False)
            if r.status_code == 200:
                return r.json()
        except requests.exceptions.RequestException as e:
            last_err = e
            time.sleep(1.5 * (i + 1))
    if last_err:
        raise last_err
    raise RuntimeError(f"请求失败: {KLINE_URL} params={params}")


def fetch_ticker(ticker: str, start: str, end: str, adjust: str = None) -> pd.DataFrame:
    """
    抓取单只美股日线。返回 DataFrame，列:
    date, Open, Close, High, Low, Volume
    adjust: hfq=后复权(默认), qfq=前复权, ''=不复权
    """
    if adjust is None:
        adjust = S.ADJUST
    market = "105"  # 美股
    adjust_map = {"qfq": "1", "hfq": "2", "": "0"}
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "101",  # 日线
        "fqt": adjust_map.get(adjust, "2"),
        "secid": f"{market}.{ticker}",
        "beg": start.replace("-", ""),
        "end": end.replace("-", ""),
        "_": "1623766962675",
    }
    data = _make_request(params)
    if not (data.get("data") and data["data"].get("klines")):
        return pd.DataFrame()

    rows = [ln.split(",") for ln in data["data"]["klines"]]
    df = pd.DataFrame(rows)
    # 东方财富 fields2 顺序: f51日期 f52开 f53收 f54高 f55低
    # f56量 f57额 f58盘前/盘后? 取前6列核心字段
    df = df.iloc[:, :6].copy()
    df.columns = ["date", "Open", "Close", "High", "Low", "Volume"]
    for c in ["Open", "Close", "High", "Low", "Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def _cache_path(ticker: str) -> str:
    adj = S.ADJUST or "hfq"
    return os.path.join(S.CACHE_DIR, f"{ticker}_{adj}.csv")


def load_cached(ticker: str) -> pd.DataFrame:
    p = _cache_path(ticker)
    if os.path.exists(p):
        return pd.read_csv(p, parse_dates=["date"])
    return pd.DataFrame()


def save_cache(ticker: str, df: pd.DataFrame):
    os.makedirs(S.CACHE_DIR, exist_ok=True)
    df.to_csv(_cache_path(ticker), index=False)


def _safe_end(end):
    """
    美股收盘保护：避免在盘中（北京时间 < 04:00）抓到"今日"未定稿的 K 线。

    东方财富对当日盘中实时 K 线的 hfq(后复权) 字段不乘复权因子，
    会直接返回前复权/真实市价，导致回测误判暴跌。
    因此：若 end 为"今天"且当前北京时间尚未到 04:00（美股未收盘），
    自动把补抓终点退回为昨天，等收盘后再抓今日定稿数据。

    返回 (end_safe, adjusted: bool)，adjusted=True 表示做了回退。
    """
    from datetime import datetime, timezone, timedelta

    end_dt = pd.to_datetime(end)
    now_bj = datetime.now(timezone.utc) + timedelta(hours=8)  # 北京时间
    today_bj = now_bj.date()
    # 美股常规收盘 = 北京时间当日 04:00；未到则今日 K 线尚未定稿
    market_closed = now_bj.hour >= 4
    if end_dt.date() == today_bj and not market_closed:
        yesterday = (end_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        return yesterday, True
    return end, False


def fetch_and_merge(tickers, start, end) -> pd.DataFrame:
    """
    抓取多个 ticker 并合并为宽表，列名 {TICKER}_Close 等。
    带本地缓存：先读缓存，不足部分补抓。
    含美股收盘保护：盘中不抓"今日"未定稿 K 线（见 _safe_end）。
    """
    end_safe, end_adjusted = _safe_end(end)
    if end_adjusted:
        print(
            f"[SAFE] 当前北京时间未到 04:00（美股未收盘），今日 K 线未定稿，"
            f"补抓终点回退为 {end_safe}（避免盘中脏数据）"
        )
    end = end_safe
    frames = {}
    for tk in tickers:
        cached = load_cached(tk)
        if not cached.empty:
            # 缓存内区间足够则直接用，否则补抓
            cached_end = cached["date"].max()
            if cached_end >= pd.to_datetime(end):
                # 命中缓存但必须按 end 截断，避免返回超出 end 的未来数据（前视偏差）
                frames[tk] = cached[cached["date"] <= pd.to_datetime(end)].reset_index(drop=True)
                print(
                    f"[CACHE] {tk} 命中缓存，截断至 {pd.to_datetime(end).date()} (共 {len(frames[tk])} 条)"
                )
                continue
            print(f"[CACHE] {tk} 缓存截止 {cached_end.date()} 不足，补抓")
        print(f"[EASTMONEY] 下载 {tk} (复权={S.ADJUST}) ...")
        fresh = fetch_ticker(tk, start, end, adjust=S.ADJUST)
        if fresh.empty:
            print(f"[WARN] {tk} 无数据")
            frames[tk] = cached if not cached.empty else pd.DataFrame()
            continue
        if not cached.empty:
            merged = pd.concat([cached, fresh]).drop_duplicates("date").sort_values("date")
        else:
            merged = fresh
        save_cache(tk, merged)
        frames[tk] = merged
        print(f"[EASTMONEY] {tk} 完成，共 {len(merged)} 条")

    if not frames:
        return pd.DataFrame()

    # 以第一个 ticker 的日期为基准对齐
    base = None
    for tk, df in frames.items():
        df = df.rename(columns=lambda c, tk=tk: f"{tk}_{c}" if c != "date" else c)
        base = df if base is None else base.merge(df, on="date", how="outer")
    base = base.sort_values("date").reset_index(drop=True)
    return base


def latest_raw_close(ticker: str) -> float:
    """
    取不复权(fqt=0)的最新收盘价，用于实盘指令估算股数。
    回测内部用后复权(hfq)算收益更准确，但 human 实盘挂单要用真实市价，
    两者口径不同：后复权会把历史分红再投资折算进价格(如 QQQ 后复权 1839、
    真实市价约 724)。实盘指令必须用真实市价估算股数，避免挂错单。

    实时取价可能失败（盘中超时 / 主工程 emoji 在 GBK 终端崩溃等），
    失败时返回 0.0，交由调用方回退到回测后复权收盘价估算。
    """
    try:
        df = fetch_ticker(ticker, start="20200101", end="20991231", adjust="")
    except Exception:  # noqa: BLE001
        return 0.0
    if df.empty:
        return 0.0
    # 按日期取最新一根 K 线的收盘价（fetch_ticker 已按 date 升序，tail(1) 即最新）
    return float(df.sort_values("date")["Close"].iloc[-1])
