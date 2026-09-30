#!/usr/bin/env python3
"""
独立数据抓取：东方财富美股日线（secid=105.{ticker}，默认前复权 qfq）。
复用主工程 instock.core.eastmoney_fetcher 的持久 session（含 cookie/headers），
避免裸 requests 被服务端 RemoteDisconnected；含本地 CSV 缓存。
"""

import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from config.settings import S

# 引入主工程（instock 位于本工程上级目录的同级 instock/）
_INSTOCK_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
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
    adjust: qfq=前复权(默认), ''=不复权(raw，用于实盘真实价估算)
    """
    if adjust is None:
        adjust = S.ADJUST
    market = "105"  # 美股
    adjust_map = {"qfq": "1", "": "0"}
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "101",  # 日线
        "fqt": adjust_map.get(adjust, "1"),
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
    adj = S.ADJUST or "qfq"
    return os.path.join(S.CACHE_DIR, f"{ticker}_{adj}.csv")


def _meta_path(ticker: str) -> str:
    adj = S.ADJUST or "qfq"
    return os.path.join(S.CACHE_DIR, f"{ticker}_{adj}.meta.json")


def load_cached(ticker: str):
    """返回 (df, cached_at: datetime|None)。cached_at 为缓存写入的北京时间。"""
    p = _cache_path(ticker)
    if os.path.exists(p):
        df = pd.read_csv(p, parse_dates=["date"])
        cached_at = None
        mp = _meta_path(ticker)
        if os.path.exists(mp):
            try:
                import json
                with open(mp, "r", encoding="utf-8") as f:
                    cached_at = pd.to_datetime(json.load(f).get("cached_at"))
            except Exception:
                cached_at = None
        return df, cached_at
    return pd.DataFrame(), None


def save_cache(ticker: str, df: pd.DataFrame):
    os.makedirs(S.CACHE_DIR, exist_ok=True)
    df.to_csv(_cache_path(ticker), index=False)
    # 记录写入时间（北京时间），用于最后一天新鲜度校验
    try:
        import json
        with open(_meta_path(ticker), "w", encoding="utf-8") as f:
            json.dump({"cached_at": (datetime.utcnow() + timedelta(hours=8)).isoformat()}, f)
    except Exception:
        pass


def _now_bj():
    """当前北京时间（naive datetime）。"""
    return datetime.utcnow() + timedelta(hours=8)


def _close_time(end) -> "datetime":
    """
    美股 D 日 K 线的收盘定稿时刻（北京时间）。
    美股盘中实时 K 线的当日字段（含复权价）尚未定稿，要等收盘后才会写入终值。
    定稿时刻 = 北京时间 (D+1) 日 04:00。
    """
    end_dt = pd.to_datetime(end)
    return (end_dt + timedelta(days=1)).replace(hour=4, minute=0, second=0, microsecond=0)


def _safe_end(end):
    """
    美股收盘保护：避免抓到"今日"未定稿的 K 线。

    东方财富对当日盘中实时 K 线的当日字段尚未定稿，
    会返回不准确的当日值，导致回测误判暴跌。
    美股 D 日 K 线要等到北京时间 D+1 日 04:00 收盘后才定稿。
    因此：若 end 落在"尚未收盘定稿"的日期（now < end+1日04:00），
    自动把补抓终点退回为 end-1，等收盘后再抓定稿数据。

    返回 (end_safe, adjusted: bool)，adjusted=True 表示做了回退。
    """
    end_dt = pd.to_datetime(end)
    now_bj = _now_bj()
    # end 对应美股交易日 D 的 K 线，定稿时刻 = 北京时间 (D+1) 日 04:00
    if now_bj < _close_time(end_dt):
        yesterday = (end_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        return yesterday, True
    return end, False


def fetch_and_merge(tickers, start, end) -> pd.DataFrame:
    """
    抓取多个 ticker 并合并为宽表，列名 {TICKER}_Close 等。
    带本地缓存：先读缓存，不足部分补抓。
    含美股收盘保护：盘中不抓"今日"未定稿 K 线（见 _safe_end）；
    并对缓存"最后一天"做新鲜度校验——若该行是在定稿前盘中抓的脏值，
    强制重新抓取覆盖，避免复权口径不一致导致的误判暴跌。
    """
    end_safe, end_adjusted = _safe_end(end)
    if end_adjusted:
        print(
            f"[SAFE] 今日(={end_safe} 次日) K 线尚未收盘定稿，补抓终点回退为 {end_safe}"
            f"（避免盘中脏数据，北京时间次日 04:00 后再跑可抓定稿值）"
        )
    end = end_safe
    frames = {}
    for tk in tickers:
        cached, cached_at = load_cached(tk)
        if not cached.empty:
            # 缓存内区间足够则考虑复用，否则补抓
            cached_end = cached["date"].max()
            if cached_end >= pd.to_datetime(end):
                # 新鲜度校验：缓存最后一行 == end 时，若写入时间早于该日定稿时刻，
                # 说明是盘中抓的脏值（当日字段未定稿），必须重抓最后一天。
                last_day_dirty = False
                if cached_end.normalize() == pd.to_datetime(end).normalize():
                    if cached_at is None:
                        # 无 meta 记录写入时间，无法证明最后一天是定稿后抓的可靠值，保守重抓
                        last_day_dirty = True
                        print(
                            f"[STALE] {tk} 缓存最后一天 {pd.to_datetime(end).date()} 无写入时间记录，"
                            f"无法确认新鲜度，重抓覆盖以排除盘中脏值"
                        )
                    elif cached_at < _close_time(end):
                        last_day_dirty = True
                        print(
                            f"[STALE] {tk} 缓存最后一天 {pd.to_datetime(end).date()} 写入于 "
                            f"{cached_at}（早于收盘定稿 {_close_time(end)}），判定为盘中脏值，重抓覆盖"
                        )
                if not last_day_dirty:
                    # 命中缓存但必须按 end 截断，避免返回超出 end 的未来数据（前视偏差）
                    frames[tk] = cached[cached["date"] <= pd.to_datetime(end)].reset_index(drop=True)
                    print(
                        f"[CACHE] {tk} 命中缓存，截断至 {pd.to_datetime(end).date()} (共 {len(frames[tk])} 条)"
                    )
                    continue
                # 脏值：剔除缓存最后一天，补抓覆盖（保留更早的缓存行）
                cached = cached[cached["date"] < pd.to_datetime(end)].reset_index(drop=True)
                print(f"[CACHE] {tk} 剔除脏值最后一天，保留至 {cached['date'].max().date()} 后补抓")
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
    回测内部用前复权(qfq)算收益（锚定最新日=真实市价），与 human 实盘挂单口径一致；
    但本函数额外实时取一次不复权价作为挂单参考，避免依赖缓存末日值。
    实盘指令必须用真实市价估算股数，避免挂错单。

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
