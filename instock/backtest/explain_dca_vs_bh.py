"""定投 vs 梭哈 深层原因分析"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import pandas as pd, numpy as np
from datetime import date
import instock.lib.database as mdb

sql = """SELECT code, date, close FROM fund_etf_hist_em
    WHERE date >= '2019-01-01' AND date <= '2026-07-21'
    AND code IN ('513300','513500') ORDER BY code, date"""
df = pd.read_sql(sql, con=mdb.engine())
df['date'] = pd.to_datetime(df['date']).dt.date

START = date(2021, 1, 1); END = date(2026, 7, 21)

for code in ['513300', '513500']:
    name = {'513300': '纳斯达克', '513500': '标普500'}[code]
    data = df[df['code'] == code].sort_values('date').reset_index(drop=True)
    mask = [(d >= START) & (d <= END) for d in data['date']]
    data = data[mask].reset_index(drop=True)

    prices = data['close'].astype(float).values
    dates = data['date'].values
    sp = float(data.iloc[0]['close'])
    ep = float(data.iloc[-1]['close'])
    bh_ret = (ep / sp - 1) * 100
    n = len(prices)

    # ===== 策略D: 梭哈 =====
    d_vals = 1000000 * prices / sp
    d_rm = np.maximum.accumulate(d_vals)
    d_dd = (d_rm - d_vals) / d_rm * 100
    d_max_dd = d_dd.max()
    d_ret = bh_ret

    # ===== 策略A: 纯定投 =====
    cash = 1000000; shares = 0
    a_vals = np.zeros(n)
    a_buys = []
    for i in range(n):
        p = prices[i]
        if i >= 200 and i % 5 == 0 and cash >= 50000:
            cash -= 50000
            shares += 50000 / p
            a_buys.append((dates[i], p, cash + shares * p))
        a_vals[i] = cash + shares * p
    a_rm = np.maximum.accumulate(a_vals)
    a_dd = (a_rm - a_vals) / a_rm * 100
    a_ret = (a_vals[-1] / 1000000 - 1) * 100

    # ===== 策略B: 低估多买+MA200守卫 =====
    def calc_score(h, idx):
        c = float(h.iloc[idx]['close']); n1 = min(1500, idx); n2 = min(500, idx)
        p1 = h['close'].iloc[idx - n1:idx + 1].astype(float); pct1 = (p1 < c).sum() / len(p1) * 100
        p2 = h['close'].iloc[idx - n2:idx + 1].astype(float); pct2 = (p2 < c).sum() / len(p2) * 100
        sp_s = pct1 * 0.6 + pct2 * 0.4
        ma = h['close'].iloc[idx - 200:idx + 1].astype(float).mean(); dev = (c / ma - 1) * 100
        sm = np.clip((dev + 30) / 60 * 100, 0, 100)
        w20 = h['close'].iloc[idx - 19:idx + 1].astype(float); m20, s20 = w20.mean(), w20.std(ddof=0) or 1
        bb = np.clip((c - (m20 - 2 * s20)) / (4 * s20) * 100, 0, 100)
        w14 = h['close'].iloc[idx - 13:idx + 1].astype(float); d = w14.diff(); g = d.clip(lower=0); l = -d.clip(upper=0)
        ag = g.iloc[1:].mean(); al = l.iloc[1:].mean(); rsi = 100 - 100 / (1 + ag / al) if al > 0 else 50
        sr = np.clip((rsi - 25) / 50 * 100, 0, 100)
        return round(sp_s * 0.35 + sm * 0.30 + bb * 0.20 + sr * 0.15, 1)

    cash2 = 1000000; shares2 = 0
    b_vals = np.zeros(n)
    b_buys, b_sells = [], []
    for i in range(n):
        p = prices[i]
        if i >= 200 and i % 5 == 0:
            score = calc_score(data, i)
            ma200 = np.mean(prices[i - 199:i + 1])
            above = p > ma200
            # buy
            bn = 3 if score < 20 else 2 if score < 35 else 1 if score < 50 else 0
            mp = max(0, int(cash2 / 50000))
            bn = min(bn, mp)
            if bn > 0:
                amt = bn * 50000; cash2 -= amt; shares2 += amt / p; b_buys.append((dates[i], p, bn))
            # sell
            sn = 2 if not above and score > 60 else 0
            if sn > 0 and shares2 > 0:
                cur = shares2 * p
                max_p = int(cur / 50000)
                sn = min(sn, max_p)
                if sn > 0:
                    amt = sn * 50000
                    if amt > cur: amt = cur
                    cash2 += amt; shares2 -= amt / p; b_sells.append((dates[i], p, sn))
        b_vals[i] = cash2 + shares2 * p
    b_rm = np.maximum.accumulate(b_vals)
    b_dd = (b_rm - b_vals) / b_rm * 100
    b_ret = (b_vals[-1] / 1000000 - 1) * 100

    # ===== 输出 =====
    print()
    print('=' * 75)
    print(f'  {name} ({code}) — 定投 vs 梭哈 深层原因')
    print('=' * 75)
    print(f'  区间: {START} ~ {END}  起始价 {sp:.2f} → 终价 {ep:.2f}')
    print()

    # 收益对比
    print(f'  {"策略":<22s} {"收益":>8s} {"最大回撤":>8s} {"vs BH":>8s}')
    print(f'  {"─" * 48}')
    print(f'  {"D_首日梭哈":<22s} {d_ret:>+7.1f}% {d_max_dd:>7.1f}% {"基准":>8s}')
    print(f'  {"A_纯定投":<22s} {a_ret:>+7.1f}% {a_dd.max():>7.1f}% {a_ret-d_ret:>+7.1f}%')
    print(f'  {"B_低估多买+MA200守卫":<22s} {b_ret:>+7.1f}% {b_dd.max():>7.1f}% {b_ret-d_ret:>+7.1f}%')
    print()

    # 钱的时间价值
    print(f'  ┌── 核心原因1: 钱在市场上待的时间不同')
    print(f'  │')
    print(f'  │  买入持有: 100万从第一天就开始涨，享受完整 +{d_ret:.0f}%')
    print(f'  │')
    print(f'  │  纯定投: 分20批买入，越晚买的钱在市场上时间越短')
    if a_buys:
        first_d = str(a_buys[0][0]); first_p = a_buys[0][1]
        last_d = str(a_buys[-1][0]); last_p = a_buys[-1][1]
        first_ret = (ep / first_p - 1) * 100
        last_ret = (ep / last_p - 1) * 100
        print(f'  │    第1份买在 {first_d} 价格 {first_p:.4f} → 赚 {first_ret:+.1f}%')
        print(f'  │    第20份买在 {last_d} 价格 {last_p:.4f} → 赚 {last_ret:+.1f}%')
        avg_p = sum(b[1] for b in a_buys) / len(a_buys)
        print(f'  │    均价 {avg_p:.4f} 终价 {ep:.4f} → 均价入场收益率 {((ep/avg_p-1)*100):+.1f}%')
        print(f'  │')
        print(f'  │  起始价 {sp:.4f} 远低于均价 {avg_p:.4f}（因价格一直涨）')
        print(f'  │  → 定投是越买越贵，不是越买越便宜')
    print(f'  │')
    print(f'  │  而梭哈买在第1天价格 {sp:.4f}，全程享受暴涨')
    print(f'  └──')

    # 回撤分析
    dd_peak_a = np.argmax(a_rm[:np.argmax(a_dd) + 1]) if np.argmax(a_dd) > 0 else 0
    dd_trough_a = np.argmax(a_dd)
    dd_peak_d = np.argmax(d_rm[:np.argmax(d_dd) + 1]) if np.argmax(d_dd) > 0 else 0
    dd_trough_d = np.argmax(d_dd)

    print()
    print(f'  ┌── 核心原因2: 定投回撤为何跟梭哈差不多？')
    print(f'  │')
    print(f'  │  最大回撤发生在2022年大熊市')
    print(f'  │')
    print(f'  │  梭哈峰值: {dates[dd_peak_d]}  谷底: {dates[dd_trough_d]} 回撤 {d_max_dd:.1f}%')
    print(f'  │  定投峰值: {dates[dd_peak_a]}  谷底: {dates[dd_trough_a]} 回撤 {a_dd.max():.1f}%')
    print(f'  │')

    # 谷底时的仓位
    trough_idx = np.argmax(a_dd)
    cash_at_trough = a_vals[trough_idx] - a_buys[-1][2] if False else 0
    # compute actual
    c, s = 1000000, 0
    for i in range(trough_idx + 1):
        p = prices[i]
        if i >= 200 and i % 5 == 0 and c >= 50000:
            c -= 50000; s += 50000 / p
    invested = 1000000 - c
    invested_pct = invested / 1000000 * 100
    print(f'  │  2022年大跌谷底时，定投已经投入了 {invested_pct:.0f}%（{invested:.0f}万）')
    print(f'  │  已投入的部分，跟大盘同步亏损')
    print(f'  │  只剩 {c:.0f} 万现金在继续定投，但这点钱摊薄不了大局')
    print(f'  │')
    print(f'  │  结论: 定投的\"分批保护\"只保最后那一点还没买的钱')
    print(f'  │        已经进去的大头，一样经历完整的下跌')
    print(f'  └──')

    print()
    print(f'  ┌── 核心原因3: 熊市->牛市切换时，谁仓位重谁赢')
    print(f'  │')
    print(f'  │  2023年开始的暴涨中:')
    print(f'  │    梭哈: 100%仓位，吃满全程')
    print(f'  │    纯定投: 100%仓位（此时也买完了），跟梭哈差不多')
    print(f'  │    B策略: 涨到一半开始卖（因分数偏高），仓位下降')
    print(f'  │')
    print(f'  │  → 但B策略在2022年低价多买了（3份/次），所以起始成本更低')
    print(f'  │  → 即使卖出一些，剩余持仓成本低，最终收益仍不错')
    print(f'  └──')

# 汇总
print()
print('  ╔' + '═' * 73 + '╗')
print('  ║  终极结论                                               ║')
print('  ╠' + '═' * 73 + '╣')
print('  ║  定投不降低回撤  — 因为2022年底已经买得差不多了         ║')
print('  ║  定投降低了收益  — 因为大多数钱买在了比第1天更高的价格  ║')
print('  ║                                                         ║')
print('  ║  定投的真正优势: 防止你在2021年11月最高点一把梭         ║')
print('  ║  如果起点是牛市顶点，定投会赚，梭哈会亏                 ║')
print('  ║  如果起点是牛市起点（如2021年初），梭哈更优             ║')
print('  ║                                                         ║')
print('  ║  策略B的MA200守卫: 在2022年跌破年线时卖掉了              ║')
print('  ║  → 避免了部分下跌，但也错过了后续反弹（卖了就难买回）   ║')
print('  ╚' + '═' * 73 + '╝')
