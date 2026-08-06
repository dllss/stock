"""估值定投 — 最大回撤对比：A定投 / B MA200守卫 / C 极高才减 / D 梭哈"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd, numpy as np
from datetime import date
import logging; logging.basicConfig(level=logging.WARNING)
import instock.lib.database as mdb
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

TOTAL_CAPITAL = 1_000_000
PORTIONS = 20; PORTION_SIZE = TOTAL_CAPITAL / PORTIONS
CHECK_INTERVAL = 5

sql = """SELECT code, date, open, close FROM fund_etf_hist_em
    WHERE date >= '2019-01-01' AND date <= '2026-07-21'
    AND code IN ('512890','513300','513500') ORDER BY code, date"""
df = pd.read_sql(sql, con=mdb.engine())
df['date'] = pd.to_datetime(df['date']).dt.date

START_DATE = date(2021, 1, 1); END_DATE = date(2026, 7, 21)
ETF_NAMES = {'513300':'纳斯达克','512890':'红利低波','513500':'标普500'}

def calc_score(history, idx):
    close=float(history.iloc[idx]['close'])
    if idx<200: return 50
    n1500=min(1500,idx); n500=min(500,idx)
    p1500=history['close'].iloc[idx-n1500:idx+1].astype(float);pct1500=(p1500<close).sum()/len(p1500)*100
    p500=history['close'].iloc[idx-n500:idx+1].astype(float);pct500=(p500<close).sum()/len(p500)*100
    s_pct=(pct1500*0.6+pct500*0.4)
    ma200=history['close'].iloc[idx-min(200,idx):idx+1].astype(float).mean();dev=(close/ma200-1)*100
    s_ma=np.clip((dev+30)/60*100,0,100)
    w20=history['close'].iloc[idx-min(20,idx):idx+1].astype(float);m20,s20=w20.mean(),w20.std(ddof=0)
    bb=np.clip((close-(m20-2*s20))/(4*s20)*100 if s20>0 else 50,0,100)
    w14=history['close'].iloc[idx-min(14,idx):idx+1].astype(float);d=w14.diff();g=d.clip(lower=0);l=-d.clip(upper=0)
    ag=g.iloc[1:].mean();al=l.iloc[1:].mean();rsi=100-100/(1+ag/al) if al>0 else (100 if ag>0 else 50)
    s_rsi=np.clip((rsi-25)/50*100,0,100)
    return round(s_pct*0.35+s_ma*0.30+bb*0.20+s_rsi*0.15,1)

def backtest_daily(data, buy_rule, sell_rule, label):
    """逐日跟踪，返回每日组合净值和最大回撤"""
    cash = TOTAL_CAPITAL; shares = 0.0
    daily_values = []
    n_buys = 0; n_sells = 0

    for i in range(len(data)):
        row = data.iloc[i]; d = row['date']; price = float(row['close'])
        if i < 200:
            daily_values.append(cash + shares * price)
            continue

        is_check = (i % CHECK_INTERVAL == 0)

        if is_check:
            score = calc_score(data, i)
            ma200 = data['close'].iloc[i-min(200,i):i+1].astype(float).mean()
            above_ma200 = price > ma200

            bn = buy_rule(score, above_ma200); sn = sell_rule(score, above_ma200)

            if bn > 0:
                p = min(bn, int(cash / PORTION_SIZE))
                if p > 0:
                    amt = p * PORTION_SIZE; shares += amt/price; cash -= amt; n_buys += 1
            if sn > 0 and shares > 0:
                cur_val = shares * price
                max_sell = int(cur_val / PORTION_SIZE)
                p = min(sn, max_sell)
                if p > 0:
                    amt = p * PORTION_SIZE
                    if amt > cur_val: amt = cur_val
                    shares -= amt/price; cash += amt; n_sells += 1

        daily_values.append(cash + shares * price)

    vals = np.array(daily_values)
    running_max = np.maximum.accumulate(vals)
    drawdowns = (running_max - vals) / running_max * 100
    max_dd = drawdowns.max()
    final_val = vals[-1]
    end_p = float(data.iloc[-1]['close']); start_p = float(data.iloc[0]['close'])
    bh = (end_p/start_p - 1) * 100
    ret = (final_val/TOTAL_CAPITAL - 1) * 100
    return {'ret':ret, 'bh':bh, 'excess':ret-bh, 'max_dd':max_dd,
            'final':final_val, 'buys':n_buys, 'sells':n_sells,
            'vals':vals, 'drawdowns':drawdowns, 'running_max':running_max,
            'dates':[data.iloc[i]['date'] for i in range(len(data))]}

# 策略定义
STRATEGIES = {
    'A_纯定投(每5天1份)': (
        lambda s,m: 1,
        lambda s,m: 0,
    ),
    'B_低估多买+MA200守卫': (
        lambda s,m: 3 if s<20 else 2 if s<35 else 1 if s<50 else 0,
        lambda s,m: 2 if not m and s>60 else 0,
    ),
    'C_低估重仓+极高才减': (
        lambda s,m: 3 if s<25 else 2 if s<40 else 1 if s<55 else 0,
        lambda s,m: 1 if s>90 else 0,
    ),
    'D_首日全仓梭哈': (
        lambda s,m: 20,
        lambda s,m: 0,
    ),
}

RESULTS = {}
for code in ['513300','512890','513500']:
    data = df[df['code']==code].sort_values('date').reset_index(drop=True)
    mask = [(d>=START_DATE)&(d<=END_DATE) for d in data['date']]
    data = data[mask].reset_index(drop=True)
    RESULTS[code] = {}
    for sname, (buy_f, sell_f) in STRATEGIES.items():
        RESULTS[code][sname] = backtest_daily(data.copy(), buy_f, sell_f, sname)

# ===== 文字表格 =====
print()
print('='*110)
print(f'  {"四大策略 收益 & 最大回撤对比":^95s}')
print(f'  {START_DATE} ~ {END_DATE} (5.5年) | 本金100万')
print('='*110)

for sname in STRATEGIES:
    print(f'\n  ┌─ {sname}')
    print(f'  │  {"ETF":<14s} {"策略收益":>8s} {"买入持有":>8s} {"超额":>8s} {"最大回撤":>8s} {"BH回撤":>8s} {"回撤改善":>8s} {"买/卖":>6s}')
    print(f'  │  {"─"*90}')

    for code in ['513300','512890','513500']:
        r = RESULTS[code][sname]
        # 计算买入持有的最大回撤
        prices = np.array([float(x) for x in df[(df['code']==code)&([(d>=START_DATE)&(d<=END_DATE) for d in df['date']])]['close'].values])
        # 直接用 same data
        data2 = df[df['code']==code].sort_values('date').reset_index(drop=True)
        mask2 = [(d>=START_DATE)&(d<=END_DATE) for d in data2['date']]
        prices2 = data2[mask2]['close'].astype(float).values
        bh_running_max = np.maximum.accumulate(prices2)
        bh_drawdowns = (bh_running_max - prices2) / bh_running_max * 100
        bh_max_dd = bh_drawdowns.max()

        dd_improve = bh_max_dd - r['max_dd']
        print(f'  │  {ETF_NAMES[code]:<14s} {r["ret"]:>+7.1f}% {r["bh"]:>+7.1f}% {r["excess"]:>+7.1f}% {r["max_dd"]:>7.1f}% {bh_max_dd:>7.1f}% {dd_improve:>+7.1f}% {r["buys"]:>2d}/{r["sells"]:<2d}')

    # 等权平均
    avg_ret = np.mean([RESULTS[c][sname]['ret'] for c in ['513300','512890','513500']])
    avg_bh = np.mean([RESULTS[c][sname]['bh'] for c in ['513300','512890','513500']])
    avg_dd = np.mean([RESULTS[c][sname]['max_dd'] for c in ['513300','512890','513500']])
    avg_bh_dd = np.mean([bh_max_dd for c in ['513300','512890','513500']])
    # recalc bh_dd properly
    bh_dds = []
    for code in ['513300','512890','513500']:
        d2 = df[df['code']==code].sort_values('date').reset_index(drop=True)
        m2 = [(d>=START_DATE)&(d<=END_DATE) for d in d2['date']]
        ps = d2[m2]['close'].astype(float).values
        rmax = np.maximum.accumulate(ps)
        bhd = (rmax - ps) / rmax * 100
        bh_dds.append(bhd.max())
    avg_bh_dd = np.mean(bh_dds)
    print(f'  │  {"─"*90}')
    print(f'  │  {"三ETF等权":<14s} {avg_ret:>+7.1f}% {avg_bh:>+7.1f}% {avg_ret-avg_bh:>+7.1f}% {avg_dd:>7.1f}% {avg_bh_dd:>7.1f}% {avg_bh_dd-avg_dd:>+7.1f}%')

# ===== 多子图K线 =====
# 每个ETF一张图，画净值曲线、回撤曲线
for code in ['513300','512890','513500']:
    data = df[df['code']==code].sort_values('date').reset_index(drop=True)
    mask = [(d>=START_DATE)&(d<=END_DATE) for d in data['date']]
    data = data[mask].reset_index(drop=True)
    dates = data['date'].values
    prices = data['close'].astype(float).values
    bh_rm = np.maximum.accumulate(prices)
    bh_dd = (bh_rm - prices) / bh_rm * 100

    fig, axes = plt.subplots(3, 1, figsize=(20, 12), sharex=True,
                              gridspec_kw={'height_ratios': [3, 1.5, 1.5]})

    # 图1：价格 + 各策略净值
    ax_p = axes[0]
    ax_p2 = ax_p.twinx()
    ax_p.plot(dates[200:], prices[200:], color='black', linewidth=1.0, alpha=0.4, label='收盘价')
    colors = {'A_纯定投(每5天1份)':'#1565c0',
              'B_低估多买+MA200守卫':'#2e7d32',
              'C_低估重仓+极高才减':'#e65100',
              'D_首日全仓梭哈':'#c62828'}
    for sname in STRATEGIES:
        r = RESULTS[code][sname]
        v = r['vals'][200:] / TOTAL_CAPITAL  # 归一化
        ax_p2.plot(dates[200:], v, color=colors[sname], linewidth=1.0, alpha=0.85, label=sname.split('_')[0])
    ax_p.set_ylabel('Price'); ax_p2.set_ylabel('Portfolio Value / Initial')
    ax_p.legend(loc='upper left', fontsize=7)
    ax_p2.legend(loc='upper right', fontsize=7)
    ax_p.set_title(f'{ETF_NAMES[code]} ({code}) — 策略净值曲线', fontsize=12, fontweight='bold')
    ax_p.grid(True, alpha=0.3)

    # 图2：各策略最大回撤
    ax_dd = axes[1]
    for sname in STRATEGIES:
        r = RESULTS[code][sname]
        ax_dd.fill_between(dates[200:], 0, -r['drawdowns'][200:],
                           color=colors[sname], alpha=0.15, label=sname.split('_')[0])
        ax_dd.plot(dates[200:], -r['drawdowns'][200:],
                   color=colors[sname], linewidth=0.8, alpha=0.7)
    ax_dd.axhline(y=-bh_dd.max(), color='black', linewidth=0.5, linestyle=':', alpha=0.5,
                  label=f'BH MaxDD {bh_dd.max():.1f}%')
    ax_dd.set_ylabel('Drawdown %'); ax_dd.set_ylim(-max(bh_dd.max()*1.15, 40), 0)
    ax_dd.legend(loc='lower left', fontsize=7)
    ax_dd.grid(True, alpha=0.3)

    # 图3：买入持有回撤
    ax_bh = axes[2]
    ax_bh.fill_between(dates[200:], 0, -bh_dd[200:], color='black', alpha=0.1)
    ax_bh.plot(dates[200:], -bh_dd[200:], color='black', linewidth=0.8)
    ax_bh.set_ylabel('BH Drawdown %'); ax_bh.set_xlabel('Date')
    ax_bh.set_ylim(-bh_dd.max()*1.15, 0)
    ax_bh.grid(True, alpha=0.3)

    for ax in axes:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right', fontsize=7)

    plt.tight_layout()
    outdir = os.path.join(os.path.dirname(__file__), '..', '..', 'script', 'plots')
    os.makedirs(outdir, exist_ok=True)
    fpath = os.path.join(outdir, f'{code}_dd_compare.png')
    fig.savefig(fpath, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f'\n  📊 回撤对比图已保存: {fpath}')
