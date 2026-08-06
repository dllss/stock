# 量窒息战法选股扫描器

基于量窒息战法方法论，扫描A股中符合量窒息形态的股票。

## 快速开始

```bash
cd D:\WorkProject\stock

# 全市场扫描
python -m instock.backtest.volume_suffocation.run_full

# 科技板块扫描
python -m instock.backtest.volume_suffocation.run_tech

# AI硬件&软件扫描
python -m instock.backtest.volume_suffocation.run_ai
```

## 模块结构

```
volume_suffocation/
    __init__.py       包入口，导出公共API
    config.py         检测参数 + 行业关键词
    detector.py       核心检测逻辑
    data_loader.py    数据库读取 + 批量加载
    html_report.py    HTML报告生成器
    run_full.py       全市场扫描入口
    run_tech.py       科技板块扫描入口
    run_ai.py         AI硬件&软件扫描入口
    README.md         本文件
```

## 检测逻辑

量窒息战法的核心判断流程：

1. **前期高量** — 60天窗口的前30天找最大成交量作为基准
2. **量窒息** — 最近5日均量/前期高量 < 20%（< 15%为严重窒息）
3. **价格横盘** — 最近5天振幅 < 8%，价格区间占比 < 10%
4. **有回撤** — 距前期高点回撤 > 3%（确保确实有调整）
5. **三重确认** — ①收红K ②次日高开 ③支撑位（前低/箱底/MA20/大阳起涨点）

## 代码调用示例

```python
from instock.backtest.volume_suffocation import (
    detect_volume_suffocation,
    analyze_trend_status,
    PARAMS,
)

# 自定义参数
custom_params = PARAMS.copy()
custom_params['vol_ratio_threshold'] = 0.15  # 更严格的窒息阈值

# 对单只股票检测
result = detect_volume_suffocation(df, params=custom_params)
if result:
    print(f"量比: {result['vol_ratio']:.1%}")
    print(f"评分: {result['score']}")
    print(f"确认: {result['confirm_status']}")
```

## 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `lookback_days` | 60 | 回看窗口天数 |
| `high_vol_period` | 30 | 前期高量基准周期 |
| `suffocation_days` | 5 | 量窒息判定天数 |
| `vol_ratio_threshold` | 0.20 | 量窒息阈值（<20%） |
| `vol_ratio_strict` | 0.15 | 严重窒息阈值（<15%） |
| `max_amplitude_5d` | 8.0 | 5日最大振幅上限(%) |
| `max_price_range_5d` | 10.0 | 5日价格区间占比上限(%) |
| `min_drawdown_from_high` | 3 | 最小回撤(%) |
| `max_drawdown_from_high` | 999 | 最大回撤(%)，已取消限制 |
| `min_price` | 2.0 | 最低股价 |
| `max_price` | 500.0 | 最高股价 |
| `min_data_days` | 40 | 最少数据天数 |
