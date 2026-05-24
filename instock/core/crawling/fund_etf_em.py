#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
东方财富网ETF数据爬虫模块 - ETF基金行情数据
=============================================

功能说明：
本模块提供从东方财富网抓取ETF（交易型开放式指数基金）行情数据的功能，包括：
1. ETF实时行情（所有ETF的当前行情）
2. ETF历史K线数据
3. ETF基本信息

核心概念：
ETF（Exchange Traded Fund）是交易型开放式指数基金，在交易所上市交易。

ETF特点：
- 像股票一样交易（T+0或T+1）
- 跟踪特定指数（如沪深300、中证500等）
- 费用低廉，透明度高
- 适合分散投资和长期持有

ETF分类：
1. 宽基ETF：跟踪大盘指数
   - 沪深300ETF（510300）
   - 中证500ETF（510500）
   - 创业板ETF（159915）

2. 行业ETF：跟踪特定行业
   - 证券ETF（512880）
   - 银行ETF（512800）
   - 医药ETF（512010）

3. 主题ETF：跟踪特定主题
   - 新能源ETF（516160）
   - 芯片ETF（512760）
   - 人工智能ETF（515980）

4. 跨境ETF：跟踪海外市场
   - 恒生ETF（159920）
   - 纳指ETF（513100）
   - 日经ETF（513520）

5. 商品ETF：跟踪商品价格
   - 黄金ETF（518880）
   - 豆粕ETF（159985）

使用场景：
- ETF套利交易
- 指数化投资
- 行业轮动策略
- 资产配置
- 对冲工具

数据来源：
东方财富网 - 基金频道 - ETF行情
https://quote.eastmoney.com/center/gridlist.html#fund_etf

核心函数：
1. fund_etf_spot_em() - ETF实时行情
2. fund_etf_hist_em() - ETF历史K线数据

技术特点：
1. 分页爬取：支持大数据量的分页获取
2. 随机延迟：避免爬取过快被封IP
3. 缓存机制：使用@lru_cache()缓存代码映射
4. 数据清洗：自动转换数据类型和格式

API参数说明：
- fs参数：市场筛选条件
  "b:MK0021,b:MK0022,b:MK0023,b:MK0024"
  b:MK0021 - 上交所ETF
  b:MK0022 - 深交所ETF
  b:MK0023 - 其他ETF
  b:MK0024 - 跨境ETF
  
- fields参数：返回字段
  f2:最新价, f3:涨跌幅, f12:代码, f14:名称
  f5:成交量, f6:成交额, f8:换手率
  f15:最高, f16:最低, f17:开盘, f18:昨收
  f20:总市值, f21:流通市值

使用示例：
```python
# 获取所有ETF实时行情
df = fund_etf_spot_em()
print(f"共获取 {len(df)} 只ETF")

# 查看成交额最大的ETF
top_volume = df.nlargest(10, '成交额')
print("最活跃的ETF:")
print(top_volume[['代码', '名称', '成交额', '涨跌幅']])

# 筛选涨幅超过3%的ETF
strong_etf = df[df['涨跌幅'] > 3]
print(f"大涨ETF: {len(strong_etf)} 只")

# 按类型分类统计
etf_types = {
    '宽基': ['510300', '510500', '159915'],
    '行业': ['512880', '512800', '512010'],
}
```

注意事项：
1. ETF交易规则与股票略有不同（部分T+0）
2. ETF有溢价/折价现象（价格vs净值）
3. 跨境ETF受汇率影响
4. 流动性差异大，注意成交量
5. API接口可能变化，需要定期维护

性能优化：
1. 全局fetcher实例：复用连接池
2. LRU缓存：减少重复请求
3. 分页获取：每次50条数据
4. 批量处理：减少API调用次数

常见问题：

Q: ETF和股票有什么区别？
A: 
- ETF跟踪指数，股票是公司
- ETF可以T+0（部分），股票T+1
- ETF费用更低，透明度更高

Q: 如何选择ETF？
A: 
- 看规模（越大越好）
- 看流动性（成交量）
- 看跟踪误差（越小越好）
- 看费率（越低越好）

Q: ETF会清盘吗？
A: 会的，规模太小（通常<5000万）可能清盘

Q: ETF有涨跌幅限制吗？
A: 
- 境内ETF：10%（创业板/科创板20%）
- 跨境ETF：无限制

依赖关系：
- pandas：数据处理和DataFrame操作
- math：数学计算（ceil向上取整）
- random：生成随机延迟时间
- time：时间控制（sleep延迟）
- functools.lru_cache：缓存装饰器
- instock.core.eastmoney_fetcher：HTTP请求封装
"""

import random
import time
from datetime import datetime
from functools import lru_cache
import math
import pandas as pd
from instock.core.eastmoney_fetcher import eastmoney_fetcher
from instock.config.delay_manager import sleep_with_delay
from instock.core.crawling.kline_utils import KLINE_COLUMNS as KLINE_DAILY_COLUMNS, apply_kline_columns

__author__ = 'myh '
__date__ = '2025/12/31 '

# ==================== 全局HTTP请求器 ====================
# 创建全局实例，供所有函数使用
# 这样可以复用连接池，提高性能
fetcher = eastmoney_fetcher()


# ==================== ETF实时行情 ====================

"""
fund_etf_spot_em - 获取ETF实时行情数据

功能：
从东方财富网获取所有ETF基金的实时行情数据

数据来源：
东方财富网 - 基金频道 - ETF行情
https://quote.eastmoney.com/center/gridlist.html#fund_etf

返回数据包含的字段：
基础信息：
- 代码：ETF代码（6位数字）
- 名称：ETF名称

价格信息：
- 最新价：当前交易价格
- 涨跌幅：价格变化百分比
- 涨跌额：价格变化金额
- 开盘价：今日开盘价
- 最高价：今日最高价
- 最低价：今日最低价
- 昨收：昨日收盘价

成交信息：
- 成交量：成交数量（手）
- 成交额：成交金额（元）
- 换手率：当日换手率（%）

市值信息：
- 总市值：基金总规模（元）
- 流通市值：可流通部分市值（元）

执行流程：
1. 设置API参数（第1页，每页50条）
2. 发送HTTP请求获取第1页数据
3. 解析JSON，提取diff数组
4. 计算总页数
5. 循环获取剩余页面（每次延迟1-1.5秒）
6. 合并所有页面的数据
7. 创建DataFrame并重命名列
8. 选择需要的列并排序
9. 返回清洗后的DataFrame

分页逻辑：
- 每页50条数据
- 总页数 = ceil(总数据量 / 50)
- 逐页获取，避免一次性请求过多数据
- 每页之间随机延迟1-1.5秒

市场筛选条件（fs参数）：
"b:MK0021,b:MK0022,b:MK0023,b:MK0024"
- b:MK0021 - 上交所ETF（51开头）
- b:MK0022 - 深交所ETF（15/16开头）
- b:MK0023 - 其他ETF
- b:MK0024 - 跨境ETF

返回值：
pandas.DataFrame，包含所有ETF的实时行情数据
如果API返回空数据，返回空的DataFrame

使用示例：
```python
# 获取全市场ETF实时行情
df = fund_etf_spot_em()
print(f"共获取 {len(df)} 只ETF")

# 查看前10只ETF
print(df.head(10))

# 筛选高流动性的ETF（成交额>1亿）
high_liquidity = df[df['成交额'] > 100000000]
print(f"高流动性ETF: {len(high_liquidity)} 只")

# 按涨跌幅排序
top_gainers = df.nlargest(10, '涨跌幅')
print("涨幅最大的10只ETF:")
print(top_gainers[['代码', '名称', '涨跌幅', '成交额']])

# 按类型统计
# 宽基ETF
broad_base_codes = ['510300', '510500', '159915', '512100']
broad_base = df[df['代码'].isin(broad_base_codes)]
```

实战应用：

1. ETF轮动策略：
```python
# 找出近期表现最好的ETF类型
sector_etf = df[df['名称'].str.contains('证券|银行|保险|地产')]
best_sector = sector_etf.nlargest(1, '涨跌幅')
```

2. 流动性筛选：
```python
# 筛选适合交易的ETF
tradable_etf = df[
    (df['成交额'] > 50000000) &  # 成交额>5000万
    (df['换手率'] > 0.5)          # 换手率>0.5%
]
```

3. 溢价率分析：
```python
# 需要结合IOPV（净值估算）计算溢价率
# 溢价率 = (价格 - IOPV) / IOPV * 100%
```

注意事项：
1. 数据量大（几百只ETF），耗时较长（约1-2分钟）
2. 建议在非交易高峰期执行
3. 可以缓存结果，避免频繁调用
4. ETF代码规则：
   - 51xxxx - 上交所ETF
   - 15xxxx/16xxxx - 深交所ETF
5. 部分ETF流动性很差，注意风险

性能优化建议：
1. 只在必要时全量更新
2. 可以使用增量更新（只更新变化的ETF）
3. 考虑使用数据库存储历史快照
4. 多线程并行获取不同类型ETF

常见错误：
- 网络超时：增加重试机制
- API返回空数据：检查参数是否正确
- 数据解析失败：检查JSON结构是否变化

ETF基础知识：

1. 交易规则：
   - T+0：当日买入可当日卖出（部分ETF）
   - T+1：次日才能卖出（大部分ETF）
   - 无印花税
   - 佣金低（通常万分之1-3）

2. 价格机制：
   - 市场价格：交易所交易价格
   - IOPV：参考净值（每15秒更新）
   - 溢价：价格 > 净值
   - 折价：价格 < 净值

3. 主要ETF列表：
   宽基：
   - 510300 沪深300ETF
   - 510500 中证500ETF
   - 159915 创业板ETF
   - 512100 中证1000ETF
   
   行业：
   - 512880 证券ETF
   - 512800 银行ETF
   - 512010 医药ETF
   - 512660 军工ETF
   
   跨境：
   - 159920 恒生ETF
   - 513100 纳指ETF
   - 513520 日经ETF
   
   商品：
   - 518880 黄金ETF
   - 159985 豆粕ETF
"""
def fund_etf_spot_em() -> pd.DataFrame:
    """
    获取ETF实时行情数据
    
    返回：
    pd.DataFrame: 包含所有ETF实时行情的DataFrame
    
    异常：
    网络错误、API异常时会抛出异常
    空数据时返回空的DataFrame
    """
    
    # ==================== 步骤1: 设置API参数 ====================
    # ETF实时行情API地址
    url = "http://88.push2.eastmoney.com/api/qt/clist/get"
    
    # 分页参数
    page_size = 50      # 每页50条数据
    page_current = 1    # 从第1页开始
    
    # API请求参数
    params = {
        "pn": page_current,     # 页码（page number）
        "pz": page_size,        # 每页大小（page size）
        "po": "1",              # 排序方向：1=升序
        "np": "1",              # 不分页标识
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",  # 用户token（固定值）
        "fltt": "2",            # 过滤类型
        "invt": "2",            # 投资类型
        "wbp2u": "|0|0|0|web",  # Web端标识
        "fid": "f12",           # 排序字段：f12=ETF代码
        # 市场筛选条件：
        # b:MK0021 - 上交所ETF
        # b:MK0022 - 深交所ETF
        # b:MK0023 - 其他ETF
        # b:MK0024 - 跨境ETF
        "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024",
        # 返回字段列表
        # f2:最新价, f3:涨跌幅, f4:涨跌额, f5:成交量, f6:成交额
        # f8:换手率, f12:代码, f14:名称, f15:最高, f16:最低
        # f17:开盘, f18:昨收, f20:总市值, f21:流通市值
        # 等其他字段...
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
        "_": "1672806290972",   # 时间戳（防止缓存）
    }
    
    # ==================== 步骤2: 获取第1页数据 ====================
    # 使用全局fetcher发送HTTP请求
    print(f"\n{'='*80}")
    print(f"[INFO] 开始获取ETF实时行情数据...")
    print(f"[INFO] API URL: {url}")
    print(f"[INFO] API功能: 获取上交所、深交所及其他ETF的实时行情数据（价格、成交量、涨跌幅等）")
    r = fetcher.make_request(url, params=params)
    
    # 解析JSON响应
    data_json = r.json()
    
    # 提取diff数组（ETF数据列表）
    data = data_json["data"]["diff"]
    
    # 检查是否有数据
    if not data:
        # 如果没有数据，返回空的DataFrame
        return pd.DataFrame()
    
    # ==================== 步骤3: 计算总页数 ====================
    # 从响应中获取总数据量
    data_count = data_json["data"]["total"]
    
    # 计算总页数：向上取整
    page_count = math.ceil(data_count / page_size)
    import logging
    logging.info(f"总共{data_count}条记录，共{page_count}页，每页{page_size}条")
    logging.debug(f"已获取第1/{page_count}页 (累计{len(data)}条)")
    
    # ==================== 步骤4: 循环获取剩余页面 ====================
    # 当还有剩余页面时，继续获取
    while page_count > 1:
        # 添加随机延迟，控制每分钟请求数<10次
        delay_time = sleep_with_delay('normal')
        
        # 页码加1
        page_current = page_current + 1
        
        # 更新请求参数中的页码
        params["pn"] = page_current
        
        # 发送HTTP请求获取当前页数据
        r = fetcher.make_request(url, params=params)
        
        # 解析JSON响应
        data_json = r.json()
        
        # 提取当前页的ETF数据
        _data = data_json["data"]["diff"]
        
        # 将当前页数据追加到总数据列表
        data.extend(_data)
        
        # 剩余页数减1
        page_count = page_count - 1
        logging.debug(f"已获取第{page_current}/{page_current + page_count - 1}页 (累计{len(data)}条) [延迟{delay_time:.1f}秒]")
    
    # ==================== 步骤5: 创建DataFrame ====================
    # 将所有数据转换为pandas DataFrame
    temp_df = pd.DataFrame(data)
    
    # ==================== 步骤6: 重命名列 ====================
    # API返回的字段名是f2,f3,f4等，需要转换为中文列名
    temp_df.rename(
        columns={
            "f12": "代码",
            "f14": "名称",
            "f2": "最新价",
            "f3": "涨跌幅",
            "f4": "涨跌额",
            "f5": "成交量",
            "f6": "成交额",
            "f17": "开盘价",
            "f15": "最高价",
            "f16": "最低价",
            "f18": "昨收",
            "f8": "换手率",
            "f21": "流通市值",
            "f20": "总市值",
        },
        inplace=True,
    )
    
    # ==================== 步骤7: 选择并排序列 ====================
    # 按照逻辑顺序重新排列列
    temp_df = temp_df[
        [
            "代码",
            "名称",
            "最新价",
            "涨跌幅",
            "涨跌额",
            "成交量",
            "成交额",
            "开盘价",
            "最高价",
            "最低价",
            "昨收",
            "换手率",
            "流通市值",
            "总市值",
        ]
    ]
    
    # ==================== 步骤8: 返回结果 ====================
    # TODO: 这里应该添加数据类型转换
    # 例如：temp_df['最新价'] = pd.to_numeric(temp_df['最新价'], errors='coerce')
    return temp_df


# ==================== ETF代码映射缓存 ====================

@lru_cache()
def _fund_etf_code_id_map_em() -> dict:
    """
    获取ETF代码到市场ID的映射关系
    
    返回：
    dict: {ETF代码: 市场ID}
    
    异常：
    网络错误或API异常时会抛出异常
    """
    
    # ==================== 步骤1: 设置API参数 ====================
    url = "http://88.push2.eastmoney.com/api/qt/clist/get"
    
    params = {
        "pn": "1",              # 页码
        "pz": "5000",           # 每页5000条（一次性获取所有ETF）
        "po": "1",              # 排序方向
        "np": "1",              # 不分页标识
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",  # 用户token
        "fltt": "2",            # 过滤类型
        "invt": "2",            # 投资类型
        "wbp2u": "|0|0|0|web",  # Web端标识
        "fid": "f3",            # 排序字段：f3=涨跌幅
        # 市场筛选条件（与spot相同）
        "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024",
        # 只返回代码(f12)和市场ID(f13)两个字段
        "fields": "f12,f13",
        "_": "1672806290972",   # 时间戳
    }
    
    # ==================== 步骤2: 发送HTTP请求 ====================
    r = fetcher.make_request(url, params=params)
    
    # ==================== 步骤3: 解析JSON响应 ====================
    data_json = r.json()
    
    # 提取diff数组
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    
    # ==================== 步骤4: 创建映射字典 ====================
    # zip(代码列, 市场ID列) → 转换为字典
    # 例如：{'159707': 1, '510300': 0, ...}
    temp_dict = dict(zip(temp_df["f12"], temp_df["f13"]))
    
    # ==================== 步骤5: 返回映射字典 ====================
    return temp_dict


# ==================== ETF历史K线数据 ====================

def fund_etf_hist_em(
    symbol: str = "159707",
    period: str = "daily",
    start_date: str = "19700101",
    end_date: str = "20500101",
    adjust: str = "",
) -> pd.DataFrame:
    """
    获取ETF历史K线数据
    
    参数：
    symbol (str): ETF代码，如"159707"
    period (str): K线周期，'daily'/'weekly'/'monthly'
    start_date (str): 开始日期，格式YYYYMMDD
    end_date (str): 结束日期，格式YYYYMMDD
    adjust (str): 复权类型，''/'qfq'/'hfq'
    
    返回：
    pd.DataFrame: 包含历史K线数据的DataFrame
    
    异常：
    网络错误、API异常或ETF代码不存在时会抛出异常
    空数据时返回空的DataFrame
    """
    
    # ==================== 步骤1: 获取代码映射 ====================
    # 从缓存中获取ETF代码到市场ID的映射
    code_id_dict = _fund_etf_code_id_map_em()
    
    # ==================== 步骤2: 设置参数字典 ====================
    # 复权类型映射
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    
    # K线周期映射
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    
    # ==================== 步骤3: 构建API请求 ====================
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    
    params = {
        # 基础字段（不使用）
        "fields1": "f1,f2,f3,f4,f5,f6",
        # K线字段：
        # f51:日期, f52:开盘, f53:收盘, f54:最高, f55:最低
        # f56:成交量, f57:成交额, f58:振幅, f59:涨跌幅, f60:涨跌额
        # f61:换手率, f116:其他；当前接口通常只返回 11 列，解析层会兼容 11/12 列
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",  # 用户token
        "klt": period_dict[period],               # K线周期
        "fqt": adjust_dict[adjust],               # 复权类型
        # 证券ID：市场ID.代码
        # 例如：1.159707（深交所）、0.510300（上交所）
        "secid": f"{code_id_dict[symbol]}.{symbol}",
        "beg": start_date,                        # 开始日期
        "end": end_date,                          # 结束日期
        "_": "1623766962675",                     # 时间戳
    }
    
    # ==================== 步骤4: 发送HTTP请求 ====================
    r = fetcher.make_request(url, params=params)
    
    # ==================== 步骤5: 解析JSON响应 ====================
    data_json = r.json()
    
    # 检查是否有数据
    if not (data_json["data"] and data_json["data"]["klines"]):
        # 如果没有数据，返回空的DataFrame
        return pd.DataFrame()
    
    # ==================== 步骤6: 创建DataFrame ====================
    # klines是CSV格式的字符串数组，每行用逗号分隔
    # 例如：["2023-01-01,1.23,1.25,1.26,1.22,10000,12345,2.5,1.5,0.02,1.2", ...]
    temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
    
    # ==================== 步骤7: 重命名列 ====================
    temp_df = apply_kline_columns(temp_df)
    
    # ==================== 步骤8: 设置索引 ====================
    # 将日期列转换为datetime类型并设置为索引
    temp_df.index = pd.to_datetime(temp_df["日期"])
    
    # 重置索引（删除原来的索引，生成新的0-based索引）
    temp_df.reset_index(inplace=True, drop=True)
    
    # ==================== 步骤9: 转换数据类型 ====================
    # 将所有数值列转换为numeric类型
    temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
    temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
    temp_df["最高"] = pd.to_numeric(temp_df["最高"])
    temp_df["最低"] = pd.to_numeric(temp_df["最低"])
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"])
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])
    
    # ==================== 步骤10: 返回结果 ====================
    return temp_df


# ==================== ETF分时行情数据 ====================

def fund_etf_hist_min_em(
    symbol: str = "159707",
    start_date: str = "1979-09-01 09:32:00",
    end_date: str = "2222-01-01 09:32:00",
    period: str = "5",
    adjust: str = "",
) -> pd.DataFrame:
    """
    获取ETF分时行情数据
    
    参数：
    symbol (str): ETF代码，如"159707"
    start_date (str): 开始日期时间，格式"YYYY-MM-DD HH:MM:SS"
    end_date (str): 结束日期时间，格式"YYYY-MM-DD HH:MM:SS"
    period (str): 分时周期，'1'/'5'/'15'/'30'/'60'
    adjust (str): 复权类型，''/'qfq'/'hfq'
    
    返回：
    pd.DataFrame: 包含分时行情数据的DataFrame
    
    异常：
    网络错误、API异常或ETF代码不存在时会抛出异常
    空数据时返回空的DataFrame
    """
    
    # ==================== 步骤1: 获取代码映射 ====================
    code_id_dict = _fund_etf_code_id_map_em()
    
    # ==================== 步骤2: 设置复权映射 ====================
    adjust_map = {
        "": "0",      # 不复权
        "qfq": "1",   # 前复权
        "hfq": "2",   # 后复权
    }
    
    # ==================== 步骤3: 判断周期，选择API ====================
    if period == "1":
        # ===== 1分钟数据：使用trends2 API =====
        
        # ==================== 步骤3.1: 构建API请求 ====================
        url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
        
        params = {
            # 基础字段（不使用）
            "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
            # 分时字段：
            # f51:时间, f52:开盘, f53:收盘, f54:最高, f55:最低
            # f56:成交量, f57:成交额, f58:最新价
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",  # 用户token
            "ndays": "5",                               # 返回5天数据
            "iscr": "0",                                # 是否包含昨日收盘
            "secid": f"{code_id_dict[symbol]}.{symbol}",  # 证券ID
            "_": "1623766962675",                       # 时间戳
        }
        
        # ==================== 步骤3.2: 发送HTTP请求 ====================
        r = fetcher.make_request(url, params=params)
        
        # ==================== 步骤3.3: 解析JSON响应 ====================
        data_json = r.json()
        
        # ==================== 步骤3.4: 创建DataFrame ====================
        # trends是CSV格式的字符串数组
        temp_df = pd.DataFrame(
            [item.split(",") for item in data_json["data"]["trends"]]
        )
        
        # ==================== 步骤3.5: 重命名列 ====================
        temp_df.columns = [
            "时间",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "最新价",
        ]
        
        # ==================== 步骤3.6: 设置索引并过滤 ====================
        # 将时间列转换为datetime类型并设置为索引
        temp_df.index = pd.to_datetime(temp_df["时间"])
        
        # 根据日期时间范围过滤数据
        temp_df = temp_df[start_date:end_date]
        
        # 重置索引
        temp_df.reset_index(drop=True, inplace=True)
        
        # ==================== 步骤3.7: 转换数据类型 ====================
        temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
        temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
        temp_df["最高"] = pd.to_numeric(temp_df["最高"])
        temp_df["最低"] = pd.to_numeric(temp_df["最低"])
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
        temp_df["最新价"] = pd.to_numeric(temp_df["最新价"])
        
        # 将时间列转换为字符串格式
        temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
        
        # ==================== 步骤3.8: 返回结果 ====================
        return temp_df
        
    else:
        # ===== 其他周期数据：使用kline API =====
        
        # ==================== 步骤4.1: 构建API请求 ====================
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        
        params = {
            # 基础字段（不使用）
            "fields1": "f1,f2,f3,f4,f5,f6",
            # K线字段
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",  # 用户token
            "klt": period,                            # K线周期（5/15/30/60）
            "fqt": adjust_map[adjust],                # 复权类型
            "secid": f"{code_id_dict[symbol]}.{symbol}",  # 证券ID
            "beg": "0",                               # 开始时间（0表示最早）
            "end": "20500000",                        # 结束时间（2050年）
            "_": "1630930917857",                     # 时间戳
        }
        
        # ==================== 步骤4.2: 发送HTTP请求 ====================
        r = fetcher.make_request(url, params=params)
        
        # ==================== 步骤4.3: 解析JSON响应 ====================
        data_json = r.json()
        
        # ==================== 步骤4.4: 创建DataFrame ====================
        temp_df = pd.DataFrame(
            [item.split(",") for item in data_json["data"]["klines"]]
        )
        
        # ==================== 步骤4.5: 重命名列 ====================
        temp_df.columns = [
            "时间",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
        
        # ==================== 步骤4.6: 设置索引并过滤 ====================
        # 将时间列转换为datetime类型并设置为索引
        temp_df.index = pd.to_datetime(temp_df["时间"])
        
        # 根据日期时间范围过滤数据
        temp_df = temp_df[start_date:end_date]
        
        # 重置索引
        temp_df.reset_index(drop=True, inplace=True)
        
        # ==================== 步骤4.7: 转换数据类型 ====================
        temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
        temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
        temp_df["最高"] = pd.to_numeric(temp_df["最高"])
        temp_df["最低"] = pd.to_numeric(temp_df["最低"])
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
        temp_df["振幅"] = pd.to_numeric(temp_df["振幅"])
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])
        
        # 将时间列转换为字符串格式
        temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
        
        # ==================== 步骤4.8: 选择并排序列 ====================
        temp_df = temp_df[
            [
                "时间",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "涨跌幅",
                "涨跌额",
                "成交量",
                "成交额",
                "振幅",
                "换手率",
            ]
        ]
        
        # ==================== 步骤4.9: 返回结果 ====================
        return temp_df


# ==================== 测试代码 ====================

if __name__ == "__main__":
    # 测试1：获取ETF实时行情
    print("=" * 80)
    print("测试1：获取ETF实时行情")
    print("=" * 80)
    fund_etf_spot_em_df = fund_etf_spot_em()
    print(f"共获取 {len(fund_etf_spot_em_df)} 只ETF")
    print(fund_etf_spot_em_df.head())
    print()

    # 测试2：获取ETF历史K线（后复权）
    print("=" * 80)
    print("测试2：获取ETF历史K线（后复权）")
    print("=" * 80)
    fund_etf_hist_hfq_em_df = fund_etf_hist_em(
        symbol="513500",
        period="daily",
        start_date="20000101",
        end_date="20230201",
        adjust="hfq",
    )
    print(f"获取 {len(fund_etf_hist_hfq_em_df)} 条数据")
    print(fund_etf_hist_hfq_em_df.head())
    print()

    # 测试3：获取ETF历史K线（前复权）
    print("=" * 80)
    print("测试3：获取ETF历史K线（前复权）")
    print("=" * 80)
    fund_etf_hist_qfq_em_df = fund_etf_hist_em(
        symbol="513500",
        period="daily",
        start_date="20000101",
        end_date="20230201",
        adjust="qfq",
    )
    print(f"获取 {len(fund_etf_hist_qfq_em_df)} 条数据")
    print(fund_etf_hist_qfq_em_df.head())
    print()

    # 测试4：获取ETF历史K线（不复权）
    print("=" * 80)
    print("测试4：获取ETF历史K线（不复权）")
    print("=" * 80)
    fund_etf_hist_em_df = fund_etf_hist_em(
        symbol="513500",
        period="daily",
        start_date="20000101",
        end_date="20230201",
        adjust="",
    )
    print(f"获取 {len(fund_etf_hist_em_df)} 条数据")
    print(fund_etf_hist_em_df.head())
    print()

    # 测试5：获取ETF分时行情
    print("=" * 80)
    print("测试5：获取ETF分时行情（5分钟K线）")
    print("=" * 80)
    fund_etf_hist_min_em_df = fund_etf_hist_min_em(
        symbol="513500",
        period="5",
        adjust="hfq",
        start_date="2023-01-01 09:32:00",
        end_date="2023-01-04 14:40:00",
    )
    print(f"获取 {len(fund_etf_hist_min_em_df)} 条数据")
    print(fund_etf_hist_min_em_df.head())
