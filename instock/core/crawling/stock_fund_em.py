#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
东方财富网资金流向爬虫模块 - 资金数据分析
============================================

功能说明：
本模块提供从东方财富网抓取股票和板块资金流向数据的功能，包括：
1. 个股资金流向排名（今日/3日/5日/10日）
2. 板块资金流向排名（行业/概念/地域）

核心概念：
资金流向是技术分析中的重要指标，反映市场中资金的进出情况。

资金分类：
- 超大单：单笔成交金额 > 100万元（机构资金）
- 大单：单笔成交金额 20-100万元（大户资金）
- 中单：单笔成交金额 5-20万元（中户资金）
- 小单：单笔成交金额 < 5万元（散户资金）

主力资金 = 超大单 + 大单
散户资金 = 中单 + 小单

关键指标：
1. 净流入额：流入金额 - 流出金额（单位：元）
   - 正值：资金净流入（买入多于卖出）
   - 负值：资金净流出（卖出多于买入）

2. 净占比：净流入额 / 总成交额 × 100%
   - 反映资金流动的强度
   - 占比越大，说明资金态度越明确

使用场景：
- 判断主力资金动向
- 识别热门板块
- 发现资金异动股票
- 辅助投资决策

数据来源：
东方财富网 - 数据中心 - 资金流向
https://data.eastmoney.com/zjlx/detail.html

核心函数：
1. stock_individual_fund_flow_rank() - 个股资金流向排名
2. stock_sector_fund_flow_rank() - 板块资金流向排名

技术特点：
1. 分页爬取：支持大数据量的分页获取（每页50条）
2. 随机延迟：避免爬取过快被封IP（1-1.5秒）
3. 多时间维度：支持今日/3日/5日/10日统计
4. 多板块类型：支持行业/概念/地域资金流
5. 数据清洗：自动过滤无效数据和转换类型

API参数说明：

个股排名：
- indicator（时间周期）：
  * "今日"：当日资金流向
  * "3日"：近3日累计资金流向
  * "5日"：近5日累计资金流向
  * "10日"：近10日累计资金流向

板块排名：
- indicator（时间周期）："今日"/"5日"/"10日"
- sector_type（板块类型）：
  * "行业资金流"：按行业分类
  * "概念资金流"：按概念主题分类
  * "地域资金流"：按地区分类

数据字段说明：

个股资金流向：
- 代码：股票代码
- 名称：股票名称
- 最新价：当前价格
- X日涨跌幅：对应周期的价格涨跌幅
- X日主力净流入-净额：主力资金净流入金额
- X日主力净流入-净占比：主力资金净流入占比
- X日超大单净流入-净额：超大单净流入金额
- X日超大单净流入-净占比：超大单净流入占比
- X日大单净流入-净额：大单净流入金额
- X日大单净流入-净占比：大单净流入占比
- X日中单净流入-净额：中单净流入金额
- X日中单净流入-净占比：中单净流入占比
- X日小单净流入-净额：小单净流入金额
- X日小单净流入-净占比：小单净流入占比

板块资金流向：
- 名称：板块名称
- X日涨跌幅：板块指数涨跌幅
- X日主力净流入-净额：板块整体主力资金净流入
- X日主力净流入-净占比：板块主力资金净流入占比
- （其他字段类似个股）
- X日主力净流入最大股：该板块中主力资金流入最多的股票

使用示例：
```python
# 获取今日个股资金流向排名
df_today = stock_individual_fund_flow_rank(indicator="今日")
print(f"共获取 {len(df_today)} 只股票")

# 查看主力资金净流入前10的股票
top_inflow = df_today.nlargest(10, '今日主力净流入-净额')
print("主力资金最看好的10只股票:")
print(top_inflow[['代码', '名称', '今日主力净流入-净额', '今日涨跌幅']])

# 获取5日板块资金流向（概念板块）
df_sector = stock_sector_fund_flow_rank(
    indicator="5日",
    sector_type="概念资金流"
)
print(f"共获取 {len(df_sector)} 个概念板块")

# 找出5日资金持续流入的板块
strong_sectors = df_sector[
    (df_sector['5日主力净流入-净额'] > 0) &
    (df_sector['5日涨跌幅'] > 0)
]
print("资金持续流入的强势板块:")
print(strong_sectors[['名称', '5日涨跌幅', '5日主力净流入-净额']])
```

实战应用：

1. 主力资金追踪：
```python
# 连续多日主力资金净流入的股票
df_5day = stock_individual_fund_flow_rank(indicator="5日")
df_10day = stock_individual_fund_flow_rank(indicator="10日")

# 5日和10日都为净流入
consistent_inflow = df_5day[
    (df_5day['5日主力净流入-净额'] > 0) &
    (df_5day['代码'].isin(df_10day[df_10day['10日主力净流入-净额'] > 0]['代码']))
]
```

2. 板块轮动分析：
```python
# 对比不同板块类型的资金流向
industry = stock_sector_fund_flow_rank("今日", "行业资金流")
concept = stock_sector_fund_flow_rank("今日", "概念资金流")

# 找出今日最热门的行业和概念
top_industry = industry.nlargest(3, '今日主力净流入-净额')
top_concept = concept.nlargest(3, '今日主力净流入-净额')
```

3. 资金异动预警：
```python
# 找出资金大幅流入但股价未涨的股票（潜在机会）
df = stock_individual_fund_flow_rank(indicator="今日")
opportunity = df[
    (df['今日主力净流入-净占比'] > 10) &  # 主力大幅流入
    (df['今日涨跌幅'] < 2)  # 但涨幅不大
]
```

注意事项：
1. 资金流向数据T+1日公布（次日晚上更新）
2. 净流入不等于股价一定上涨，需结合其他因素
3. 短期资金流动可能受消息面影响较大
4. 长期趋势更有参考价值
5. API接口可能变化，需要定期维护

性能优化：
1. 全局fetcher实例：复用连接池，提高性能
2. 分页获取：每次50条数据，避免单次请求过大
3. 随机延迟：1-1.5秒，平衡速度和稳定性
4. 批量处理：减少API调用次数

常见问题：

Q: 为什么关注资金流向？
A: 资金流向反映主力资金的真实意图，是重要的先行指标

Q: 主力资金净流入就一定涨吗？
A: 不一定。需要结合：
- 流入的持续性（单日vs多日）
- 流入的规模（小额vs大额）
- 股价位置（高位vs低位）
- 市场环境（牛市vs熊市）

Q: 如何识别假信号？
A: 
- 看持续性：连续多日流入更可靠
- 看规模：大额流入比小额更有意义
- 看配合：资金流入+成交量放大+价格上涨
- 看位置：低位流入比高位流入更安全

Q: 超大单、大单、中单、小单有什么区别？
A: 
- 超大单：机构资金，最具参考价值
- 大单：大户资金，有一定参考性
- 中单：中户资金，参考性一般
- 小单：散户资金，反向指标（散户追高杀跌）

依赖关系：
- pandas：数据处理和DataFrame操作
- json：JSON数据解析（板块数据使用JSONP格式）
- random：生成随机延迟时间
- time：时间控制（sleep延迟和生成时间戳）
- math：数学计算（ceil向上取整计算页数）
- instock.core.eastmoney_fetcher：HTTP请求封装
"""

import json
import logging
import random
import time
from datetime import datetime
import math
import pandas as pd
from instock.core.eastmoney_fetcher import eastmoney_fetcher
from instock.config.delay_manager import sleep_with_delay

__author__ = 'myh '
__date__ = '2025/12/31 '

# ==================== 全局HTTP请求器 ====================
# 创建全局实例，供所有函数使用
# 这样可以复用连接池，提高性能
fetcher = eastmoney_fetcher()


def stock_individual_fund_flow_rank(indicator: str = "5日") -> pd.DataFrame:
    """
    东方财富网-数据中心-资金流向-个股排名
    
    获取指定时间周期内的个股资金流向排名数据，支持今日/3日/5日/10日统计。
    
    参数说明：
        indicator (str): 时间周期，可选值：
            - "今日"：当日资金流向
            - "3日"：近3日累计资金流向
            - "5日"：近5日累计资金流向
            - "10日"：近10日累计资金流向
    
    返回：
        pd.DataFrame: 包含以下字段的DataFrame
            - 代码：股票代码
            - 名称：股票名称
            - 最新价：当前价格
            - X日涨跌幅：对应周期的价格涨跌幅
            - X日主力净流入-净额：主力资金净流入金额
            - X日主力净流入-净占比：主力资金净流入占比
            - X日超大单净流入-净额/净占比
            - X日大单净流入-净额/净占比
            - X日中单净流入-净额/净占比
            - X日小单净流入-净额/净占比
    
    数据来源：
        https://data.eastmoney.com/zjlx/detail.html
    
    技术实现：
        1. 构造API请求参数（根据不同周期选择不同的字段）
        2. 分页获取数据（每页50条）
        3. 合并所有页面数据
        4. 数据清洗和字段重命名
        5. 选择需要的列并返回
    
    使用示例：
        >>> # 获取今日资金流向
        >>> df_today = stock_individual_fund_flow_rank("今日")
        >>> print(df_today.head())
        
        >>> # 获取5日资金流向
        >>> df_5day = stock_individual_fund_flow_rank("5日")
        >>> print(df_5day.head())
    """
    # ==================== 步骤1: 定义不同周期的API参数字段映射 ====================
    # 每个周期对应不同的排序字段(fid)和数据字段(fields)
    # f62: 今日主力净流入, f267: 3日主力净流入, f164: 5日主力净流入, f174: 10日主力净流入
    indicator_map = {
        "今日": [
            "f62",  # 排序字段：今日主力净流入
            "f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124",  # 数据字段
        ],
        "3日": [
            "f267",  # 排序字段：3日主力净流入
            "f12,f14,f2,f127,f267,f268,f269,f270,f271,f272,f273,f274,f275,f276,f257,f258,f124",
        ],
        "5日": [
            "f164",  # 排序字段：5日主力净流入
            "f12,f14,f2,f109,f164,f165,f166,f167,f168,f169,f170,f171,f172,f173,f257,f258,f124",
        ],
        "10日": [
            "f174",  # 排序字段：10日主力净流入
            "f12,f14,f2,f160,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183,f260,f261,f124",
        ],
    }
    
    # ==================== 步骤2: 构造API请求URL和参数 ====================
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    page_size = 50  # 每页50条数据
    page_current = 1  # 当前页码
    
    params = {
        "fid": indicator_map[indicator][0],  # 排序字段
        "po": "1",  # 排序方向：1=降序
        "pz": page_size,  # 每页数量
        "pn": page_current,  # 当前页码
        "np": "1",  # 是否显示总数
        "fltt": "2",  # 复权类型
        "invt": "2",  # 投资类型
        "ut": "b2884a393a59ad64002292a3e90d46a5",  # 用户token
        "fs": "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2",  # 筛选条件（A股）
        "fields": indicator_map[indicator][1],  # 返回字段
    }
    
    # ==================== 步骤3: 发送第一次请求获取第一页数据 ====================
    print(f"\n{'='*80}")
    print(f"[INFO] 开始获取{indicator}资金流向数据...")
    print(f"[INFO] API URL: {url}")
    print(f"[INFO] API功能: 获取A股个股资金流向排名，包括主力、超大单、大单、中单、小单的净流入额和占比")
    r = fetcher.make_request(url, params=params)
    
    # 调试：检查响应内容
    try:
        data_json = r.json()
    except Exception as e:
        print(f"❌ JSON解析失败: {e}")
        print(f"   Status Code: {r.status_code}")
        print(f"   Content-Type: {r.headers.get('Content-Type')}")
        print(f"   Response Length: {len(r.content)} bytes")
        print(f"   First 500 chars: {r.text[:500]}")
        raise
    
    data = data_json["data"]["diff"]  # 获取数据列表
    data_count = data_json["data"]["total"]  # 获取总记录数
    page_count = math.ceil(data_count / page_size)  # 计算总页数
    import logging
    logging.info(f"总共{data_count}条记录，共{page_count}页，每页{page_size}条")
    logging.debug(f"已获取第1/{page_count}页 (累计{len(data)}条)")
    
    # ==================== 步骤4: 分页获取剩余数据 ====================
    # 如果有多页，继续请求直到获取所有数据
    while page_count > 1:
        # 添加随机延迟，控制每分钟请求数<10次
        delay_time = sleep_with_delay('normal')
        
        page_current = page_current + 1
        params["pn"] = page_current
        
        r = fetcher.make_request(url, params=params)
        data_json = r.json()
        _data = data_json["data"]["diff"]
        data.extend(_data)  # 将新数据追加到列表中
        page_count = page_count - 1
        logging.debug(f"已获取第{page_current}/{page_current + page_count - 1}页 (累计{len(data)}条) [延迟{delay_time:.1f}秒]")
    
    # ==================== 步骤5: 转换为DataFrame并清洗数据 ====================
    temp_df = pd.DataFrame(data)
    
    # 过滤掉无效数据（f2为"-"表示停牌或无数据）
    temp_df = temp_df[~temp_df["f2"].isin(["-"])]
    
    # ==================== 步骤6: 根据不同周期重命名列 ====================
    if indicator == "今日":
        temp_df.columns = [
            "最新价",
            "今日涨跌幅",
            "代码",
            "名称",
            "今日主力净流入-净额",
            "今日超大单净流入-净额",
            "今日超大单净流入-净占比",
            "今日大单净流入-净额",
            "今日大单净流入-净占比",
            "今日中单净流入-净额",
            "今日中单净流入-净占比",
            "今日小单净流入-净额",
            "今日小单净流入-净占比",
            "_",  # 占位符（不需要的字段）
            "今日主力净流入-净占比",
            "_",
            "_",
            "_",
        ]
        # 选择需要的列并重新排序
        temp_df = temp_df[
            [
                "代码",
                "名称",
                "最新价",
                "今日涨跌幅",
                "今日主力净流入-净额",
                "今日主力净流入-净占比",
                "今日超大单净流入-净额",
                "今日超大单净流入-净占比",
                "今日大单净流入-净额",
                "今日大单净流入-净占比",
                "今日中单净流入-净额",
                "今日中单净流入-净占比",
                "今日小单净流入-净额",
                "今日小单净流入-净占比",
            ]
        ]
    elif indicator == "3日":
        temp_df.columns = [
            "最新价",
            "代码",
            "名称",
            "_",
            "3日涨跌幅",
            "_",
            "_",
            "_",
            "3日主力净流入-净额",
            "3日主力净流入-净占比",
            "3日超大单净流入-净额",
            "3日超大单净流入-净占比",
            "3日大单净流入-净额",
            "3日大单净流入-净占比",
            "3日中单净流入-净额",
            "3日中单净流入-净占比",
            "3日小单净流入-净额",
            "3日小单净流入-净占比",
        ]
        temp_df = temp_df[
            [
                "代码",
                "名称",
                "最新价",
                "3日涨跌幅",
                "3日主力净流入-净额",
                "3日主力净流入-净占比",
                "3日超大单净流入-净额",
                "3日超大单净流入-净占比",
                "3日大单净流入-净额",
                "3日大单净流入-净占比",
                "3日中单净流入-净额",
                "3日中单净流入-净占比",
                "3日小单净流入-净额",
                "3日小单净流入-净占比",
            ]
        ]
    elif indicator == "5日":
        temp_df.columns = [
            "最新价",
            "代码",
            "名称",
            "5日涨跌幅",
            "_",
            "5日主力净流入-净额",
            "5日主力净流入-净占比",
            "5日超大单净流入-净额",
            "5日超大单净流入-净占比",
            "5日大单净流入-净额",
            "5日大单净流入-净占比",
            "5日中单净流入-净额",
            "5日中单净流入-净占比",
            "5日小单净流入-净额",
            "5日小单净流入-净占比",
            "_",
            "_",
            "_",
        ]
        temp_df = temp_df[
            [
                "代码",
                "名称",
                "最新价",
                "5日涨跌幅",
                "5日主力净流入-净额",
                "5日主力净流入-净占比",
                "5日超大单净流入-净额",
                "5日超大单净流入-净占比",
                "5日大单净流入-净额",
                "5日大单净流入-净占比",
                "5日中单净流入-净额",
                "5日中单净流入-净占比",
                "5日小单净流入-净额",
                "5日小单净流入-净占比",
            ]
        ]
    elif indicator == "10日":
        temp_df.columns = [
            "最新价",
            "代码",
            "名称",
            "_",
            "10日涨跌幅",
            "10日主力净流入-净额",
            "10日主力净流入-净占比",
            "10日超大单净流入-净额",
            "10日超大单净流入-净占比",
            "10日大单净流入-净额",
            "10日大单净流入-净占比",
            "10日中单净流入-净额",
            "10日中单净流入-净占比",
            "10日小单净流入-净额",
            "10日小单净流入-净占比",
            "_",
            "_",
            "_",
        ]
        temp_df = temp_df[
            [
                "代码",
                "名称",
                "最新价",
                "10日涨跌幅",
                "10日主力净流入-净额",
                "10日主力净流入-净占比",
                "10日超大单净流入-净额",
                "10日超大单净流入-净占比",
                "10日大单净流入-净额",
                "10日大单净流入-净占比",
                "10日中单净流入-净额",
                "10日中单净流入-净占比",
                "10日小单净流入-净额",
                "10日小单净流入-净占比",
            ]
        ]
    
    return temp_df


def stock_sector_fund_flow_rank(
    indicator: str = "10日", sector_type: str = "行业资金流"
) -> pd.DataFrame:
    """
    东方财富网-数据中心-资金流向-板块资金流排名
    
    获取指定时间周期和板块类型的资金流向排名数据。
    
    参数说明：
        indicator (str): 时间周期，可选值：
            - "今日"：当日资金流向
            - "5日"：近5日累计资金流向
            - "10日"：近10日累计资金流向
        
        sector_type (str): 板块类型，可选值：
            - "行业资金流"：按行业分类（如：银行、地产、医药等）
            - "概念资金流"：按概念主题分类（如：人工智能、新能源等）
            - "地域资金流"：按地区分类（如：北京、上海、广东等）
    
    返回：
        pd.DataFrame: 包含以下字段的DataFrame
            - 名称：板块名称
            - X日涨跌幅：板块指数涨跌幅
            - X日主力净流入-净额：板块整体主力资金净流入
            - X日主力净流入-净占比：板块主力资金净流入占比
            - X日超大单/大单/中单/小单净流入-净额/净占比
            - X日主力净流入最大股：该板块中主力资金流入最多的股票
    
    数据来源：
        https://data.eastmoney.com/bkzj/hy.html
    
    技术实现：
        1. 根据板块类型和时间周期构造API参数
        2. 使用JSONP格式获取数据（需要特殊解析）
        3. 分页获取所有数据
        4. 数据清洗和字段重命名
        5. 选择需要的列并返回
    
    使用示例：
        >>> # 获取今日行业资金流向
        >>> industry = stock_sector_fund_flow_rank("今日", "行业资金流")
        >>> print(industry.head())
        
        >>> # 获取5日概念资金流向
        >>> concept = stock_sector_fund_flow_rank("5日", "概念资金流")
        >>> print(concept.head())
    """
    # ==================== 步骤1: 定义板块类型映射 ====================
    # 2=行业, 3=概念, 1=地域
    sector_type_map = {
        "行业资金流": "2",
        "概念资金流": "3",
        "地域资金流": "1"
    }
    
    # ==================== 步骤2: 定义不同周期的API参数字段映射 ====================
    indicator_map = {
        "今日": [
            "f62",  # 排序字段
            "1",  # 统计周期
            "f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124",
        ],
        "5日": [
            "f164",
            "5",
            "f12,f14,f2,f109,f164,f165,f166,f167,f168,f169,f170,f171,f172,f173,f257,f258,f124",
        ],
        "10日": [
            "f174",
            "10",
            "f12,f14,f2,f160,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183,f260,f261,f124",
        ],
    }
    
    # ==================== 步骤3: 构造API请求URL和参数 ====================
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    page_size = 50
    page_current = 1
    
    params = {
        "pn": page_current,  # 当前页码
        "pz": page_size,  # 每页数量
        "po": "1",  # 排序方向
        "np": "1",  # 是否显示总数
        "ut": "b2884a393a59ad64002292a3e90d46a5",  # 用户token
        "fltt": "2",  # 复权类型
        "invt": "2",  # 投资类型
        "fid0": indicator_map[indicator][0],  # 排序字段
        "fs": f"m:90 t:{sector_type_map[sector_type]}",  # 板块筛选条件
        "stat": indicator_map[indicator][1],  # 统计周期
        "fields": indicator_map[indicator][2],  # 返回字段
        "rt": "52975239",  # 随机数
        "cb": "jQuery18308357908311220152_1589256588824",  # JSONP回调函数名
        "_": int(time.time() * 1000),  # 时间戳
    }
    
    # ==================== 步骤4: 发送第一次请求获取第一页数据 ====================
    print(f"\n{'='*80}")
    print(f"[INFO] 开始获取{indicator}{sector_type}数据...")
    print(f"[INFO] API URL: {url}")
    print(f"[INFO] API功能: 获取{sector_type}的资金流向排名，包括各板块的主力资金净流入和领涨股")
    r = fetcher.make_request(url, params=params)
    text_data = r.text
    
    # 解析JSONP格式数据（需要去除回调函数包装）
    # 格式：jQuery...({...}); 需要提取中间的JSON部分
    data_json = json.loads(text_data[text_data.find("{"):-2])
    data = data_json["data"]["diff"]
    
    data_count = data_json["data"]["total"]
    page_count = math.ceil(data_count / page_size)
    logging.info(f"总共{data_count}条记录，共{page_count}页，每页{page_size}条")
    logging.debug(f"已获取第1/{page_count}页 (累计{len(data)}条)")
    
    # ==================== 步骤5: 分页获取剩余数据 ====================
    while page_count > 1:
        # 添加随机延迟，控制每分钟请求数<10次
        delay_time = sleep_with_delay('normal')
        
        page_current = page_current + 1
        params["pn"] = page_current
        
        r = fetcher.make_request(url, params=params)
        text_data = r.text
        json_data = json.loads(text_data[text_data.find("{"):-2])
        _data = json_data["data"]["diff"]
        data.extend(_data)
        page_count = page_count - 1
        logging.debug(f"已获取第{page_current}/{page_current + page_count - 1}页 (累计{len(data)}条) [延迟{delay_time:.1f}秒]")
    
    # ==================== 步骤6: 转换为DataFrame并清洗数据 ====================
    temp_df = pd.DataFrame(data)
    
    # 过滤掉无效数据
    temp_df = temp_df[~temp_df["f2"].isin(["-"])]
    
    # ==================== 步骤7: 根据不同周期重命名列 ====================
    if indicator == "今日":
        temp_df.columns = [
            "-",
            "今日涨跌幅",
            "_",
            "名称",
            "今日主力净流入-净额",
            "今日超大单净流入-净额",
            "今日超大单净流入-净占比",
            "今日大单净流入-净额",
            "今日大单净流入-净占比",
            "今日中单净流入-净额",
            "今日中单净流入-净占比",
            "今日小单净流入-净额",
            "今日小单净流入-净占比",
            "-",
            "今日主力净流入-净占比",
            "今日主力净流入最大股",
            "今日主力净流入最大股代码",
            "是否净流入",
        ]
        
        temp_df = temp_df[
            [
                "名称",
                "今日涨跌幅",
                "今日主力净流入-净额",
                "今日主力净流入-净占比",
                "今日超大单净流入-净额",
                "今日超大单净流入-净占比",
                "今日大单净流入-净额",
                "今日大单净流入-净占比",
                "今日中单净流入-净额",
                "今日中单净流入-净占比",
                "今日小单净流入-净额",
                "今日小单净流入-净占比",
                "今日主力净流入最大股",
            ]
        ]
    elif indicator == "5日":
        temp_df.columns = [
            "-",
            "_",
            "名称",
            "5日涨跌幅",
            "_",
            "5日主力净流入-净额",
            "5日主力净流入-净占比",
            "5日超大单净流入-净额",
            "5日超大单净流入-净占比",
            "5日大单净流入-净额",
            "5日大单净流入-净占比",
            "5日中单净流入-净额",
            "5日中单净流入-净占比",
            "5日小单净流入-净额",
            "5日小单净流入-净占比",
            "5日主力净流入最大股",
            "_",
            "_",
        ]
        
        temp_df = temp_df[
            [
                "名称",
                "5日涨跌幅",
                "5日主力净流入-净额",
                "5日主力净流入-净占比",
                "5日超大单净流入-净额",
                "5日超大单净流入-净占比",
                "5日大单净流入-净额",
                "5日大单净流入-净占比",
                "5日中单净流入-净额",
                "5日中单净流入-净占比",
                "5日小单净流入-净额",
                "5日小单净流入-净占比",
                "5日主力净流入最大股",
            ]
        ]
    elif indicator == "10日":
        temp_df.columns = [
            "-",
            "_",
            "名称",
            "_",
            "10日涨跌幅",
            "10日主力净流入-净额",
            "10日主力净流入-净占比",
            "10日超大单净流入-净额",
            "10日超大单净流入-净占比",
            "10日大单净流入-净额",
            "10日大单净流入-净占比",
            "10日中单净流入-净额",
            "10日中单净流入-净占比",
            "10日小单净流入-净额",
            "10日小单净流入-净占比",
            "10日主力净流入最大股",
            "_",
            "_",
        ]
        
        temp_df = temp_df[
            [
                "名称",
                "10日涨跌幅",
                "10日主力净流入-净额",
                "10日主力净流入-净占比",
                "10日超大单净流入-净额",
                "10日超大单净流入-净占比",
                "10日大单净流入-净额",
                "10日大单净流入-净占比",
                "10日中单净流入-净额",
                "10日中单净流入-净占比",
                "10日小单净流入-净额",
                "10日小单净流入-净占比",
                "10日主力净流入最大股",
            ]
        ]
    
    return temp_df


if __name__ == "__main__":
    # ==================== 测试代码 ====================
    
    # 测试1：获取今日个股资金流向排名
    print("=" * 80)
    print("测试1：获取今日个股资金流向排名")
    print("=" * 80)
    stock_individual_fund_flow_rank_df = stock_individual_fund_flow_rank(indicator="今日")
    print(f"共获取 {len(stock_individual_fund_flow_rank_df)} 只股票")
    print(stock_individual_fund_flow_rank_df.head())
    print()
    
    # 测试2：获取3日个股资金流向排名
    print("=" * 80)
    print("测试2：获取3日个股资金流向排名")
    print("=" * 80)
    stock_individual_fund_flow_rank_df = stock_individual_fund_flow_rank(indicator="3日")
    print(f"共获取 {len(stock_individual_fund_flow_rank_df)} 只股票")
    print(stock_individual_fund_flow_rank_df.head())
    print()
    
    # 测试3：获取5日个股资金流向排名
    print("=" * 80)
    print("测试3：获取5日个股资金流向排名")
    print("=" * 80)
    stock_individual_fund_flow_rank_df = stock_individual_fund_flow_rank(indicator="5日")
    print(f"共获取 {len(stock_individual_fund_flow_rank_df)} 只股票")
    print(stock_individual_fund_flow_rank_df.head())
    print()
    
    # 测试4：获取10日个股资金流向排名
    print("=" * 80)
    print("测试4：获取10日个股资金流向排名")
    print("=" * 80)
    stock_individual_fund_flow_rank_df = stock_individual_fund_flow_rank(
        indicator="10日"
    )
    print(f"共获取 {len(stock_individual_fund_flow_rank_df)} 只股票")
    print(stock_individual_fund_flow_rank_df.head())
    print()
    
    # 测试5：获取5日概念板块资金流向排名
    print("=" * 80)
    print("测试5：获取5日概念板块资金流向排名")
    print("=" * 80)
    stock_sector_fund_flow_rank_df = stock_sector_fund_flow_rank(
        indicator="5日", sector_type="概念资金流"
    )
    print(f"共获取 {len(stock_sector_fund_flow_rank_df)} 个概念板块")
    print(stock_sector_fund_flow_rank_df.head())
    print()
    
    # 测试6：获取今日行业板块资金流向排名
    print("=" * 80)
    print("测试6：获取今日行业板块资金流向排名")
    print("=" * 80)
    stock_sector_fund_flow_rank_df = stock_sector_fund_flow_rank(
        indicator="今日", sector_type="行业资金流"
    )
    print(f"共获取 {len(stock_sector_fund_flow_rank_df)} 个行业板块")
    print(stock_sector_fund_flow_rank_df.head())
