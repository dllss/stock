#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
东方财富网大宗交易数据爬虫模块 - 大宗交易分析
==============================================

功能说明：
本模块提供从东方财富网抓取股票大宗交易数据的功能，包括：
1. 市场统计（每日大宗交易总体情况）
2. 每日明细（具体交易记录）
3. 个股统计
4. 营业部统计

核心概念：
大宗交易（Block Trade）是指单笔交易规模较大的证券交易。

大宗交易特点：
- 交易量大：通常超过一定门槛（如30万股或200万元）
- 价格协商：买卖双方协商确定价格
- 场外交易：不在集中竞价系统进行
- 信息披露：交易后需要披露
- 影响市场：可能预示主力动向

交易门槛（满足其一即可）：
A股：
- 单笔交易数量 ≥ 30万股
- 单笔交易金额 ≥ 200万元

B股：
- 单笔交易数量 ≥ 30万股
- 单笔交易金额 ≥ 20万美元

基金：
- 单笔交易数量 ≥ 200万份
- 单笔交易金额 ≥ 200万元

债券：
- 单笔交易数量 ≥ 1000手
- 单笔交易金额 ≥ 100万元

价格类型：
1. 溢价交易：成交价 > 收盘价
   - 可能表示买方看好后市
   - 机构积极建仓信号

2. 折价交易：成交价 < 收盘价
   - 可能表示卖方急于套现
   - 常见于股东减持

3. 平价交易：成交价 = 收盘价
   - 正常市场行为

关键指标：
1. 成交总额：当日大宗交易总金额
2. 溢价成交总额：溢价交易的总金额
3. 折价成交总额：折价交易的总金额
4. 溢价/折价占比：反映市场情绪

使用场景：
- 追踪机构资金动向
- 发现大股东增减持
- 判断市场情绪
- 识别潜在投资机会
- 风险评估

数据来源：
东方财富网 - 数据中心 - 大宗交易
http://data.eastmoney.com/dzjy/dzjy_sctj.aspx

核心函数：
1. stock_dzjy_sctj() - 市场统计
2. stock_dzjy_mrmx() - 每日明细
3. stock_dzjy_ggtj() - 个股统计
4. stock_dzjy_yybph() - 营业部排名

技术特点：
1. 分页爬取：支持大数据量的分页获取
2. 随机延迟：避免爬取过快被封IP
3. 多类型支持：A股/B股/基金/债券
4. 日期范围筛选：支持自定义时间区间
5. 数据清洗：自动转换数据类型和格式

API参数说明：
- symbol（交易类型）：
  * "A股"：A股大宗交易
  * "B股"：B股大宗交易
  * "基金"：基金大宗交易
  * "债券"：债券大宗交易

- start_date/end_date：日期范围（格式：YYYYMMDD）

数据字段说明：

市场统计：
- 交易日期：统计日期
- 上证指数：当日收盘点位
- 上证指数涨跌幅：大盘表现
- 大宗交易成交总额：当日总成交额
- 溢价成交总额：溢价交易金额
- 溢价成交总额占比：溢价比例
- 折价成交总额：折价交易金额
- 折价成交总额占比：折价比例

每日明细：
- 交易日期：交易发生日期
- 代码：证券代码
- 名称：证券名称
- 涨跌幅：当日涨跌幅
- 收盘价：当日收盘价
- 成交价：大宗交易成交价格
- 折溢率：(成交价-收盘价)/收盘价×100%
- 成交量：交易数量
- 成交额：交易金额
- 买方营业部：买入方席位
- 卖方营业部：卖出方席位
- 后续表现：1日/5日/10日/20日涨跌幅

使用示例：
```python
# 获取市场统计数据
df_market = stock_dzjy_sctj()
print(f"共获取 {len(df_market)} 天的数据")

# 查看最近一天的情况
latest = df_market.iloc[0]
print(f"最新交易日: {latest['交易日期']}")
print(f"成交总额: {latest['大宗交易成交总额']/100000000:.2f}亿")
print(f"溢价占比: {latest['溢价成交总额占比']:.2f}%")
print(f"折价占比: {latest['折价成交总额占比']:.2f}%")

# 获取某日的A股大宗交易明细
df_detail = stock_dzjy_mrmx(
    symbol='A股',
    start_date='20240101',
    end_date='20240101'
)
print(f"当日共 {len(df_detail)} 笔交易")

# 筛选大额溢价交易（成交额>1亿且溢价）
big_premium = df_detail[
    (df_detail['成交额'] > 100000000) & 
    (df_detail['折溢率'] > 0)
]

# 按买方营业部统计
buyer_stats = df_detail.groupby('买方营业部')['成交额'].sum()
top_buyers = buyer_stats.nlargest(10)
```

实战应用：

1. 机构动向追踪：
```python
# 知名营业部的大额交易
famous_seats = ['机构专用', '沪股通专用', '深股通专用']
institution_trades = df_detail[
    df_detail['买方营业部'].isin(famous_seats)
]
```

2. 溢价交易分析：
```python
# 持续溢价可能表示看好
premium_stocks = df_detail[
    (df_detail['折溢率'] > 5) &  # 溢价>5%
    (df_detail['成交额'] > 50000000)  # 成交额>5000万
]
```

3. 后续表现跟踪：
```python
# 溢价交易后的表现
good_performance = df_detail[
    (df_detail['折溢率'] > 0) & 
    (df_detail['1日后涨跌幅'] > 0)
]
success_rate = len(good_performance) / len(df_detail) * 100
print(f"溢价交易成功率: {success_rate:.2f}%")
```

注意事项：
1. 大宗交易数据T+1日公布
2. 部分交易可能不披露详细信息
3. 需要结合其他指标综合判断
4. 溢价不一定涨，折价不一定跌
5. API接口可能变化，需要定期维护

性能优化：
1. 全局fetcher实例：复用连接池
2. 分页获取：每次500-5000条数据
3. 随机延迟：1-1.5秒避免封禁
4. 批量处理：减少API调用次数

常见问题：

Q: 为什么关注大宗交易？
A: 大宗交易反映机构和大股东的真实意图，是重要信号

Q: 溢价交易一定好吗？
A: 不一定。需要结合：
- 溢价幅度
- 成交量大小
- 买方身份
- 市场环境

Q: 如何识别假信号？
A: 
- 看持续性：单笔vs连续
- 看成交量：小额vs大额
- 看买方：机构vs游资
- 看位置：高位vs低位

Q: 大宗交易对股价影响大吗？
A: 
- 短期：可能有心理影响
- 长期：取决于基本面
- 大额溢价：通常利好
- 大额折价：通常利空

依赖关系：
- pandas：数据处理和DataFrame操作
- random：生成随机延迟时间
- time：时间控制（sleep延迟）
- instock.core.eastmoney_fetcher：HTTP请求封装
"""

import random
import time

import pandas as pd
from instock.core.eastmoney_fetcher import eastmoney_fetcher
from instock.config.delay_manager import sleep_with_delay

__author__ = 'myh '
__date__ = '2025/12/31 '

# ==================== 全局HTTP请求器 ====================
# 创建全局实例，供所有函数使用
# 这样可以复用连接池，提高性能
fetcher = eastmoney_fetcher()


# ==================== 大宗交易市场统计 ====================

"""
stock_dzjy_sctj - 获取大宗交易市场统计数据

功能：
从东方财富网获取每日大宗交易的市场总体统计

数据来源：
东方财富网 - 数据中心 - 大宗交易 - 市场统计
http://data.eastmoney.com/dzjy/dzjy_sctj.aspx

返回数据包含的字段：
- 序号：记录序号
- 交易日期：统计日期
- 上证指数：当日上证指数收盘价
- 上证指数涨跌幅：大盘当日表现
- 大宗交易成交总额：当日所有大宗交易总金额
- 溢价成交总额：溢价交易的总金额
- 溢价成交总额占比：溢价交易占总成交的比例
- 折价成交总额：折价交易的总金额
- 折价成交总额占比：折价交易占总成交的比例

执行流程：
1. 设置API参数（第1页，每页500条）
2. 发送HTTP请求获取第1页数据
3. 解析JSON，提取data数组
4. 计算总页数
5. 循环获取剩余页面（每次延迟1-1.5秒）
6. 合并所有页面的数据
7. 创建DataFrame并重命名列
8. 转换数据类型
9. 返回结果

API说明：
URL: https://datacenter-web.eastmoney.com/api/data/v1/get

关键参数：
- reportName: "PRT_BLOCKTRADE_MARKET_STA"（报表名称）
- columns: 返回字段列表
- sortColumns: TRADE_DATE（按日期排序）
- sortTypes: -1（降序，最新的在前）
- pageSize: 每页500条

返回值：
pandas.DataFrame，包含历史每日的市场统计数据

使用示例：
```python
# 获取市场统计数据
df = stock_dzjy_sctj()
print(f"共获取 {len(df)} 天的数据")

# 查看最近一天的情况
latest = df.iloc[0]
print(f"\n最新交易日: {latest['交易日期']}")
print(f"上证指数: {latest['上证指数']}")
print(f"大盘涨跌: {latest['上证指数涨跌幅']:.2f}%")
print(f"成交总额: {latest['大宗交易成交总额']/100000000:.2f}亿元")
print(f"溢价占比: {latest['溢价成交总额占比']:.2f}%")
print(f"折价占比: {latest['折价成交总额占比']:.2f}%")

# 分析市场情绪
# 溢价占比高：市场乐观
# 折价占比高：市场悲观
avg_premium = df['溢价成交总额占比'].mean()
avg_discount = df['折价成交总额占比'].mean()
print(f"\n平均溢价占比: {avg_premium:.2f}%")
print(f"平均折价占比: {avg_discount:.2f}%")

if avg_premium > avg_discount:
    print("市场整体偏向乐观")
else:
    print("市场整体偏向悲观")

# 找出成交最活跃的日子
top_active = df.nlargest(10, '大宗交易成交总额')
print("\n成交最活跃的10天:")
print(top_active[['交易日期', '大宗交易成交总额', '上证指数涨跌幅']])
```

实战应用：

1. 市场情绪指标：
```python
# 计算净溢价率（溢价-折价）
df['净溢价率'] = df['溢价成交总额占比'] - df['折价成交总额占比']

# 净溢价率为正：市场乐观
# 净溢价率为负：市场悲观
recent_sentiment = df.head(5)['净溢价率'].mean()
if recent_sentiment > 0:
    print("近期市场情绪乐观")
else:
    print("近期市场情绪悲观")
```

2. 与大盘相关性分析：
```python
# 大宗交易活跃度与大盘走势的关系
correlation = df['大宗交易成交总额'].corr(df['上证指数涨跌幅'])
print(f"成交总额与大盘涨跌相关系数: {correlation:.4f}")
```

3. 异常交易日识别：
```python
# 找出成交异常的日期（超过平均值2倍标准差）
mean_amt = df['大宗交易成交总额'].mean()
std_amt = df['大宗交易成交总额'].std()
threshold = mean_amt + 2 * std_amt

abnormal_days = df[df['大宗交易成交总额'] > threshold]
print(f"异常活跃交易日: {len(abnormal_days)} 天")
```

注意事项：
1. 数据是T+1日公布（次日晚上）
2. 只统计达到大宗交易标准的交易
3. 不包含协议转让等其他大额交易
4. 周末和节假日无数据
5. 早期数据可能不完整

解读技巧：

1. 溢价vs折价：
   - 溢价占比 > 折价占比：市场乐观
   - 溢价占比 < 折价占比：市场悲观
   - 两者接近：市场中性

2. 成交总额：
   - 突然放大：可能有重大事件
   - 持续低迷：市场交投清淡
   - 结合大盘走势分析

3. 趋势判断：
   - 连续多日溢价为主：牛市特征
   - 连续多日折价为主：熊市特征
   - 交替出现：震荡市特征
"""
def stock_dzjy_sctj() -> pd.DataFrame:
    """
    获取大宗交易市场统计数据
    
    返回：
    pd.DataFrame: 包含每日市场统计的DataFrame
    
    异常：
    网络错误、API异常时会抛出异常
    """
    
    # ==================== 步骤1: 构造API请求 ====================
    # 大宗交易市场统计API地址
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    
    # 构造请求参数
    params = {
        'sortColumns': 'TRADE_DATE',  # 按交易日期排序
        'sortTypes': '-1',            # 降序（最新的在前）
        'pageSize': '500',            # 每页500条
        'pageNumber': '1',            # 第1页
        'reportName': 'PRT_BLOCKTRADE_MARKET_STA',  # 报表名称
        # 返回字段列表：
        # TRADE_DATE: 交易日期
        # SZ_INDEX: 上证指数
        # SZ_CHANGE_RATE: 上证指数涨跌幅
        # BLOCKTRADE_DEAL_AMT: 大宗交易成交总额
        # PREMIUM_DEAL_AMT: 溢价成交总额
        # PREMIUM_RATIO: 溢价成交总额占比
        # DISCOUNT_DEAL_AMT: 折价成交总额
        # DISCOUNT_RATIO: 折价成交总额占比
        'columns': 'TRADE_DATE,SZ_INDEX,SZ_CHANGE_RATE,BLOCKTRADE_DEAL_AMT,PREMIUM_DEAL_AMT,PREMIUM_RATIO,DISCOUNT_DEAL_AMT,DISCOUNT_RATIO',
        'source': 'WEB',     # 数据来源
        'client': 'WEB',     # 客户端类型
    }
    
    # ==================== 步骤2: 获取第1页数据 ====================
    # 发送HTTP请求
    r = fetcher.make_request(url, params=params)
    
    # 解析JSON响应
    data_json = r.json()
    
    # 获取总页数
    total_page = int(data_json['result']["pages"])
    
    # 初始化结果DataFrame
    big_df = pd.DataFrame()
    
    # ==================== 步骤3: 循环获取所有页面 ====================
    for page in range(1, total_page+1):
        # 添加随机延迟，控制每分钟请求数<10次（间隔9-12秒）
        sleep_with_delay('normal')
        
        # 更新页码参数
        params.update({'pageNumber': page})
        
        # 发送HTTP请求
        r = fetcher.make_request(url, params=params)
        
        # 解析JSON响应
        data_json = r.json()
        
        # 提取当前页数据
        temp_df = pd.DataFrame(data_json['result']["data"])
        
        # 合并到总DataFrame
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    
    # ==================== 步骤4: 数据清洗和整理 ====================
    # 重置索引
    big_df.reset_index(inplace=True)
    
    # 索引从1开始
    big_df['index'] = big_df['index'] + 1
    
    # ==================== 步骤5: 重命名列 ====================
    big_df.columns = [
        "序号",
        "交易日期",
        "上证指数",
        "上证指数涨跌幅",
        "大宗交易成交总额",
        "溢价成交总额",
        "溢价成交总额占比",
        "折价成交总额",
        "折价成交总额占比",
    ]
    
    # ==================== 步骤6: 转换数据类型 ====================
    # 日期字段：转换为date类型
    big_df["交易日期"] = pd.to_datetime(big_df["交易日期"]).dt.date
    
    # 数值字段：转换为numeric类型
    big_df["上证指数"] = pd.to_numeric(big_df["上证指数"])
    big_df["上证指数涨跌幅"] = pd.to_numeric(big_df["上证指数涨跌幅"])
    big_df["大宗交易成交总额"] = pd.to_numeric(big_df["大宗交易成交总额"])
    big_df["溢价成交总额"] = pd.to_numeric(big_df["溢价成交总额"])
    big_df["溢价成交总额占比"] = pd.to_numeric(big_df["溢价成交总额占比"])
    big_df["折价成交总额"] = pd.to_numeric(big_df["折价成交总额"])
    big_df["折价成交总额占比"] = pd.to_numeric(big_df["折价成交总额占比"])
    
    # ==================== 步骤7: 返回结果 ====================
    return big_df


"""
东方财富网-数据中心-大宗交易-每日明细
http://data.eastmoney.com/dzjy/dzjy_mrmxa.aspx
:param symbol: choice of {'A股', 'B股', '基金', '债券'}
:type symbol: str
:param start_date: 开始日期
:type start_date: str
:param end_date: 结束日期
:type end_date: str
:return: 每日明细
:rtype: pandas.DataFrame
"""
def stock_dzjy_mrmx(symbol: str = '基金', start_date: str = '20220104', end_date: str = '20220104') -> pd.DataFrame:
    symbol_map = {
        'A股': '1',
        'B股': '2',
        '基金': '3',
        '债券': '4',
    }
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        'sortColumns': 'SECURITY_CODE',
        'sortTypes': '1',
        'pageSize': '5000',
        'pageNumber': '1',
        'reportName': 'RPT_DATA_BLOCKTRADE',
        'columns': 'TRADE_DATE,SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,CHANGE_RATE,CLOSE_PRICE,DEAL_PRICE,PREMIUM_RATIO,DEAL_VOLUME,DEAL_AMT,TURNOVER_RATE,BUYER_NAME,SELLER_NAME,CHANGE_RATE_1DAYS,CHANGE_RATE_5DAYS,CHANGE_RATE_10DAYS,CHANGE_RATE_20DAYS,BUYER_CODE,SELLER_CODE',
        'source': 'WEB',
        'client': 'WEB',
        'filter': f"""(SECURITY_TYPE_WEB={symbol_map[symbol]})(TRADE_DATE>='{'-'.join([start_date[:4], start_date[4:6], start_date[6:]])}')(TRADE_DATE<='{'-'.join([end_date[:4], end_date[4:6], end_date[6:]])}')"""
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    if not data_json['result']["data"]:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data_json['result']["data"])
    temp_df.reset_index(inplace=True)
    temp_df['index'] = temp_df.index + 1
    if symbol in {'A股'}:
        temp_df.columns = [
            "序号",
            "交易日期",
            "证券代码",
            "-",
            "证券简称",
            "涨跌幅",
            "收盘价",
            "成交价",
            "折溢率",
            "成交量",
            "成交额",
            "成交额/流通市值",
            "买方营业部",
            "卖方营业部",
            "_",
            "_",
            "_",
            "_",
            "_",
            "_",
        ]
        temp_df["交易日期"] = pd.to_datetime(temp_df["交易日期"]).dt.date
        temp_df = temp_df[[
            "序号",
            "交易日期",
            "证券代码",
            "证券简称",
            "涨跌幅",
            "收盘价",
            "成交价",
            "折溢率",
            "成交量",
            "成交额",
            "成交额/流通市值",
            "买方营业部",
            "卖方营业部",
        ]]
        temp_df['涨跌幅'] = pd.to_numeric(temp_df['涨跌幅'])
        temp_df['收盘价'] = pd.to_numeric(temp_df['收盘价'])
        temp_df['成交价'] = pd.to_numeric(temp_df['成交价'])
        temp_df['折溢率'] = pd.to_numeric(temp_df['折溢率'])
        temp_df['成交量'] = pd.to_numeric(temp_df['成交量'])
        temp_df['成交额'] = pd.to_numeric(temp_df['成交额'])
        temp_df['成交额/流通市值'] = pd.to_numeric(temp_df['成交额/流通市值'])
    if symbol in {'B股', '基金', '债券'}:
        temp_df.columns = [
            "序号",
            "交易日期",
            "证券代码",
            "-",
            "证券简称",
            "-",
            "-",
            "成交价",
            "-",
            "成交量",
            "成交额",
            "-",
            "买方营业部",
            "卖方营业部",
            "_",
            "_",
            "_",
            "_",
            "_",
            "_",
        ]
        temp_df["交易日期"] = pd.to_datetime(temp_df["交易日期"]).dt.date
        temp_df = temp_df[[
            "序号",
            "交易日期",
            "证券代码",
            "证券简称",
            "成交价",
            "成交量",
            "成交额",
            "买方营业部",
            "卖方营业部",
        ]]
        temp_df['成交价'] = pd.to_numeric(temp_df['成交价'])
        temp_df['成交量'] = pd.to_numeric(temp_df['成交量'])
        temp_df['成交额'] = pd.to_numeric(temp_df['成交额'])
    return temp_df


"""
东方财富网-数据中心-大宗交易-每日统计
http://data.eastmoney.com/dzjy/dzjy_mrtj.aspx
:param start_date: 开始日期
:type start_date: str
:param end_date: 结束日期
:type end_date: str
:return: 每日统计
:rtype: pandas.DataFrame
"""
def stock_dzjy_mrtj(start_date: str = '20220105', end_date: str = '20220105') -> pd.DataFrame:
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        'sortColumns': 'TURNOVERRATE',
        'sortTypes': '-1',
        'pageSize': '5000',
        'pageNumber': '1',
        'reportName': 'RPT_BLOCKTRADE_STA',
        'columns': 'TRADE_DATE,SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,CHANGE_RATE,CLOSE_PRICE,AVERAGE_PRICE,PREMIUM_RATIO,DEAL_NUM,VOLUME,DEAL_AMT,TURNOVERRATE,D1_CLOSE_ADJCHRATE,D5_CLOSE_ADJCHRATE,D10_CLOSE_ADJCHRATE,D20_CLOSE_ADJCHRATE',
        'source': 'WEB',
        'client': 'WEB',
        'filter': f"(TRADE_DATE>='{'-'.join([start_date[:4], start_date[4:6], start_date[6:]])}')(TRADE_DATE<='{'-'.join([end_date[:4], end_date[4:6], end_date[6:]])}')"
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json['result']["data"])
    temp_df.reset_index(inplace=True)
    temp_df['index'] = temp_df.index + 1
    temp_df.columns = [
        "序号",
        "交易日期",
        "证券代码",
        "-",
        "证券简称",
        "涨跌幅",
        "收盘价",
        "成交价",
        "折溢率",
        "成交笔数",
        "成交总量",
        "成交总额",
        "成交总额/流通市值",
        "_",
        "_",
        "_",
        "_",
    ]
    temp_df["交易日期"] = pd.to_datetime(temp_df["交易日期"]).dt.date
    temp_df = temp_df[[
        "序号",
        "交易日期",
        "证券代码",
        "证券简称",
        "收盘价",
        "涨跌幅",
        "成交价",
        "折溢率",
        "成交笔数",
        "成交总量",
        "成交总额",
        "成交总额/流通市值",
    ]]
    temp_df['涨跌幅'] = pd.to_numeric(temp_df['涨跌幅'])
    temp_df['收盘价'] = pd.to_numeric(temp_df['收盘价'])
    temp_df['成交价'] = pd.to_numeric(temp_df['成交价'])
    temp_df['折溢率'] = pd.to_numeric(temp_df['折溢率'])
    temp_df['成交笔数'] = pd.to_numeric(temp_df['成交笔数'])
    temp_df['成交总量'] = pd.to_numeric(temp_df['成交总量'])
    temp_df['成交总额'] = pd.to_numeric(temp_df['成交总额'])
    temp_df['成交总额/流通市值'] = pd.to_numeric(temp_df['成交总额/流通市值'])
    return temp_df


"""
东方财富网-数据中心-大宗交易-活跃 A 股统计
http://data.eastmoney.com/dzjy/dzjy_hygtj.aspx
:param symbol: choice of {'近一月', '近三月', '近六月', '近一年'}
:type symbol: str
:return: 活跃 A 股统计
:rtype: pandas.DataFrame
"""
def stock_dzjy_hygtj(symbol: str = '近三月') -> pd.DataFrame:
    period_map = {
        '近一月': '1',
        '近三月': '3',
        '近六月': '6',
        '近一年': '12',
    }
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        'sortColumns': 'DEAL_NUM,SECURITY_CODE',
        'sortTypes': '-1,-1',
        'pageSize': '5000',
        'pageNumber': '1',
        'reportName': 'RPT_BLOCKTRADE_ACSTA',
        'columns': 'SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,CLOSE_PRICE,CHANGE_RATE,TRADE_DATE,DEAL_AMT,PREMIUM_RATIO,SUM_TURNOVERRATE,DEAL_NUM,PREMIUM_TIMES,DISCOUNT_TIMES,D1_AVG_ADJCHRATE,D5_AVG_ADJCHRATE,D10_AVG_ADJCHRATE,D20_AVG_ADJCHRATE,DATE_TYPE_CODE',
        'source': 'WEB',
        'client': 'WEB',
        'filter': f'(DATE_TYPE_CODE={period_map[symbol]})',
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    total_page = data_json['result']["pages"]
    big_df = pd.DataFrame()
    for page in range(1, int(total_page)+1):
        # 添加随机延迟，控制每分钟请求数<10次（间隔9-12秒）
        sleep_with_delay('normal')
        params.update({"pageNumber": page})
        r = fetcher.make_request(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json['result']["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)

    big_df.reset_index(inplace=True)
    big_df['index'] = big_df.index + 1
    big_df.columns = [
        "序号",
        "证券代码",
        "_",
        "证券简称",
        "最新价",
        "涨跌幅",
        "最近上榜日",
        "总成交额",
        "折溢率",
        "成交总额/流通市值",
        "上榜次数-总计",
        "上榜次数-溢价",
        "上榜次数-折价",
        "上榜日后平均涨跌幅-1日",
        "上榜日后平均涨跌幅-5日",
        "上榜日后平均涨跌幅-10日",
        "上榜日后平均涨跌幅-20日",
        "_",
    ]
    big_df = big_df[[
        "序号",
        "证券代码",
        "证券简称",
        "最新价",
        "涨跌幅",
        "最近上榜日",
        "上榜次数-总计",
        "上榜次数-溢价",
        "上榜次数-折价",
        "总成交额",
        "折溢率",
        "成交总额/流通市值",
        "上榜日后平均涨跌幅-1日",
        "上榜日后平均涨跌幅-5日",
        "上榜日后平均涨跌幅-10日",
        "上榜日后平均涨跌幅-20日",
    ]]
    big_df["最近上榜日"] = pd.to_datetime(big_df["最近上榜日"]).dt.date
    big_df["最新价"] = pd.to_numeric(big_df["最新价"])
    big_df["涨跌幅"] = pd.to_numeric(big_df["涨跌幅"])
    big_df["上榜次数-总计"] = pd.to_numeric(big_df["上榜次数-总计"])
    big_df["上榜次数-溢价"] = pd.to_numeric(big_df["上榜次数-溢价"])
    big_df["上榜次数-折价"] = pd.to_numeric(big_df["上榜次数-折价"])
    big_df["总成交额"] = pd.to_numeric(big_df["总成交额"])
    big_df["折溢率"] = pd.to_numeric(big_df["折溢率"])
    big_df["成交总额/流通市值"] = pd.to_numeric(big_df["成交总额/流通市值"])
    big_df["上榜日后平均涨跌幅-1日"] = pd.to_numeric(big_df["上榜日后平均涨跌幅-1日"])
    big_df["上榜日后平均涨跌幅-5日"] = pd.to_numeric(big_df["上榜日后平均涨跌幅-5日"])
    big_df["上榜日后平均涨跌幅-10日"] = pd.to_numeric(big_df["上榜日后平均涨跌幅-10日"])
    big_df["上榜日后平均涨跌幅-20日"] = pd.to_numeric(big_df["上榜日后平均涨跌幅-20日"])
    return big_df


"""
东方财富网-数据中心-大宗交易-活跃营业部统计
https://data.eastmoney.com/dzjy/dzjy_hyyybtj.html
:param symbol: choice of {'当前交易日', '近3日', '近5日', '近10日', '近30日'}
:type symbol: str
:return: 活跃营业部统计
:rtype: pandas.DataFrame
"""
def stock_dzjy_hyyybtj(symbol: str = '近3日') -> pd.DataFrame:
    period_map = {
        '当前交易日': '1',
        '近3日': '3',
        '近5日': '5',
        '近10日': '10',
        '近30日': '30',
    }
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        'sortColumns': 'BUYER_NUM,TOTAL_BUYAMT',
        'sortTypes': '-1,-1',
        'pageSize': '5000',
        'pageNumber': '1',
        'reportName': 'RPT_BLOCKTRADE_OPERATEDEPTSTATISTICS',
        'columns': 'OPERATEDEPT_CODE,OPERATEDEPT_NAME,ONLIST_DATE,STOCK_DETAILS,BUYER_NUM,SELLER_NUM,TOTAL_BUYAMT,TOTAL_SELLAMT,TOTAL_NETAMT,N_DATE',
        'source': 'WEB',
        'client': 'WEB',
        'filter': f'(N_DATE=-{period_map[symbol]})',
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    total_page = data_json['result']["pages"]
    big_df = pd.DataFrame()
    for page in range(1, int(total_page)+1):
        # 添加随机延迟，控制每分钟请求数<10次（间隔9-12秒）
        sleep_with_delay('normal')
        params.update({"pageNumber": page})
        r = fetcher.make_request(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json['result']["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)

    big_df.reset_index(inplace=True)
    big_df['index'] = big_df.index + 1
    big_df.columns = [
        "序号",
        "_",
        "营业部名称",
        "最近上榜日",
        "买入的股票",
        "次数总计-买入",
        "次数总计-卖出",
        "成交金额统计-买入",
        "成交金额统计-卖出",
        "成交金额统计-净买入额",
        "_",
    ]
    big_df = big_df[[
        "序号",
        "最近上榜日",
        "营业部名称",
        "次数总计-买入",
        "次数总计-卖出",
        "成交金额统计-买入",
        "成交金额统计-卖出",
        "成交金额统计-净买入额",
        "买入的股票",
    ]]
    big_df["最近上榜日"] = pd.to_datetime(big_df["最近上榜日"]).dt.date
    big_df["次数总计-买入"] = pd.to_numeric(big_df["次数总计-买入"])
    big_df["次数总计-卖出"] = pd.to_numeric(big_df["次数总计-卖出"])
    big_df["成交金额统计-买入"] = pd.to_numeric(big_df["成交金额统计-买入"])
    big_df["成交金额统计-卖出"] = pd.to_numeric(big_df["成交金额统计-卖出"])
    big_df["成交金额统计-净买入额"] = pd.to_numeric(big_df["成交金额统计-净买入额"])
    return big_df


"""
东方财富网-数据中心-大宗交易-营业部排行
http://data.eastmoney.com/dzjy/dzjy_yybph.aspx
:param symbol: choice of {'近一月', '近三月', '近六月', '近一年'}
:type symbol: str
:return: 营业部排行
:rtype: pandas.DataFrame
"""
def stock_dzjy_yybph(symbol: str = '近三月') -> pd.DataFrame:
    period_map = {
        '近一月': '30',
        '近三月': '90',
        '近六月': '120',
        '近一年': '360',
    }
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        'sortColumns': 'D5_BUYER_NUM,D1_AVERAGE_INCREASE',
        'sortTypes': '-1,-1',
        'pageSize': '5000',
        'pageNumber': '1',
        'reportName': 'RPT_BLOCKTRADE_OPERATEDEPT_RANK',
        'columns': 'OPERATEDEPT_CODE,OPERATEDEPT_NAME,D1_BUYER_NUM,D1_AVERAGE_INCREASE,D1_RISE_PROBABILITY,D5_BUYER_NUM,D5_AVERAGE_INCREASE,D5_RISE_PROBABILITY,D10_BUYER_NUM,D10_AVERAGE_INCREASE,D10_RISE_PROBABILITY,D20_BUYER_NUM,D20_AVERAGE_INCREASE,D20_RISE_PROBABILITY,N_DATE,RELATED_ORG_CODE',
        'source': 'WEB',
        'client': 'WEB',
        'filter': f'(N_DATE=-{period_map[symbol]})',
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    total_page = data_json['result']["pages"]
    big_df = pd.DataFrame()
    for page in range(1, int(total_page)+1):
        # 添加随机延迟，控制每分钟请求数<10次（间隔9-12秒）
        sleep_with_delay('normal')
        params.update({"pageNumber": page})
        r = fetcher.make_request(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json['result']["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)

    big_df.reset_index(inplace=True)
    big_df['index'] = big_df.index + 1
    big_df.columns = [
        "序号",
        "_",
        "营业部名称",
        "上榜后1天-买入次数",
        "上榜后1天-平均涨幅",
        "上榜后1天-上涨概率",
        "上榜后5天-买入次数",
        "上榜后5天-平均涨幅",
        "上榜后5天-上涨概率",
        "上榜后10天-买入次数",
        "上榜后10天-平均涨幅",
        "上榜后10天-上涨概率",
        "上榜后20天-买入次数",
        "上榜后20天-平均涨幅",
        "上榜后20天-上涨概率",
        "_",
        "_",
    ]
    big_df = big_df[[
        "序号",
        "营业部名称",
        "上榜后1天-买入次数",
        "上榜后1天-平均涨幅",
        "上榜后1天-上涨概率",
        "上榜后5天-买入次数",
        "上榜后5天-平均涨幅",
        "上榜后5天-上涨概率",
        "上榜后10天-买入次数",
        "上榜后10天-平均涨幅",
        "上榜后10天-上涨概率",
        "上榜后20天-买入次数",
        "上榜后20天-平均涨幅",
        "上榜后20天-上涨概率",
    ]]
    big_df['上榜后1天-买入次数'] = pd.to_numeric(big_df['上榜后1天-买入次数'])
    big_df['上榜后1天-平均涨幅'] = pd.to_numeric(big_df['上榜后1天-平均涨幅'])
    big_df['上榜后1天-上涨概率'] = pd.to_numeric(big_df['上榜后1天-上涨概率'])
    big_df['上榜后5天-买入次数'] = pd.to_numeric(big_df['上榜后5天-买入次数'])
    big_df['上榜后5天-平均涨幅'] = pd.to_numeric(big_df['上榜后5天-平均涨幅'])
    big_df['上榜后5天-上涨概率'] = pd.to_numeric(big_df['上榜后5天-上涨概率'])
    big_df['上榜后10天-买入次数'] = pd.to_numeric(big_df['上榜后10天-买入次数'])
    big_df['上榜后10天-平均涨幅'] = pd.to_numeric(big_df['上榜后10天-平均涨幅'])
    big_df['上榜后10天-上涨概率'] = pd.to_numeric(big_df['上榜后10天-上涨概率'])
    big_df['上榜后20天-买入次数'] = pd.to_numeric(big_df['上榜后20天-买入次数'])
    big_df['上榜后20天-平均涨幅'] = pd.to_numeric(big_df['上榜后20天-平均涨幅'])
    big_df['上榜后20天-上涨概率'] = pd.to_numeric(big_df['上榜后20天-上涨概率'])
    return big_df


if __name__ == "__main__":
    stock_dzjy_sctj_df = stock_dzjy_sctj()
    print(stock_dzjy_sctj_df)

    stock_dzjy_mrmx_df = stock_dzjy_mrmx(symbol='债券', start_date='20201204', end_date='20201204')
    print(stock_dzjy_mrmx_df)

    stock_dzjy_mrtj_df = stock_dzjy_mrtj(start_date='20201204', end_date='20201204')
    print(stock_dzjy_mrtj_df)

    stock_dzjy_hygtj_df = stock_dzjy_hygtj(symbol='近三月')
    print(stock_dzjy_hygtj_df)

    stock_dzjy_hyyybtj_df = stock_dzjy_hyyybtj(symbol='近3日')
    print(stock_dzjy_hyyybtj_df)

    stock_dzjy_yybph_df = stock_dzjy_yybph(symbol='近三月')
    print(stock_dzjy_yybph_df)
