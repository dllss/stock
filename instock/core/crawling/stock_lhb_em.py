# -*- coding:utf-8 -*-
# !/usr/bin/env python
"""
东方财富网龙虎榜数据爬虫模块 - 龙虎榜数据分析
==============================================

功能说明：
本模块提供从东方财富网抓取股票龙虎榜数据的功能，包括：
1. 龙虎榜详情（每日上榜股票明细）
2. 个股上榜统计（近一月/三月/六月/一年）
3. 营业部上榜统计
4. 营业部成交排行
5. 机构席位追踪
6. 游资席位分析

核心概念：
龙虎榜是交易所公布的当日异动股票交易信息，反映主力资金的动向。

上榜条件（满足其一即可）：
1. 日价格振幅达到15%
2. 日换手率达到20%
3. 日价格涨幅偏离值达到7%
4. 日价格跌幅偏离值达到7%
5. 无涨跌幅限制的股票
6. 连续三个交易日内涨跌幅偏离值累计达到20%

关键指标：
1. 龙虎榜净买额 = 买入额 - 卖出额
   - 正值：净买入（看多）
   - 负值：净卖出（看空）

2. 净买额占总成交比 = 龙虎榜净买额 / 市场总成交额
   - 反映龙虎榜资金的影响力

3. 成交额占总成交比 = 龙虎榜成交额 / 市场总成交额
   - 反映龙虎榜交易的活跃度

4. 上榜后表现：
   - D1/D2/D5/D10：上榜后1/2/5/10日的涨跌幅
   - 用于评估上榜后的短期走势

席位类型：
- 机构席位：基金、保险、社保等机构投资者
- 游资席位：知名营业部（如银河绍兴、国泰君安上海江苏路等）
- 沪股通/深股通：北向资金
- 量化基金：程序化交易席位

使用场景：
- 追踪主力资金动向
- 发现热门题材和龙头股
- 分析游资操作风格
- 判断短期走势
- 识别机构建仓行为

数据来源：
东方财富网 - 数据中心 - 龙虎榜单
https://data.eastmoney.com/stock/tradedetail.html

核心函数：
1. stock_lhb_detail_em() - 龙虎榜详情
2. stock_lhb_stock_statistic_em() - 个股上榜统计
3. stock_lhb_seat_statistic_em() - 营业部上榜统计
4. stock_lhb_seat_deal_rank_em() - 营业部成交排行
5. stock_lhb_jgstatistic_em() - 机构席位追踪
6. stock_lhb_yybph_em() - 营业部排名

技术特点：
1. 分页爬取：支持大数据量的分页获取
2. 随机延迟：避免爬取过快被封IP
3. 日期范围筛选：支持自定义时间区间
4. 多维度统计：个股、营业部、机构等
5. 数据清洗：自动转换数据类型和格式

API参数说明：
- start_date/end_date：日期范围（格式：YYYYMMDD）
- symbol：统计周期
  * "近一月"：最近一个月
  * "近三月"：最近三个月
  * "近六月"：最近六个月
  * "近一年"：最近一年

数据字段说明：
基础信息：
- 代码：股票代码
- 名称：股票名称
- 上榜日：登上龙虎榜的日期
- 上榜原因：为何上榜（振幅、换手率等）

价格信息：
- 收盘价：当日收盘价
- 涨跌幅：当日涨跌幅百分比

资金流向：
- 龙虎榜净买额：买入额-卖出额
- 龙虎榜买入额：买方总金额
- 龙虎榜卖出额：卖方总金额
- 龙虎榜成交额：买卖总额
- 市场总成交额：全市场成交金额
- 净买额占总成交比：影响力指标
- 成交额占总成交比：活跃度指标

其他指标：
- 换手率：当日换手率
- 流通市值：流通股本×股价
- 解读：专业分析师点评
- 上榜后N日：后续表现

使用示例：
```python
# 获取最近一周的龙虎榜详情
df_detail = stock_lhb_detail_em(
    start_date="20240101",
    end_date="20240107"
)
print(f"共获取 {len(df_detail)} 条记录")

# 筛选机构大额买入的股票
institution_buy = df_detail[
    (df_detail['龙虎榜净买额'] > 100000000) &  # 净买额>1亿
    (df_detail['上榜原因'].str.contains('机构', na=False))
]

# 获取近一月个股上榜统计
df_stat = stock_lhb_stock_statistic_em(symbol="近一月")
print("上榜次数最多的股票:")
print(df_stat.nlargest(10, '上榜次数'))

# 分析营业部活跃度
df_seat = stock_lhb_seat_statistic_em(symbol="近一月")
print("最活跃营业部:")
print(df_seat.nlargest(10, '上榜次数'))
```

实战应用：

1. 龙头股识别：
```python
# 频繁上榜且涨幅大的股票
dragon_stocks = df_stat[
    (df_stat['上榜次数'] >= 3) & 
    (df_stat['平均涨跌幅'] > 10)
]
```

2. 机构动向追踪：
```python
# 机构净买入的股票
institution_inflow = df_detail[
    df_detail['解读'].str.contains('机构买入', na=False)
].nlargest(10, '龙虎榜净买额')
```

3. 游资风格分析：
```python
# 某营业部的操作记录
yyb_trades = df_detail[
    df_detail['解读'].str.contains('华泰证券深圳益田路', na=False)
]
```

注意事项：
1. 龙虎榜数据T+1日公布（次日晚上）
2. 只显示前5名买入和前5名卖出席位
3. 部分席位可能隐藏真实身份
4. 需要结合其他指标综合判断
5. API接口可能变化，需要定期维护

性能优化：
1. 全局fetcher实例：复用连接池
2. 分页获取：每次5000条数据
3. 随机延迟：1-1.5秒避免封禁
4. 批量处理：减少API调用次数

常见问题：

Q: 为什么关注龙虎榜？
A: 龙虎榜揭示主力资金动向，是短线交易的重要参考

Q: 机构席位和游资席位有什么区别？
A: 
- 机构：中长期投资，稳健操作
- 游资：短线炒作，快进快出

Q: 如何判断是真突破还是假突破？
A: 
- 真突破：机构主导 + 持续放量 + 基本面支撑
- 假突破：游资炒作 + 快速拉升 + 缺乏基本面

Q: 龙虎榜数据滞后吗？
A: 是的，T+1日公布，但仍是重要参考

依赖关系：
- pandas：数据处理和DataFrame操作
- random：生成随机延迟时间
- time：时间控制（sleep延迟）
- tqdm：进度条显示
- instock.core.eastmoney_fetcher：HTTP请求封装
"""

import random
import time

import pandas as pd
from tqdm import tqdm
from instock.core.eastmoney_fetcher import eastmoney_fetcher
from instock.config.delay_manager import sleep_with_delay

__author__ = 'myh '
__date__ = '2025/12/31 '

# ==================== 全局HTTP请求器 ====================
# 创建全局实例，供所有函数使用
# 这样可以复用连接池，提高性能
fetcher = eastmoney_fetcher()


# ==================== 龙虎榜详情 ====================

def stock_lhb_detail_em(
    start_date: str = "20230403", end_date: str = "20230417"
) -> pd.DataFrame:
    """
    获取龙虎榜详情数据
    
    功能：
    从东方财富网获取指定日期范围内的龙虎榜详细信息
    
    参数：
    start_date (str): 开始日期
        - 格式：YYYYMMDD
        - 例如："20230403"
    
    end_date (str): 结束日期
        - 格式：YYYYMMDD
        - 例如："20230417"
    
    返回：
    pd.DataFrame: 包含龙虎榜详情的DataFrame
    
    列说明：
    - 代码：股票代码
    - 名称：股票名称
    - 上榜日：登上龙虎榜的日期
    - 解读：专业分析师点评
    - 收盘价：当日收盘价
    - 涨跌幅：当日涨跌幅（%）
    - 龙虎榜净买额：买入额-卖出额（元）
    - 龙虎榜买入额：买方总金额（元）
    - 龙虎榜卖出额：卖方总金额（元）
    - 龙虎榜成交额：买卖总额（元）
    - 市场总成交额：全市场成交金额（元）
    - 净买额占总成交比：净买额/总成交（%）
    - 成交额占总成交比：龙虎榜成交额/总成交（%）
    - 换手率：当日换手率（%）
    - 流通市值：流通股本×股价（元）
    - 上榜原因：为何上榜（振幅/换手率/偏离值等）
    - 上榜后1日：上榜后第1日涨跌幅（%）
    - 上榜后2日：上榜后第2日涨跌幅（%）
    - 上榜后5日：上榜后第5日涨跌幅（%）
    - 上榜后10日：上榜后第10日涨跌幅（%）
    
    执行流程：
    1. 转换日期格式（YYYYMMDD → YYYY-MM-DD）
    2. 构造API请求参数
    3. 获取第1页数据
    4. 计算总页数
    5. 循环获取剩余页面（每次延迟1-1.5秒）
    6. 合并所有页面数据
    7. 重命名列（英文→中文）
    8. 选择并排序需要的列
    9. 转换数据类型
    10. 返回结果
    
    API说明：
    URL: https://datacenter-web.eastmoney.com/api/data/v1/get
    
    关键参数：
    - reportName: "RPT_DAILYBILLBOARD_DETAILSNEW"（报表名称）
    - columns: 返回字段列表（SECURITY_CODE, TRADE_DATE等）
    - filter: 日期筛选条件
    - sortColumns: 排序字段（代码升序，日期降序）
    - pageSize: 每页5000条
    
    使用示例：
    ```python
    # 获取一周的龙虎榜数据
    df = stock_lhb_detail_em(
        start_date="20240101",
        end_date="20240107"
    )
    print(f"共获取 {len(df)} 条记录")
    
    # 筛选机构大额买入
    institution_buy = df[
        (df['龙虎榜净买额'] > 100000000) &
        (df['上榜原因'].str.contains('机构', na=False))
    ]
    
    # 查看上榜后表现最好的股票
    top_performers = df.nlargest(10, '上榜后5日')
    ```
    
    注意事项：
    1. 日期范围不要太大，避免数据量过大
    2. 数据是T+1日公布（次日晚上）
    3. 只包含满足上榜条件的股票
    4. 部分字段可能为空（如解读）
    """
    
    # ==================== 步骤1: 转换日期格式 ====================
    # API要求日期格式为YYYY-MM-DD，而输入是YYYYMMDD
    # 使用字符串切片和join进行转换
    # 例如："20230403" → "2023-04-03"
    start_date = "-".join([start_date[:4], start_date[4:6], start_date[6:]])
    end_date = "-".join([end_date[:4], end_date[4:6], end_date[6:]])
    
    # ==================== 步骤2: 构造API请求 ====================
    # 龙虎榜详情API地址
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    
    # 构造请求参数
    params = {
        # 排序字段：SECURITY_CODE（代码）升序，TRADE_DATE（日期）降序
        "sortColumns": "SECURITY_CODE,TRADE_DATE",
        "sortTypes": "1,-1",  # 1=升序，-1=降序
        "pageSize": "5000",   # 每页5000条
        "pageNumber": "1",    # 第1页
        # 报表名称：每日龙虎榜详情（新版）
        "reportName": "RPT_DAILYBILLBOARD_DETAILSNEW",
        # 返回字段列表（逗号分隔）
        # SECURITY_CODE: 股票代码
        # SECURITY_NAME_ABBR: 股票简称
        # TRADE_DATE: 交易日期
        # CLOSE_PRICE: 收盘价
        # CHANGE_RATE: 涨跌幅
        # BILLBOARD_NET_AMT: 龙虎榜净买额
        # BILLBOARD_BUY_AMT: 龙虎榜买入额
        # BILLBOARD_SELL_AMT: 龙虎榜卖出额
        # EXPLANATION: 上榜原因
        # D1_CLOSE_ADJCHRATE: 上榜后1日涨跌幅
        # 等等...
        "columns": "SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,TRADE_DATE,EXPLAIN,CLOSE_PRICE,CHANGE_RATE,BILLBOARD_NET_AMT,BILLBOARD_BUY_AMT,BILLBOARD_SELL_AMT,BILLBOARD_DEAL_AMT,ACCUM_AMOUNT,DEAL_NET_RATIO,DEAL_AMOUNT_RATIO,TURNOVERRATE,FREE_MARKET_CAP,EXPLANATION,D1_CLOSE_ADJCHRATE,D2_CLOSE_ADJCHRATE,D5_CLOSE_ADJCHRATE,D10_CLOSE_ADJCHRATE,SECURITY_TYPE_CODE",
        "source": "WEB",     # 数据来源
        "client": "WEB",     # 客户端类型
        # 日期筛选条件
        # 语法：(字段名<='结束日期')(字段名>='开始日期')
        "filter": f"(TRADE_DATE<='{end_date}')(TRADE_DATE>='{start_date}')",
    }
    
    # ==================== 步骤3: 获取第1页数据 ====================
    # 发送HTTP请求
    r = fetcher.make_request(url, params=params)
    
    # 解析JSON响应
    data_json = r.json()
    
    # 获取总页数
    total_page_num = data_json["result"]["pages"]
    
    # 初始化结果DataFrame
    big_df = pd.DataFrame()
    
    # ==================== 步骤4: 循环获取所有页面 ====================
    # 使用tqdm显示进度条
    for page in range(1, total_page_num + 1):
        # 添加随机延迟，控制每分钟请求数<10次（间隔9-12秒）
        sleep_with_delay('normal')
        
        # 更新页码参数
        params.update({
            "pageNumber": page,
        })
        
        # 发送HTTP请求
        r = fetcher.make_request(url, params=params)
        
        # 解析JSON响应
        data_json = r.json()
        
        # 提取当前页数据
        temp_df = pd.DataFrame(data_json["result"]["data"])
        
        # 合并到总DataFrame
        # ignore_index=True：重置索引
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    
    # ==================== 步骤5: 数据清洗和整理 ====================
    # 重置索引
    big_df.reset_index(inplace=True)
    
    # 索引从1开始（更符合阅读习惯）
    big_df["index"] = big_df.index + 1
    
    # ==================== 步骤6: 重命名列 ====================
    # 将英文字段名转换为中文
    # 使用rename方法，inplace=True表示原地修改
    big_df.rename(
        columns={
            "index": "-",  # 占位符（不需要的列）
            "SECURITY_CODE": "代码",
            "SECUCODE": "-",
            "SECURITY_NAME_ABBR": "名称",
            "TRADE_DATE": "上榜日",
            "EXPLAIN": "解读",
            "CLOSE_PRICE": "收盘价",
            "CHANGE_RATE": "涨跌幅",
            "BILLBOARD_NET_AMT": "龙虎榜净买额",
            "BILLBOARD_BUY_AMT": "龙虎榜买入额",
            "BILLBOARD_SELL_AMT": "龙虎榜卖出额",
            "BILLBOARD_DEAL_AMT": "龙虎榜成交额",
            "ACCUM_AMOUNT": "市场总成交额",
            "DEAL_NET_RATIO": "净买额占总成交比",
            "DEAL_AMOUNT_RATIO": "成交额占总成交比",
            "TURNOVERRATE": "换手率",
            "FREE_MARKET_CAP": "流通市值",
            "EXPLANATION": "上榜原因",
            "D1_CLOSE_ADJCHRATE": "上榜后1日",
            "D2_CLOSE_ADJCHRATE": "上榜后2日",
            "D5_CLOSE_ADJCHRATE": "上榜后5日",
            "D10_CLOSE_ADJCHRATE": "上榜后10日",
        },
        inplace=True,
    )
    
    # ==================== 步骤7: 选择并排序列 ====================
    # 按照逻辑顺序排列列
    big_df = big_df[
        [
            "代码",
            "名称",
            "上榜日",
            "解读",
            "收盘价",
            "涨跌幅",
            "龙虎榜净买额",
            "龙虎榜买入额",
            "龙虎榜卖出额",
            "龙虎榜成交额",
            "市场总成交额",
            "净买额占总成交比",
            "成交额占总成交比",
            "换手率",
            "流通市值",
            "上榜原因",
            "上榜后1日",
            "上榜后2日",
            "上榜后5日",
            "上榜后10日",
        ]
    ]
    
    # ==================== 步骤8: 转换数据类型 ====================
    # 日期字段：转换为date类型（只保留日期，去掉时间）
    big_df["上榜日"] = pd.to_datetime(big_df["上榜日"]).dt.date
    
    # 数值字段：转换为numeric类型
    # errors="coerce"：转换失败时设为NaN
    big_df["收盘价"] = pd.to_numeric(big_df["收盘价"], errors="coerce")
    big_df["涨跌幅"] = pd.to_numeric(big_df["涨跌幅"], errors="coerce")
    big_df["龙虎榜净买额"] = pd.to_numeric(big_df["龙虎榜净买额"], errors="coerce")
    big_df["龙虎榜买入额"] = pd.to_numeric(big_df["龙虎榜买入额"], errors="coerce")
    big_df["龙虎榜卖出额"] = pd.to_numeric(big_df["龙虎榜卖出额"], errors="coerce")
    big_df["龙虎榜成交额"] = pd.to_numeric(big_df["龙虎榜成交额"], errors="coerce")
    big_df["市场总成交额"] = pd.to_numeric(big_df["市场总成交额"], errors="coerce")
    big_df["净买额占总成交比"] = pd.to_numeric(big_df["净买额占总成交比"], errors="coerce")
    big_df["成交额占总成交比"] = pd.to_numeric(big_df["成交额占总成交比"], errors="coerce")
    big_df["换手率"] = pd.to_numeric(big_df["换手率"], errors="coerce")
    big_df["流通市值"] = pd.to_numeric(big_df["流通市值"], errors="coerce")
    big_df["上榜后1日"] = pd.to_numeric(big_df["上榜后1日"], errors="coerce")
    big_df["上榜后2日"] = pd.to_numeric(big_df["上榜后2日"], errors="coerce")
    big_df["上榜后5日"] = pd.to_numeric(big_df["上榜后5日"], errors="coerce")
    big_df["上榜后10日"] = pd.to_numeric(big_df["上榜后10日"], errors="coerce")
    
    # ==================== 步骤9: 返回结果 ====================
    return big_df


"""
东方财富网-数据中心-龙虎榜单-个股上榜统计
https://data.eastmoney.com/stock/tradedetail.html
:param symbol: choice of {"近一月", "近三月", "近六月", "近一年"}
:type symbol: str
:return: 个股上榜统计
:rtype: pandas.DataFrame
"""
def stock_lhb_stock_statistic_em(symbol: str = "近一月") -> pd.DataFrame:
    symbol_map = {
        "近一月": "01",
        "近三月": "02",
        "近六月": "03",
        "近一年": "04",
    }
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "BILLBOARD_TIMES,LATEST_TDATE,SECURITY_CODE",
        "sortTypes": "-1,-1,1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_BILLBOARD_TRADEALL",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f'(STATISTICS_CYCLE="{symbol_map[symbol]}")',
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    temp_df.reset_index(inplace=True)
    temp_df["index"] = temp_df.index + 1
    temp_df.columns = [
        "序号",
        "-",
        "代码",
        "最近上榜日",
        "名称",
        "近1个月涨跌幅",
        "近3个月涨跌幅",
        "近6个月涨跌幅",
        "近1年涨跌幅",
        "涨跌幅",
        "收盘价",
        "-",
        "龙虎榜总成交额",
        "龙虎榜净买额",
        "-",
        "-",
        "机构买入净额",
        "上榜次数",
        "龙虎榜买入额",
        "龙虎榜卖出额",
        "机构买入总额",
        "机构卖出总额",
        "买方机构次数",
        "卖方机构次数",
        "-",
    ]
    temp_df = temp_df[
        [
            "序号",
            "代码",
            "名称",
            "最近上榜日",
            "收盘价",
            "涨跌幅",
            "上榜次数",
            "龙虎榜净买额",
            "龙虎榜买入额",
            "龙虎榜卖出额",
            "龙虎榜总成交额",
            "买方机构次数",
            "卖方机构次数",
            "机构买入净额",
            "机构买入总额",
            "机构卖出总额",
            "近1个月涨跌幅",
            "近3个月涨跌幅",
            "近6个月涨跌幅",
            "近1年涨跌幅",
        ]
    ]
    temp_df["最近上榜日"] = pd.to_datetime(temp_df["最近上榜日"]).dt.date
    return temp_df


def stock_lhb_jgmmtj_em(
    start_date: str = "20220906", end_date: str = "20220906"
) -> pd.DataFrame:
    """
    东方财富网-数据中心-龙虎榜单-机构买卖每日统计
    https://data.eastmoney.com/stock/jgmmtj.html
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :return: 机构买卖每日统计
    :rtype: pandas.DataFrame
    """
    start_date = "-".join([start_date[:4], start_date[4:6], start_date[6:]])
    end_date = "-".join([end_date[:4], end_date[4:6], end_date[6:]])
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "NET_BUY_AMT,TRADE_DATE,SECURITY_CODE",
        "sortTypes": "-1,-1,1",
        "pageSize": "5000",
        "pageNumber": "1",
        "reportName": "RPT_ORGANIZATION_TRADE_DETAILS",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f"(TRADE_DATE>='{start_date}')(TRADE_DATE<='{end_date}')",
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    temp_df.reset_index(inplace=True)
    temp_df["index"] = temp_df.index + 1
    temp_df.columns = [
        "序号",
        "-",
        "名称",
        "代码",
        "上榜日期",
        "收盘价",
        "涨跌幅",
        "买方机构数",
        "卖方机构数",
        "机构买入总额",
        "机构卖出总额",
        "机构买入净额",
        "市场总成交额",
        "机构净买额占总成交额比",
        "换手率",
        "流通市值",
        "上榜原因",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
    ]
    temp_df = temp_df[
        [
            "序号",
            "代码",
            "名称",
            "收盘价",
            "涨跌幅",
            "买方机构数",
            "卖方机构数",
            "机构买入总额",
            "机构卖出总额",
            "机构买入净额",
            "市场总成交额",
            "机构净买额占总成交额比",
            "换手率",
            "流通市值",
            "上榜原因",
            "上榜日期",
        ]
    ]
    temp_df["上榜日期"] = pd.to_datetime(temp_df["上榜日期"]).dt.date
    temp_df["收盘价"] = pd.to_numeric(temp_df["收盘价"], errors="coerce")
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
    temp_df["买方机构数"] = pd.to_numeric(temp_df["买方机构数"], errors="coerce")
    temp_df["卖方机构数"] = pd.to_numeric(temp_df["卖方机构数"], errors="coerce")
    temp_df["机构买入总额"] = pd.to_numeric(temp_df["机构买入总额"], errors="coerce")
    temp_df["机构卖出总额"] = pd.to_numeric(temp_df["机构卖出总额"], errors="coerce")
    temp_df["机构买入净额"] = pd.to_numeric(temp_df["机构买入净额"], errors="coerce")
    temp_df["市场总成交额"] = pd.to_numeric(temp_df["市场总成交额"], errors="coerce")
    temp_df["机构净买额占总成交额比"] = pd.to_numeric(temp_df["机构净买额占总成交额比"], errors="coerce")
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
    temp_df["流通市值"] = pd.to_numeric(temp_df["流通市值"], errors="coerce")

    return temp_df


"""
东方财富网-数据中心-龙虎榜单-机构席位追踪
https://data.eastmoney.com/stock/jgstatistic.html
:param symbol: choice of {"近一月", "近三月", "近六月", "近一年"}
:type symbol: str
:return: 机构席位追踪
:rtype: pandas.DataFrame
"""
def stock_lhb_jgstatistic_em(symbol: str = "近一月") -> pd.DataFrame:
    symbol_map = {
        "近一月": "01",
        "近三月": "02",
        "近六月": "03",
        "近一年": "04",
    }
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "ONLIST_TIMES,SECURITY_CODE",
        "sortTypes": "-1,1",
        "pageSize": "5000",
        "pageNumber": "1",
        "reportName": "RPT_ORGANIZATION_SEATNEW",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f'(STATISTICSCYCLE="{symbol_map[symbol]}")',
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    for page in tqdm(range(1, total_page + 1), leave=False):
        # 添加随机延迟，控制每分钟请求数<10次（间隔9-12秒）
        sleep_with_delay('normal')
        params.update({"pageNumber": page})
        r = fetcher.make_request(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)

    big_df.reset_index(inplace=True)
    big_df["index"] = big_df.index + 1
    big_df.rename(
        columns={
            "index": "序号",
            "SECURITY_CODE": "代码",
            "SECURITY_NAME_ABBR": "名称",
            "CLOSE_PRICE": "收盘价",
            "CHANGE_RATE": "涨跌幅",
            "AMOUNT": "龙虎榜成交金额",
            "ONLIST_TIMES": "上榜次数",
            "BUY_AMT": "机构买入额",
            "BUY_TIMES": "机构买入次数",
            "SELL_AMT": "机构卖出额",
            "SELL_TIMES": "机构卖出次数",
            "NET_BUY_AMT": "机构净买额",
            "M1_CLOSE_ADJCHRATE": "近1个月涨跌幅",
            "M3_CLOSE_ADJCHRATE": "近3个月涨跌幅",
            "M6_CLOSE_ADJCHRATE": "近6个月涨跌幅",
            "Y1_CLOSE_ADJCHRATE": "近1年涨跌幅",
        },
        inplace=True,
    )
    big_df = big_df[
        [
            "序号",
            "代码",
            "名称",
            "收盘价",
            "涨跌幅",
            "龙虎榜成交金额",
            "上榜次数",
            "机构买入额",
            "机构买入次数",
            "机构卖出额",
            "机构卖出次数",
            "机构净买额",
            "近1个月涨跌幅",
            "近3个月涨跌幅",
            "近6个月涨跌幅",
            "近1年涨跌幅",
        ]
    ]

    big_df["收盘价"] = pd.to_numeric(big_df["收盘价"], errors="coerce")
    big_df["涨跌幅"] = pd.to_numeric(big_df["涨跌幅"], errors="coerce")
    big_df["龙虎榜成交金额"] = pd.to_numeric(big_df["龙虎榜成交金额"], errors="coerce")
    big_df["上榜次数"] = pd.to_numeric(big_df["上榜次数"], errors="coerce")
    big_df["机构买入额"] = pd.to_numeric(big_df["机构买入额"], errors="coerce")
    big_df["机构买入次数"] = pd.to_numeric(big_df["机构买入次数"], errors="coerce")
    big_df["机构卖出额"] = pd.to_numeric(big_df["机构卖出额"], errors="coerce")
    big_df["机构卖出次数"] = pd.to_numeric(big_df["机构卖出次数"], errors="coerce")
    big_df["机构净买额"] = pd.to_numeric(big_df["机构净买额"], errors="coerce")
    big_df["近1个月涨跌幅"] = pd.to_numeric(big_df["近1个月涨跌幅"], errors="coerce")
    big_df["近3个月涨跌幅"] = pd.to_numeric(big_df["近3个月涨跌幅"], errors="coerce")
    big_df["近6个月涨跌幅"] = pd.to_numeric(big_df["近6个月涨跌幅"], errors="coerce")
    big_df["近1年涨跌幅"] = pd.to_numeric(big_df["近1年涨跌幅"], errors="coerce")
    return big_df


def stock_lhb_hyyyb_em(
    start_date: str = "20220324", end_date: str = "20220324"
) -> pd.DataFrame:
    """
    东方财富网-数据中心-龙虎榜单-每日活跃营业部
    https://data.eastmoney.com/stock/jgmmtj.html
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :return: 每日活跃营业部
    :rtype: pandas.DataFrame
    """
    start_date = "-".join([start_date[:4], start_date[4:6], start_date[6:]])
    end_date = "-".join([end_date[:4], end_date[4:6], end_date[6:]])
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "TOTAL_NETAMT,ONLIST_DATE,OPERATEDEPT_CODE",
        "sortTypes": "-1,-1,1",
        "pageSize": "5000",
        "pageNumber": "1",
        "reportName": "RPT_OPERATEDEPT_ACTIVE",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f"(ONLIST_DATE>='{start_date}')(ONLIST_DATE<='{end_date}')",
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]

    big_df = pd.DataFrame()
    for page in tqdm(range(1, total_page + 1), leave=False):
        # 添加随机延迟，控制每分钟请求数<10次（间隔9-12秒）
        sleep_with_delay('normal')
        params.update({"pageNumber": page})
        r = fetcher.make_request(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)

    big_df.reset_index(inplace=True)
    big_df["index"] = big_df.index + 1
    big_df.columns = [
        "序号",
        "营业部名称",
        "上榜日",
        "买入个股数",
        "卖出个股数",
        "买入总金额",
        "卖出总金额",
        "总买卖净额",
        "-",
        "-",
        "买入股票",
        "-",
        "-",
    ]
    big_df = big_df[
        [
            "序号",
            "营业部名称",
            "上榜日",
            "买入个股数",
            "卖出个股数",
            "买入总金额",
            "卖出总金额",
            "总买卖净额",
            "买入股票",
        ]
    ]

    big_df["上榜日"] = pd.to_datetime(big_df["上榜日"]).dt.date
    big_df["买入个股数"] = pd.to_numeric(big_df["买入个股数"])
    big_df["卖出个股数"] = pd.to_numeric(big_df["卖出个股数"])
    big_df["买入总金额"] = pd.to_numeric(big_df["买入总金额"])
    big_df["卖出总金额"] = pd.to_numeric(big_df["卖出总金额"])
    big_df["总买卖净额"] = pd.to_numeric(big_df["总买卖净额"])
    return big_df


"""
东方财富网-数据中心-龙虎榜单-营业部排行
https://data.eastmoney.com/stock/yybph.html
:param symbol: choice of {"近一月", "近三月", "近六月", "近一年"}
:type symbol: str
:return: 营业部排行
:rtype: pandas.DataFrame
"""
def stock_lhb_yybph_em(symbol: str = "近一月") -> pd.DataFrame:
    symbol_map = {
        "近一月": "01",
        "近三月": "02",
        "近六月": "03",
        "近一年": "04",
    }
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "TOTAL_BUYER_SALESTIMES_1DAY,OPERATEDEPT_CODE",
        "sortTypes": "-1,1",
        "pageSize": "5000",
        "pageNumber": "1",
        "reportName": "RPT_RATEDEPT_RETURNT_RANKING",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f'(STATISTICSCYCLE="{symbol_map[symbol]}")',
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    for page in tqdm(range(1, total_page + 1), leave=False):
        # 添加随机延迟，控制每分钟请求数<10次（间隔9-12秒）
        sleep_with_delay('normal')
        params.update({"pageNumber": page})
        r = fetcher.make_request(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)

    big_df.reset_index(inplace=True)
    big_df["index"] = big_df.index + 1
    big_df.rename(
        columns={
            "index": "序号",
            "OPERATEDEPT_NAME": "营业部名称",
            "TOTAL_BUYER_SALESTIMES_1DAY": "上榜后1天-买入次数",
            "AVERAGE_INCREASE_1DAY": "上榜后1天-平均涨幅",
            "RISE_PROBABILITY_1DAY": "上榜后1天-上涨概率",
            "TOTAL_BUYER_SALESTIMES_2DAY": "上榜后2天-买入次数",
            "AVERAGE_INCREASE_2DAY": "上榜后2天-平均涨幅",
            "RISE_PROBABILITY_2DAY": "上榜后2天-上涨概率",
            "TOTAL_BUYER_SALESTIMES_3DAY": "上榜后3天-买入次数",
            "AVERAGE_INCREASE_3DAY": "上榜后3天-平均涨幅",
            "RISE_PROBABILITY_3DAY": "上榜后3天-上涨概率",
            "TOTAL_BUYER_SALESTIMES_5DAY": "上榜后5天-买入次数",
            "AVERAGE_INCREASE_5DAY": "上榜后5天-平均涨幅",
            "RISE_PROBABILITY_5DAY": "上榜后5天-上涨概率",
            "TOTAL_BUYER_SALESTIMES_10DAY": "上榜后10天-买入次数",
            "AVERAGE_INCREASE_10DAY": "上榜后10天-平均涨幅",
            "RISE_PROBABILITY_10DAY": "上榜后10天-上涨概率",
        },
        inplace=True,
    )
    big_df = big_df[
        [
            "序号",
            "营业部名称",
            "上榜后1天-买入次数",
            "上榜后1天-平均涨幅",
            "上榜后1天-上涨概率",
            "上榜后2天-买入次数",
            "上榜后2天-平均涨幅",
            "上榜后2天-上涨概率",
            "上榜后3天-买入次数",
            "上榜后3天-平均涨幅",
            "上榜后3天-上涨概率",
            "上榜后5天-买入次数",
            "上榜后5天-平均涨幅",
            "上榜后5天-上涨概率",
            "上榜后10天-买入次数",
            "上榜后10天-平均涨幅",
            "上榜后10天-上涨概率",
        ]
    ]

    big_df["上榜后1天-买入次数"] = pd.to_numeric(big_df["上榜后1天-买入次数"], errors="coerce")
    big_df["上榜后1天-平均涨幅"] = pd.to_numeric(big_df["上榜后1天-平均涨幅"], errors="coerce")
    big_df["上榜后1天-上涨概率"] = pd.to_numeric(big_df["上榜后1天-上涨概率"], errors="coerce")

    big_df["上榜后2天-买入次数"] = pd.to_numeric(big_df["上榜后2天-买入次数"], errors="coerce")
    big_df["上榜后2天-平均涨幅"] = pd.to_numeric(big_df["上榜后2天-平均涨幅"], errors="coerce")
    big_df["上榜后2天-上涨概率"] = pd.to_numeric(big_df["上榜后2天-上涨概率"], errors="coerce")

    big_df["上榜后3天-买入次数"] = pd.to_numeric(big_df["上榜后3天-买入次数"], errors="coerce")
    big_df["上榜后3天-平均涨幅"] = pd.to_numeric(big_df["上榜后3天-平均涨幅"], errors="coerce")
    big_df["上榜后3天-上涨概率"] = pd.to_numeric(big_df["上榜后3天-上涨概率"], errors="coerce")

    big_df["上榜后5天-买入次数"] = pd.to_numeric(big_df["上榜后5天-买入次数"], errors="coerce")
    big_df["上榜后5天-平均涨幅"] = pd.to_numeric(big_df["上榜后5天-平均涨幅"], errors="coerce")
    big_df["上榜后5天-上涨概率"] = pd.to_numeric(big_df["上榜后5天-上涨概率"], errors="coerce")

    big_df["上榜后10天-买入次数"] = pd.to_numeric(big_df["上榜后10天-买入次数"], errors="coerce")
    big_df["上榜后10天-平均涨幅"] = pd.to_numeric(big_df["上榜后10天-平均涨幅"], errors="coerce")
    big_df["上榜后10天-上涨概率"] = pd.to_numeric(big_df["上榜后10天-上涨概率"], errors="coerce")
    return big_df


"""
东方财富网-数据中心-龙虎榜单-营业部统计
https://data.eastmoney.com/stock/traderstatistic.html
:param symbol: choice of {"近一月", "近三月", "近六月", "近一年"}
:type symbol: str
:return: 营业部统计
:rtype: pandas.DataFrame
"""
def stock_lhb_traderstatistic_em(symbol: str = "近一月") -> pd.DataFrame:
    symbol_map = {
        "近一月": "01",
        "近三月": "02",
        "近六月": "03",
        "近一年": "04",
    }
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "AMOUNT,OPERATEDEPT_CODE",
        "sortTypes": "-1,1",
        "pageSize": "5000",
        "pageNumber": "1",
        "reportName": "RPT_OPERATEDEPT_LIST_STATISTICS",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f'(STATISTICSCYCLE="{symbol_map[symbol]}")',
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    for page in tqdm(range(1, total_page + 1), leave=False):
        # 添加随机延迟，控制每分钟请求数<10次（间隔9-12秒）
        sleep_with_delay('normal')
        params.update({"pageNumber": page})
        r = fetcher.make_request(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)

    big_df.reset_index(inplace=True)
    big_df["index"] = big_df.index + 1
    big_df.rename(
        columns={
            "index": "序号",
            "OPERATEDEPT_NAME": "营业部名称",
            "AMOUNT": "龙虎榜成交金额",
            "SALES_ONLIST_TIMES": "上榜次数",
            "ACT_BUY": "买入额",
            "TOTAL_BUYER_SALESTIMES": "买入次数",
            "ACT_SELL": "卖出额",
            "TOTAL_SELLER_SALESTIMES": "卖出次数",
        },
        inplace=True,
    )
    big_df = big_df[
        [
            "序号",
            "营业部名称",
            "龙虎榜成交金额",
            "上榜次数",
            "买入额",
            "买入次数",
            "卖出额",
            "卖出次数",
        ]
    ]

    big_df["龙虎榜成交金额"] = pd.to_numeric(big_df["龙虎榜成交金额"], errors="coerce")
    big_df["上榜次数"] = pd.to_numeric(big_df["上榜次数"], errors="coerce")
    big_df["买入额"] = pd.to_numeric(big_df["买入额"], errors="coerce")
    big_df["买入次数"] = pd.to_numeric(big_df["买入次数"], errors="coerce")
    big_df["卖出额"] = pd.to_numeric(big_df["卖出额"], errors="coerce")
    big_df["卖出次数"] = pd.to_numeric(big_df["卖出次数"], errors="coerce")
    return big_df


"""
东方财富网-数据中心-龙虎榜单-个股龙虎榜详情-日期
https://data.eastmoney.com/stock/tradedetail.html
:param symbol: 股票代码
:type symbol: str
:return: 个股龙虎榜详情-日期
:rtype: pandas.DataFrame
"""
def stock_lhb_stock_detail_date_em(symbol: str = "600077") -> pd.DataFrame:
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_LHB_BOARDDATE",
        "columns": "SECURITY_CODE,TRADE_DATE,TR_DATE",
        "filter": f'(SECURITY_CODE="{symbol}")',
        "pageNumber": "1",
        "pageSize": "1000",
        "sortTypes": "-1",
        "sortColumns": "TRADE_DATE",
        "source": "WEB",
        "client": "WEB",
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    temp_df.reset_index(inplace=True)
    temp_df["index"] = temp_df.index + 1
    temp_df.columns = [
        "序号",
        "股票代码",
        "交易日",
        "-",
    ]
    temp_df = temp_df[
        [
            "序号",
            "股票代码",
            "交易日",
        ]
    ]
    temp_df["交易日"] = pd.to_datetime(temp_df["交易日"]).dt.date
    return temp_df


def stock_lhb_stock_detail_em(
    symbol: str = "000788", date: str = "20220315", flag: str = "卖出"
) -> pd.DataFrame:
    """
    东方财富网-数据中心-龙虎榜单-个股龙虎榜详情
    https://data.eastmoney.com/stock/lhb/600077.html
    :param symbol: 股票代码
    :type symbol: str
    :param date: 查询日期; 需要通过 ak.stock_lhb_stock_detail_date_em(symbol="600077") 接口获取相应股票的有龙虎榜详情数据的日期
    :type date: str
    :param flag: choice of {"买入", "卖出"}
    :type flag: str
    :return: 个股龙虎榜详情
    :rtype: pandas.DataFrame
    """
    flag_map = {
        "买入": "BUY",
        "卖出": "SELL",
    }
    report_map = {
        "买入": "RPT_BILLBOARD_DAILYDETAILSBUY",
        "卖出": "RPT_BILLBOARD_DAILYDETAILSSELL",
    }
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": report_map[flag],
        "columns": "ALL",
        "filter": f"""(TRADE_DATE='{'-'.join([date[:4], date[4:6], date[6:]])}')(SECURITY_CODE="{symbol}")""",
        "pageNumber": "1",
        "pageSize": "500",
        "sortTypes": "-1",
        "sortColumns": flag_map[flag],
        "source": "WEB",
        "client": "WEB",
        "_": "1647338693644",
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    temp_df.reset_index(inplace=True)
    temp_df["index"] = temp_df.index + 1

    if flag == "买入":
        temp_df.columns = [
            "序号",
            "-",
            "-",
            "-",
            "-",
            "交易营业部名称",
            "类型",
            "-",
            "-",
            "-",
            "-",
            "买入金额",
            "卖出金额",
            "净额",
            "-",
            "-",
            "-",
            "-",
            "买入金额-占总成交比例",
            "卖出金额-占总成交比例",
            "-",
        ]
        temp_df = temp_df[
            [
                "序号",
                "交易营业部名称",
                "买入金额",
                "买入金额-占总成交比例",
                "卖出金额",
                "卖出金额-占总成交比例",
                "净额",
                "类型",
            ]
        ]
        temp_df["买入金额"] = pd.to_numeric(temp_df["买入金额"])
        temp_df["买入金额-占总成交比例"] = pd.to_numeric(temp_df["买入金额-占总成交比例"])
        temp_df["卖出金额"] = pd.to_numeric(temp_df["卖出金额"])
        temp_df["卖出金额-占总成交比例"] = pd.to_numeric(temp_df["卖出金额-占总成交比例"])
        temp_df.sort_values("类型", inplace=True)
        temp_df.reset_index(inplace=True, drop=True)
        temp_df["序号"] = range(1, len(temp_df["序号"]) + 1)
    else:
        temp_df.columns = [
            "序号",
            "-",
            "-",
            "-",
            "-",
            "交易营业部名称",
            "类型",
            "-",
            "-",
            "-",
            "-",
            "买入金额",
            "卖出金额",
            "净额",
            "-",
            "-",
            "-",
            "-",
            "买入金额-占总成交比例",
            "卖出金额-占总成交比例",
            "-",
        ]
        temp_df = temp_df[
            [
                "序号",
                "交易营业部名称",
                "买入金额",
                "买入金额-占总成交比例",
                "卖出金额",
                "卖出金额-占总成交比例",
                "净额",
                "类型",
            ]
        ]
        temp_df["买入金额"] = pd.to_numeric(temp_df["买入金额"])
        temp_df["买入金额-占总成交比例"] = pd.to_numeric(temp_df["买入金额-占总成交比例"])
        temp_df["卖出金额"] = pd.to_numeric(temp_df["卖出金额"])
        temp_df["卖出金额-占总成交比例"] = pd.to_numeric(temp_df["卖出金额-占总成交比例"])
        temp_df.sort_values("类型", inplace=True)
        temp_df.reset_index(inplace=True, drop=True)
        temp_df["序号"] = range(1, len(temp_df["序号"]) + 1)
    return temp_df


if __name__ == "__main__":
    stock_lhb_detail_em_df = stock_lhb_detail_em(
        start_date="20230403", end_date="20230417"
    )
    print(stock_lhb_detail_em_df)

    stock_lhb_stock_statistic_em_df = stock_lhb_stock_statistic_em(symbol="近一月")
    print(stock_lhb_stock_statistic_em_df)

    stock_lhb_stock_statistic_em_df = stock_lhb_stock_statistic_em(symbol="近三月")
    print(stock_lhb_stock_statistic_em_df)

    stock_lhb_stock_statistic_em_df = stock_lhb_stock_statistic_em(symbol="近六月")
    print(stock_lhb_stock_statistic_em_df)

    stock_lhb_stock_statistic_em_df = stock_lhb_stock_statistic_em(symbol="近一年")
    print(stock_lhb_stock_statistic_em_df)

    stock_lhb_jgmmtj_em_df = stock_lhb_jgmmtj_em(
        start_date="20220904", end_date="20220906"
    )
    print(stock_lhb_jgmmtj_em_df)

    stock_lhb_jgstatistic_em_df = stock_lhb_jgstatistic_em(symbol="近一月")
    print(stock_lhb_jgstatistic_em_df)

    stock_lhb_hyyyb_em_df = stock_lhb_hyyyb_em(
        start_date="20220324", end_date="20220324"
    )
    print(stock_lhb_hyyyb_em_df)

    stock_lhb_yybph_em_df = stock_lhb_yybph_em(symbol="近一月")
    print(stock_lhb_yybph_em_df)

    stock_lhb_traderstatistic_em_df = stock_lhb_traderstatistic_em(symbol="近一月")
    print(stock_lhb_traderstatistic_em_df)

    stock_lhb_stock_detail_date_em_df = stock_lhb_stock_detail_date_em(symbol="002901")
    print(stock_lhb_stock_detail_date_em_df)

    stock_lhb_stock_detail_em_df = stock_lhb_stock_detail_em(
        symbol="002901", date="20221012", flag="买入"
    )
    print(stock_lhb_stock_detail_em_df)

    stock_lhb_stock_detail_em_df = stock_lhb_stock_detail_em(
        symbol="600016", date="20220324", flag="买入"
    )
    print(stock_lhb_stock_detail_em_df)
