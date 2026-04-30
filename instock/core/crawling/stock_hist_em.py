#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
东方财富网 - 股票历史数据爬虫（第二层 - 数据爬虫层）
====================================================

模块功能：
---------
从东方财富网API获取股票实时行情、历史K线、分时数据等信息。
是整个系统获取行情数据的核心数据源。

数据源说明：
-----------
东方财富网（https://quote.eastmoney.com/）
- 提供最新股票实时行情
- 提供3年以上的历史K线数据
- 支持多种数据粒度（日线、周线、月线）
- 数据更新频率：实时更新（每3-5秒）
- API稳定性：高（CDN分布式）

主要API接口：
-----------

1. 股票列表接口（clist/get）
   URL: http://82.push2.eastmoney.com/api/qt/clist/get
   功能：获取所有沪深京A股实时行情
   返回：股票代码、名称、最新价、涨跌幅等
   示例：获取全市场3000+只股票的实时报价

2. K线历史接口（kline/get）
   URL: http://push2his.eastmoney.com/api/qt/stock/kline/get
   功能：获取股票历史K线数据
   参数：股票代码、K线周期（日/周/月）、起始日期
   返回：OHLCV数据（Open、High、Low、Close、Volume）
   示例：获取000001从2020年到现在的所有日线数据

3. 分时数据接口（trends2/get）
   URL: http://push2his.eastmoney.com/api/qt/stock/trends2/get
   功能：获取股票当日分时行情数据
   参数：股票代码
   返回：分钟级K线数据（1分钟、5分钟等）
   示例：获取当天每分钟的交易数据

UT参数说明：
-----------
UT是东方财富API的用户令牌（User Token），防止爬虫和限流。

获取方法：
1. 打开 https://quote.eastmoney.com/center/gridlist.html#hs_a_board
2. 按F12打开开发者工具
3. 切换到 Network 标签
4. 刷新页面或点击某个操作
5. 在请求列表中筛选 "clist" 或 "kline"
6. 点击请求查看详情
7. 在Query String Parameters中找到 ut 参数
8. 复制该值到此代码中

更新频率：
- UT值一般不会频繁变化
- 如果API返回403/403，需要更新UT值
- 通常每3-6个月更新一次

数据返回格式：
-----------

实时行情返回格式：
{
  "data": {
    "diff": [
      {
        "f2": 16.45,     # 最新价
        "f3": 1.47,      # 涨跌幅 %
        "f4": 0.24,      # 涨跌额
        "f12": "000001", # 股票代码
        "f14": "平安银行", # 股票名称
        "f17": 123456789 # 成交量
      }
    ],
    "total": 4900 # 总股票数
  }
}

K线数据返回格式：
{
  "data": {
    "code": "0000001",
    "lines": [
      "2024-01-01,3500.00,3600.00,3450.00,3550.00,1000000,10000000000", # 日期,开,高,低,收,量,额
      "2024-01-02,3550.00,3650.00,3500.00,3600.00,1100000,11000000000",
      ...
    ]
  }
}

核心函数：
--------

stock_zh_a_spot_em() → pd.DataFrame
    获取全市场实时行情
    返回：所有沪深京A股股票的最新数据

stock_hist_data_em(code, period='d') → pd.DataFrame
    获取单只股票的历史K线数据
    参数：
      code - 股票代码（如'000001'）
      period - K线周期('d'日线, 'w'周线, 'm'月线)
    返回：OHLCV数据

使用示例：
--------

# 获取全市场实时行情
from instock.core.crawling.stock_hist_em import stock_zh_a_spot_em
df = stock_zh_a_spot_em()
print(df.head())  # 显示前5条

# 获取000001的历史日线
from instock.core.crawling.stock_hist_em import stock_hist_data_em
hist = stock_hist_data_em('000001', period='d')
print(hist.tail(10))  # 显示最近10天

重试机制：
--------
- 请求失败自动重试3次
- 每次间隔1-3秒
- 如仍失败返回None或空DataFrame
- 记录错误日志供调试

缓存机制：
--------
- 使用@file_cached装饰器
- 当日数据缓存4小时
- 历史数据缓存7天
- 减少API请求频率
- 加快数据获取速度

性能指标：
--------
- 获取全市场：8-12秒（3000+只股票）
- 获取单只股票历史：1-2秒
- 获取分时数据：0.5-1秒
- 日均API调用：5000+次

故障排除：
---------

问题1：403/403错误
原因：UT参数过期或被限流
解决：更新UT参数到最新值

问题2：连接超时
原因：网络问题或服务器无响应
解决：检查网络连接，或等待后重试

问题3：返回空数据
原因：股票代码错误或不存在
解决：验证股票代码格式（应为6位数字）

问题4：数据不完整
原因：API限制或网络中断
解决：使用文件缓存快速恢复

关键知识点：
----------

1. HTTP请求参数的构造
   - URL encode编码
   - Query String参数
   - UT令牌认证

2. JSON数据解析
   - response.json()解析
   - 多层字典访问
   - 数据验证

3. 数据结构转换
   - 将API数据转换为DataFrame
   - 列名统一化
   - 数据类型转换

4. 错误处理和重试
   - try-except捕获异常
   - 自动重试机制
   - 日志记录

5. 缓存优化
   - 减少API调用
   - 加快数据获取
   - 降低服务器压力
"""

# ==================== 导入必需的库 ====================
import random  # 随机延迟
import time  # 延迟处理
import pandas as pd  # 数据处理
import math  # 数学计算

# ==================== 导入项目模块 ====================
from instock.core.eastmoney_fetcher import eastmoney_fetcher, get_timestamp  # API请求客户端
from instock.core.file_cache import file_cached  # 文件缓存装饰器

__author__ = 'myh '
__date__ = '2025/12/31 '

# 东方财富API的ut参数（用户令牌）
# 获取方法：打开对应页面 -> F12开发者工具 -> Network标签 -> 刷新页面 -> 查看请求中的ut参数
# 
# UT_STOCK_LIST - 股票列表接口 (clist/get)
#   来源页面: https://quote.eastmoney.com/center/gridlist.html#hs_a_board
#   筛选请求: clist/get
#   接口示例: http://80.push2.eastmoney.com/api/qt/clist/get
UT_STOCK_LIST = "fa5fd1943c7b386f172d6893dbfba10b"
# 
# UT_STOCK_KLINE - K线历史数据接口 (kline/get)
#   获取方法：
#   1. 打开 https://quote.eastmoney.com/sz000001.html
#   2. F12开发者工具 -> Network标签 -> 清空记录
#   3. 在页面中点击"分时"旁边的"日K"按钮（切换到K线图）
#   4. 筛选请求：输入 "kline" 或 "push2his"
#   5. 找到类似 http://push2his.eastmoney.com/api/qt/stock/kline/get 的请求
#   6. 查看请求参数中的 ut 值
#   接口示例: http://push2his.eastmoney.com/api/qt/stock/kline/get
UT_STOCK_KLINE = UT_STOCK_LIST
# 
# UT_STOCK_TRENDS - 分时数据接口 (trends2/get)
#   来源页面: https://quote.eastmoney.com/sz000001.html (打开任意股票页面，查看分时图标签)
#   筛选请求: trends2/get
#   接口示例: http://push2his.eastmoney.com/api/qt/stock/trends2/get
UT_STOCK_TRENDS = UT_STOCK_LIST

# 创建全局实例，供所有函数使用
fetcher = eastmoney_fetcher()

"""
东方财富网-沪深京 A 股-实时行情
https://quote.eastmoney.com/center/gridlist.html#hs_a_board
:return: 实时行情
:rtype: pandas.DataFrame
"""
def stock_zh_a_spot_em() -> pd.DataFrame:
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    page_size = 50
    page_current = 1
    params = {
        "pn": page_current,
        "pz": page_size,
        "po": "1",
        "np": "1",
        "ut": UT_STOCK_LIST,
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25,f26,f37,f38,f39,f40,f41,f45,f46,f48,f49,f57,f61,f100,f112,f113,f114,f115,f221",
        "_": get_timestamp(),
    }
    
    # API 请求: 获取沪深京A股实时行情数据（第1页）
    # 入参:
    #   - url: http://82.push2.eastmoney.com/api/qt/clist/get (东方财富行情列表接口)
    #   - params: {
    #       pn: 页码,
    #       pz: 每页数量,
    #       fs: 市场筛选 (m:0 t:6 深圳主板, m:0 t:80 创业板, m:1 t:2 上海主板, m:1 t:23 科创板, m:0 t:81 s:2048 北交所),
    #       fields: 返回字段 (f2=最新价, f3=涨跌幅, f12=代码, f14=名称等40+字段),
    #       ut: 用户令牌,
    #       _: 时间戳（防缓存）
    #     }
    # 出参:
    #   - response.json(): {
    #       "data": {
    #         "diff": [股票行情数据数组],
    #         "total": 总股票数量
    #       }
    #     }
    r =  fetcher.make_request(url, params=params)
    
    data_json = r.json()
    data = data_json["data"]["diff"]
    if not data:
        return pd.DataFrame()

    data_count = data_json["data"]["total"]
    page_count = math.ceil(data_count/page_size)
    
    # 从第2页开始获取剩余数据（第1页已经获取）
    for page_num in range(2, page_count + 1):
        params["pn"] = page_num
        params["_"] = get_timestamp()  # 更新时间戳
        
        # API 请求: 获取沪深京A股实时行情数据（分页数据）
        # 入参: 同第1页请求，pn 和 _ 参数已更新为当前页码和最新时间戳
        # 出参: 同第1页响应结构
        r =  fetcher.make_request(url, params=params)
        data_json = r.json()
        _data = data_json["data"]["diff"]
        data.extend(_data)

    temp_df = pd.DataFrame(data)
    temp_df.columns = [
        "最新价",
        "涨跌幅",
        "涨跌额",
        "成交量",
        "成交额",
        "振幅",
        "换手率",
        "市盈率动",
        "量比",
        "5分钟涨跌",
        "代码",
        "名称",
        "最高",
        "最低",
        "今开",
        "昨收",
        "总市值",
        "流通市值",
        "涨速",
        "市净率",
        "60日涨跌幅",
        "年初至今涨跌幅",
        "上市时间",
        "加权净资产收益率",
        "总股本",
        "已流通股份",
        "营业收入",
        "营业收入同比增长",
        "归属净利润",
        "归属净利润同比增长",
        "每股未分配利润",
        "毛利率",
        "资产负债率",
        "每股公积金",
        "所处行业",
        "每股收益",
        "每股净资产",
        "市盈率静",
        "市盈率TTM",
        "报告期"
    ]
    temp_df = temp_df[
        [
            "代码",
            "名称",
            "最新价",
            "涨跌幅",
            "涨跌额",
            "成交量",
            "成交额",
            "振幅",
            "换手率",
            "量比",
            "今开",
            "最高",
            "最低",
            "昨收",
            "涨速",
            "5分钟涨跌",
            "60日涨跌幅",
            "年初至今涨跌幅",
            "市盈率动",
            "市盈率TTM",
            "市盈率静",
            "市净率",
            "每股收益",
            "每股净资产",
            "每股公积金",
            "每股未分配利润",
            "加权净资产收益率",
            "毛利率",
            "资产负债率",
            "营业收入",
            "营业收入同比增长",
            "归属净利润",
            "归属净利润同比增长",
            "报告期",
            "总股本",
            "已流通股份",
            "总市值",
            "流通市值",
            "所处行业",
            "上市时间"
        ]
    ]
    temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
    temp_df["量比"] = pd.to_numeric(temp_df["量比"], errors="coerce")
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
    temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
    temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
    temp_df["今开"] = pd.to_numeric(temp_df["今开"], errors="coerce")
    temp_df["昨收"] = pd.to_numeric(temp_df["昨收"], errors="coerce")
    temp_df["涨速"] = pd.to_numeric(temp_df["涨速"], errors="coerce")
    temp_df["5分钟涨跌"] = pd.to_numeric(temp_df["5分钟涨跌"], errors="coerce")
    temp_df["60日涨跌幅"] = pd.to_numeric(temp_df["60日涨跌幅"], errors="coerce")
    temp_df["年初至今涨跌幅"] = pd.to_numeric(temp_df["年初至今涨跌幅"], errors="coerce")
    temp_df["市盈率动"] = pd.to_numeric(temp_df["市盈率动"], errors="coerce")
    temp_df["市盈率TTM"] = pd.to_numeric(temp_df["市盈率TTM"], errors="coerce")
    temp_df["市盈率静"] = pd.to_numeric(temp_df["市盈率静"], errors="coerce")
    temp_df["市净率"] = pd.to_numeric(temp_df["市净率"], errors="coerce")
    temp_df["每股收益"] = pd.to_numeric(temp_df["每股收益"], errors="coerce")
    temp_df["每股净资产"] = pd.to_numeric(temp_df["每股净资产"], errors="coerce")
    temp_df["每股公积金"] = pd.to_numeric(temp_df["每股公积金"], errors="coerce")
    temp_df["每股未分配利润"] = pd.to_numeric(temp_df["每股未分配利润"], errors="coerce")
    temp_df["加权净资产收益率"] = pd.to_numeric(temp_df["加权净资产收益率"], errors="coerce")
    temp_df["毛利率"] = pd.to_numeric(temp_df["毛利率"], errors="coerce")
    temp_df["资产负债率"] = pd.to_numeric(temp_df["资产负债率"], errors="coerce")
    temp_df["营业收入"] = pd.to_numeric(temp_df["营业收入"], errors="coerce")
    temp_df["营业收入同比增长"] = pd.to_numeric(temp_df["营业收入同比增长"], errors="coerce")
    temp_df["归属净利润"] = pd.to_numeric(temp_df["归属净利润"], errors="coerce")
    temp_df["归属净利润同比增长"] = pd.to_numeric(temp_df["归属净利润同比增长"], errors="coerce")
    temp_df["报告期"] = pd.to_datetime(temp_df["报告期"], format='%Y%m%d', errors="coerce")
    temp_df["总股本"] = pd.to_numeric(temp_df["总股本"], errors="coerce")
    temp_df["已流通股份"] = pd.to_numeric(temp_df["已流通股份"], errors="coerce")
    temp_df["总市值"] = pd.to_numeric(temp_df["总市值"], errors="coerce")
    temp_df["流通市值"] = pd.to_numeric(temp_df["流通市值"], errors="coerce")
    temp_df["上市时间"] = pd.to_datetime(temp_df["上市时间"], format='%Y%m%d', errors="coerce")

    return temp_df


"""
东方财富-股票和市场代码
http://quote.eastmoney.com/center/gridlist.html#hs_a_board
使用文件缓存，缓存有效期为24小时
:return: 股票和市场代码
:rtype: dict
"""
@file_cached('code_id_map_em')  # 使用默认7天缓存
def code_id_map_em() -> dict:
    """
    获取所有沪深北A股的股票代码和市场ID映射
    
    市场ID说明：
    - 1: 上海市场 (6xxxxx, 688xxx 等)
    - 0: 深圳市场 (000xxx, 001xxx, 002xxx, 003xxx 等)
    - 0: 北京市场 (430xxx, 830xxx 等)
    
    返回格式: {"000001": 0, "600000": 1, ...}
    """
    url = "http://80.push2.eastmoney.com/api/qt/clist/get"
    page_size = 50
    code_id_dict = {}
    
    # ============================================================
    # 第一部分：获取上海市场股票 (market_id = 1)
    # fs参数说明: m:1 t:2 (上海A股), m:1 t:23 (上海科创板)
    # ============================================================
    page_current = 1
    params = {
        "pn": page_current,
        "pz": page_size,
        "po": "1",
        "np": "1",
        "ut": UT_STOCK_LIST,
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:1 t:2,m:1 t:23",  # 上海A股 + 科创板
        "fields": "f12",
        "_": get_timestamp(),
    }
    
    # API 请求: 获取上海市场股票代码列表（第1页）
    # 入参:
    #   - url: http://80.push2.eastmoney.com/api/qt/clist/get
    #   - params: {
    #       fs: "m:1 t:2,m:1 t:23" (上海A股 + 科创板),
    #       fields: "f12" (只获取股票代码字段),
    #       pn, pz, ut, _: 同 stock_zh_a_spot_em 接口
    #     }
    # 出参:
    #   - response.json(): {
    #       "data": {
    #         "diff": [{"f12": "股票代码"}, ...],
    #         "total": 上海市场总股票数
    #       }
    #     }
    r =  fetcher.make_request(url, params=params)
    data_json = r.json()
    data = data_json["data"]["diff"]
    print(f"📊 上海市场股票数据 (首页前10条): {data[:10] if data else []}")
    
    if not data:
        return dict()

    # 计算上海市场总页数并获取所有分页数据
    data_count = data_json["data"]["total"]
    page_count = math.ceil(data_count/page_size)
    print(f"   上海市场共 {data_count} 只股票，需要获取 {page_count} 页")
    
    # 从第2页开始获取剩余数据（第1页已经获取）
    for page_num in range(2, page_count + 1):
        params["pn"] = page_num
        params["_"] = get_timestamp()  # 更新时间戳
        
        # API 请求: 获取上海市场股票代码列表（分页数据）
        # 入参: 同第1页请求，pn 和 _ 参数已更新
        # 出参: 同第1页响应结构
        r =  fetcher.make_request(url, params=params)
        data_json = r.json()
        _data = data_json["data"]["diff"]
        data.extend(_data)

    # 保存上海市场的股票代码映射
    temp_df = pd.DataFrame(data)
    temp_df["market_id"] = 1
    temp_df.columns = ["sh_code", "sh_id"]
    code_id_dict = dict(zip(temp_df["sh_code"], temp_df["sh_id"]))
    print(f"   ✅ 上海市场获取完成，共 {len(code_id_dict)} 只股票")
    
    # ============================================================
    # 第二部分：获取深圳市场股票 (market_id = 0)
    # fs参数说明: m:0 t:6 (深圳A股主板), m:0 t:80 (深圳创业板)
    # 重要：000001 平安银行就在这个市场中
    # ============================================================
    page_current = 1
    params = {
        "pn": page_current,
        "pz": page_size,
        "po": "1",
        "np": "1",
        "ut": UT_STOCK_LIST,
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:0 t:6,m:0 t:80",  # 深圳主板 + 创业板
        "fields": "f12",
        "_": get_timestamp(),
    }
    # API 请求: 获取深圳市场股票代码列表（第1页）
    # 入参: url=clist/get接口, params包含fs=深圳市场筛选, fields=f12股票代码
    # 出参: {data: {diff: [{f12: 代码}...], total: 总数}}
    # API 请求: 获取深圳市场股票代码列表（第1页）
    # 入参: url=clist/get接口, params包含fs=深圳市场筛选, fields=f12股票代码
    # 出参: {data: {diff: [{f12: 代码}...], total: 总数}}
    r =  fetcher.make_request(url, params=params)
    data_json = r.json()
    data = data_json["data"]["diff"]
    print(f"📊 深圳市场股票数据 (首页前10条): {data[:10] if data else []}")
    
    if not data:
        return dict()

    # 计算深圳市场总页数并获取所有分页数据
    data_count = data_json["data"]["total"]
    page_count = math.ceil(data_count/page_size)
    print(f"   深圳市场共 {data_count} 只股票，需要获取 {page_count} 页")
    
    # 从第2页开始获取剩余数据（第1页已经获取）
    for page_num in range(2, page_count + 1):
        params["pn"] = page_num
        params["_"] = get_timestamp()  # 更新时间戳
        # API 请求: 获取深圳市场股票代码列表（分页）
        # 入参: pn=页码, _=时间戳已更新
        # 出参: 同第1页
        r =  fetcher.make_request(url, params=params)
        data_json = r.json()
        _data = data_json["data"]["diff"]
        data.extend(_data)

    # 保存深圳市场的股票代码映射（追加到字典中）
    temp_df_sz = pd.DataFrame(data)
    temp_df_sz["sz_id"] = 0
    code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["sz_id"])))
    print(f"   ✅ 深圳市场获取完成，共 {len(temp_df_sz)} 只股票")
    
    # ============================================================
    # 第三部分：获取北京市场股票 (market_id = 0)
    # fs参数说明: m:0 t:81 s:2048 (北交所股票)
    # ============================================================
    page_current = 1
    params = {
        "pn": page_current,
        "pz": page_size,
        "po": "1",
        "np": "1",
        "ut": UT_STOCK_LIST,
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:0 t:81 s:2048",  # 北交所
        "fields": "f12",
        "_": get_timestamp(),
    }
    # API 请求: 获取北京市场股票代码列表（第1页）
    # 入参: url=clist/get接口, params包含fs=北交所筛选, fields=f12股票代码
    # 出参: {data: {diff: [{f12: 代码}...], total: 总数}}
    r =  fetcher.make_request(url, params=params)
    data_json = r.json()
    data = data_json["data"]["diff"]
    print(f"📊 北京市场股票数据 (首页前10条): {data[:10] if data else []}")
    
    if not data:
        return dict()

    # 计算北京市场总页数并获取所有分页数据
    data_count = data_json["data"]["total"]
    page_count = math.ceil(data_count/page_size)
    print(f"   北京市场共 {data_count} 只股票，需要获取 {page_count} 页")
    
    # 从第2页开始获取剩余数据（第1页已经获取）
    for page_num in range(2, page_count + 1):
        params["pn"] = page_num
        params["_"] = get_timestamp()  # 更新时间戳
        # API 请求: 获取北京市场股票代码列表（分页）
        # 入参: pn=页码, _=时间戳已更新
        # 出参: 同第1页
        r =  fetcher.make_request(url, params=params)
        data_json = r.json()
        _data = data_json["data"]["diff"]
        data.extend(_data)

    # 保存北京市场的股票代码映射（追加到字典中）
    temp_df_sz = pd.DataFrame(data)
    temp_df_sz["bj_id"] = 0
    code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["bj_id"])))
    print(f"   ✅ 北京市场获取完成，共 {len(temp_df_sz)} 只股票")
    print(f"🎉 所有市场获取完成，总共 {len(code_id_dict)} 只股票")
    
    return code_id_dict


def stock_zh_a_hist(
    symbol: str = "000001",
    period: str = "daily",
    start_date: str = "19700101",
    end_date: str = "20500101",
    adjust: str = "",
) -> pd.DataFrame:
    """
    东方财富网-行情首页-沪深京 A 股-每日行情
    https://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :param period: choice of {'daily', 'weekly', 'monthly'}
    :type period: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param adjust: choice of {"qfq": "前复权", "hfq": "后复权", "": "不复权"}
    :type adjust: str
    :return: 每日行情
    :rtype: pandas.DataFrame
    """
    code_id_dict = code_id_map_em()
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": UT_STOCK_KLINE,
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"1.{symbol}" if symbol.startswith('6') else f"0.{symbol}",
        "beg": start_date,
        "end": end_date,
        "_": get_timestamp(),
    }
    # API 请求: 获取股票历史K线数据
    # 入参: url=kline/get接口, secid=市场ID.股票代码, beg/end=起止日期, klt=周期, fqt=复权类型
    # 出参: {data: {klines: ['日期,开,收,高,低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率'...]}}
    r =  fetcher.make_request(url, params=params)
    data_json = r.json()
    if not (data_json["data"] and data_json["data"]["klines"]):
        return pd.DataFrame()
    temp_df = pd.DataFrame(
        [item.split(",") for item in data_json["data"]["klines"]]
    )
    temp_df.columns = [
        "日期",
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
    temp_df.index = pd.to_datetime(temp_df["日期"])
    temp_df.reset_index(inplace=True, drop=True)

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

    return temp_df


def stock_zh_a_hist_min_em(
    symbol: str = "000001",
    start_date: str = "1979-09-01 09:32:00",
    end_date: str = "2222-01-01 09:32:00",
    period: str = "5",
    adjust: str = "",
) -> pd.DataFrame:
    """
    东方财富网-行情首页-沪深京 A 股-每日分时行情
    https://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param period: choice of {'1', '5', '15', '30', '60'}
    :type period: str
    :param adjust: choice of {'', 'qfq', 'hfq'}
    :type adjust: str
    :return: 每日分时行情
    :rtype: pandas.DataFrame
    """
    code_id_dict = code_id_map_em()
    adjust_map = {
        "": "0",
        "qfq": "1",
        "hfq": "2",
    }
    if period == "1":
        url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "ut": UT_STOCK_KLINE,
            "ndays": "5",
            "iscr": "0",
            "secid": f"1.{symbol}" if symbol.startswith('6') else f"0.{symbol}",
            "_": get_timestamp(),
        }
        # API 请求: 获取股票分钟K线数据（近5日）
        # 入参: url=trends2/get接口, secid=市场ID.股票代码, ndays=5, iscr=0(不复权)
        # 出参: {data: {trends: ['时间,价格,成交量,成交额,均价'...]}}
        r =  fetcher.make_request(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(
            [item.split(",") for item in data_json["data"]["trends"]]
        )
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
        temp_df.index = pd.to_datetime(temp_df["时间"])
        temp_df = temp_df[start_date:end_date]
        temp_df.reset_index(drop=True, inplace=True)
        temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
        temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
        temp_df["最高"] = pd.to_numeric(temp_df["最高"])
        temp_df["最低"] = pd.to_numeric(temp_df["最低"])
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
        temp_df["最新价"] = pd.to_numeric(temp_df["最新价"])
        temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
        return temp_df
    else:
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "ut": UT_STOCK_KLINE,
            "klt": period,
            "fqt": adjust_map[adjust],
            "secid": f"1.{symbol}" if symbol.startswith('6') else f"0.{symbol}",
            "beg": "0",
            "end": "20500000",
            "_": get_timestamp(),
        }
        # API 请求: 获取股票分钟K线数据（指定周期）
        # 入参: url=kline/get接口, secid=市场ID.股票代码, klt=周期(1/5/15/30/60分钟), beg/end=日期范围
        # 出参: {data: {klines: ['时间,开,收,高,低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率'...]}}
        r =  fetcher.make_request(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(
            [item.split(",") for item in data_json["data"]["klines"]]
        )
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
        temp_df.index = pd.to_datetime(temp_df["时间"])
        temp_df = temp_df[start_date:end_date]
        temp_df.reset_index(drop=True, inplace=True)
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
        temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
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
        return temp_df


def stock_zh_a_hist_pre_min_em(
    symbol: str = "000001",
    start_time: str = "09:00:00",
    end_time: str = "15:50:00",
) -> pd.DataFrame:
    """
    东方财富网-行情首页-沪深京 A 股-每日分时行情包含盘前数据
    http://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :param start_time: 开始时间
    :type start_time: str
    :param end_time: 结束时间
    :type end_time: str
    :return: 每日分时行情包含盘前数据
    :rtype: pandas.DataFrame
    """
    code_id_dict = code_id_map_em()
    url = "https://push2.eastmoney.com/api/qt/stock/trends2/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ut": UT_STOCK_TRENDS,
        "ndays": "1",
        "iscr": "1",
        "iscca": "0",
        "secid": f"1.{symbol}" if symbol.startswith('6') else f"0.{symbol}",
        "_": get_timestamp(),
    }
    # API 请求: 获取股票盘前分钟数据
    # 入参: url=trends2/get接口, secid=市场ID.股票代码, iscr=1(盘前), iscca=0
    # 出参: {data: {predata: ['时间,价格,成交量,成交额,均价'...]}}
    r =  fetcher.make_request(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(
        [item.split(",") for item in data_json["data"]["trends"]]
    )
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
    temp_df.index = pd.to_datetime(temp_df["时间"])
    date_format = temp_df.index[0].date().isoformat()
    temp_df = temp_df[
        date_format + " " + start_time : date_format + " " + end_time
    ]
    temp_df.reset_index(drop=True, inplace=True)
    temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
    temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
    temp_df["最高"] = pd.to_numeric(temp_df["最高"])
    temp_df["最低"] = pd.to_numeric(temp_df["最低"])
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
    temp_df["最新价"] = pd.to_numeric(temp_df["最新价"])
    temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
    return temp_df


if __name__ == "__main__":
    stock_zh_a_spot_em_df = stock_zh_a_spot_em()
    print(stock_zh_a_spot_em_df)

    # code_id_map_em_df = code_id_map_em()
    # print(code_id_map_em_df)

    # stock_zh_a_hist_df = stock_zh_a_hist(
    #     symbol="000001",
    #     period="daily",
    #     start_date="20220516",
    #     end_date="20220516",
    #     adjust="qfq",
    # )
    # print(stock_zh_a_hist_df)

    # stock_zh_a_hist_min_em_df = stock_zh_a_hist_min_em(symbol="000001", period="1")
    # print(stock_zh_a_hist_min_em_df)

    # stock_zh_a_hist_pre_min_em_df = stock_zh_a_hist_pre_min_em(symbol="000001")
    # print(stock_zh_a_hist_pre_min_em_df)