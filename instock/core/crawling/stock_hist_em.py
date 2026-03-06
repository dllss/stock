#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2022/6/19 15:26
Desc: 东方财富网-行情首页-沪深京 A 股
"""
import random
import time
import pandas as pd
import math
from instock.core.eastmoney_fetcher import eastmoney_fetcher, get_timestamp
from instock.core.file_cache import file_cached

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
        "secid": f"{code_id_dict[symbol]}.{symbol}",
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
            "secid": f"{code_id_dict[symbol]}.{symbol}",
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
            "secid": f"{code_id_dict[symbol]}.{symbol}",
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
        "secid": f"{code_id_dict[symbol]}.{symbol}",
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