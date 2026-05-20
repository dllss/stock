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
import json
import os
from datetime import datetime, timedelta
from instock.core.eastmoney_fetcher import eastmoney_fetcher
from instock.config.delay_manager import sleep_with_delay

__author__ = 'myh '
__date__ = '2025/12/31 '

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
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25,f26,f37,f38,f39,f40,f41,f45,f46,f48,f49,f57,f61,f100,f112,f113,f114,f115,f221",
        "_": "1623833739532",
    }
    r =  fetcher.make_request(url, params=params, show_detail_log=False)
    data_json = r.json()
    data = data_json["data"]["diff"]
    if not data:
        return pd.DataFrame()

    data_count = data_json["data"]["total"]
    page_count = math.ceil(data_count/page_size)
    import logging
    logging.info(f"总共{data_count}条记录，共{page_count}页，每页{page_size}条")
    print(f"📋 正在获取股票代码列表... (第1/{page_count}页)")
    while page_count > 1:
        # 添加随机延迟，控制每分钟请求数<10次（从配置文件读取）
        delay_time = sleep_with_delay('normal')
        page_current = page_current + 1
        params["pn"] = page_current
        r =  fetcher.make_request(url, params=params, show_detail_log=False)
        data_json = r.json()
        _data = data_json["data"]["diff"]
        data.extend(_data)
        page_count =page_count - 1
        print(f"📋 正在获取股票代码列表... (第{page_current}/{page_current + page_count - 1}页)")

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
:return: 股票和市场代码
:rtype: dict
"""
def code_id_map_em(use_cache: bool = True, cache_expire_hours: int = 720) -> dict:
    """
    获取股票代码与市场ID的映射表（增强版：包含股票名称、行业等信息）
    
    参数：
        use_cache: 是否使用缓存（默认True）
        cache_expire_hours: 缓存过期时间（小时），默认720小时（30天）
            - 股票代码变化频率极低，每月更新一次足够
            - 大幅减少API调用频率，提升性能
    
    返回：
        dict: {股票代码: {name, market_id, market_name, market_code, stock_type, industry, listing_date}} 的映射字典
    """
    # 缓存文件路径
    cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'cache')
    cache_file = os.path.join(cache_dir, 'stock_code_map.json')
    
    # 尝试从缓存读取
    if use_cache and os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                
            # 检查缓存是否过期
            cache_time = datetime.fromisoformat(cache_data.get('cache_time', ''))
            if datetime.now() - cache_time < timedelta(hours=cache_expire_hours):
                print(f"✅ 从缓存加载股票代码映射表（缓存时间: {cache_time.strftime('%Y-%m-%d %H:%M:%S')}）")
                code_map = cache_data.get('code_map', {})
                
                # 【向后兼容】如果是旧版本（简单数字映射），需要升级
                if code_map and isinstance(list(code_map.values())[0], int):
                    print(f"⚠️  检测到旧版本缓存格式，正在升级到新版本...")
                    return None  # 返回None触发重新获取
                
                return code_map
            else:
                print(f"⚠️  缓存已过期，重新获取...")
        except Exception as e:
            print(f"⚠️  读取缓存失败: {e}，重新获取...")
    
    # 从 API 获取数据（增强版：获取名称、行业、上市时间等信息）
    url = "http://80.push2.eastmoney.com/api/qt/clist/get"
    page_size = 50
    page_current = 1
    params = {
        "pn": page_current,
        "pz": page_size,
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:1 t:2,m:1 t:23",
        "fields": "f12,f14,f45,f57",  # f12:代码, f14:名称, f45:行业, f57:上市时间
        "_": "1623833739532",
    }
    r =  fetcher.make_request(url, params=params, show_detail_log=False)
    data_json = r.json()
    data = data_json["data"]["diff"]
    if not data:
        return dict()

    data_count = data_json["data"]["total"]
    page_count = math.ceil(data_count/page_size)
    print(f"📋 正在获取上海交易所代码... (第1/{page_count}页)")
    while page_count > 1:
        # 添加随机延迟，控制每分钟请求数<10次
        delay_time = sleep_with_delay('normal')
        page_current = page_current + 1
        params["pn"] = page_current
        r =  fetcher.make_request(url, params=params, show_detail_log=False)
        data_json = r.json()
        _data = data_json["data"]["diff"]
        data.extend(_data)
        page_count =page_count - 1
        print(f"📋 正在获取上海交易所代码... (第{page_current}/{page_current + page_count - 1}页) [延时: {delay_time:.2f}秒]")

    temp_df = pd.DataFrame(data)
    # 构造增强的股票信息对象
    code_id_dict = {}
    for _, row in temp_df.iterrows():
        code = row['f12']
        name = row.get('f14', '')
        industry = row.get('f45', '')
        listing_date = str(row.get('f57', '')) if row.get('f57') else ''
        
        # 判断股票类型
        if code.startswith('688'):
            stock_type = '科创板'
        elif code.startswith('689'):
            stock_type = '科创板'
        else:
            stock_type = '主板'
        
        code_id_dict[code] = {
            'name': name,
            'market_id': 1,
            'market_name': '上海证券交易所',
            'market_code': 'SH',
            'stock_type': stock_type,
            'industry': industry,
            'listing_date': listing_date
        }
    page_current = 1
    params = {
        "pn": page_current,
        "pz": page_size,
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:0 t:6,m:0 t:80",
        "fields": "f12,f14,f45,f57",  # f12:代码, f14:名称, f45:行业, f57:上市时间
        "_": "1623833739532",
    }
    r =  fetcher.make_request(url, params=params, show_detail_log=False)
    data_json = r.json()
    data = data_json["data"]["diff"]
    if not data:
        return dict()

    data_count = data_json["data"]["total"]
    page_count = math.ceil(data_count/page_size)
    print(f"📋 正在获取深圳交易所代码... (第1/{page_count}页)")
    while page_count > 1:
        # 添加随机延迟，控制每分钟请求数<10次
        delay_time = sleep_with_delay('normal')
        page_current = page_current + 1
        params["pn"] = page_current
        r =  fetcher.make_request(url, params=params, show_detail_log=False)
        data_json = r.json()
        _data = data_json["data"]["diff"]
        data.extend(_data)
        page_count =page_count - 1
        print(f"📋 正在获取深圳交易所代码... (第{page_current}/{page_current + page_count - 1}页) [延时: {delay_time:.2f}秒]")

    temp_df_sz = pd.DataFrame(data)
    # 添加深圳交易所股票信息
    for _, row in temp_df_sz.iterrows():
        code = row['f12']
        name = row.get('f14', '')
        industry = row.get('f45', '')
        listing_date = str(row.get('f57', '')) if row.get('f57') else ''
        
        # 判断股票类型
        if code.startswith('300') or code.startswith('301'):
            stock_type = '创业板'
        else:
            stock_type = '主板'
        
        code_id_dict[code] = {
            'name': name,
            'market_id': 0,
            'market_name': '深圳证券交易所',
            'market_code': 'SZ',
            'stock_type': stock_type,
            'industry': industry,
            'listing_date': listing_date
        }
    page_current = 1
    params = {
        "pn": page_current,
        "pz": page_size,
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:0 t:81 s:2048",
        "fields": "f12,f14,f45,f57",  # f12:代码, f14:名称, f45:行业, f57:上市时间
        "_": "1623833739532",
    }
    r =  fetcher.make_request(url, params=params, show_detail_log=False)
    data_json = r.json()
    data = data_json["data"]["diff"]
    if not data:
        return dict()

    data_count = data_json["data"]["total"]
    page_count = math.ceil(data_count/page_size)
    print(f"📋 正在获取北京交易所代码... (第1/{page_count}页)")
    while page_count > 1:
        # 添加随机延迟，控制每分钟请求数<10次
        delay_time = sleep_with_delay('normal')
        page_current = page_current + 1
        params["pn"] = page_current
        r =  fetcher.make_request(url, params=params, show_detail_log=False)
        data_json = r.json()
        _data = data_json["data"]["diff"]
        data.extend(_data)
        page_count =page_count - 1
        print(f"📋 正在获取北京交易所代码... (第{page_current}/{page_current + page_count - 1}页) [延时: {delay_time:.2f}秒]")

    temp_df_bj = pd.DataFrame(data)
    # 添加北京交易所股票信息
    for _, row in temp_df_bj.iterrows():
        code = row['f12']
        name = row.get('f14', '')
        industry = row.get('f45', '')
        listing_date = str(row.get('f57', '')) if row.get('f57') else ''
        
        code_id_dict[code] = {
            'name': name,
            'market_id': 2,
            'market_name': '北京证券交易所',
            'market_code': 'BJ',
            'stock_type': '北交所',
            'industry': industry,
            'listing_date': listing_date
        }
    
    # 保存到缓存文件
    try:
        # 确保缓存目录存在
        os.makedirs(cache_dir, exist_ok=True)
        
        # 计算统计信息
        sh_count = sum(1 for v in code_id_dict.values() if v['market_id'] == 1)
        sz_count = sum(1 for v in code_id_dict.values() if v['market_id'] == 0)
        bj_count = sum(1 for v in code_id_dict.values() if v['market_id'] == 2)
        
        # 按类型统计
        type_count = {}
        for v in code_id_dict.values():
            stock_type = v['stock_type']
            type_count[stock_type] = type_count.get(stock_type, 0) + 1
        
        cache_data = {
            'cache_time': datetime.now().isoformat(),
            'cache_version': '2.1',
            'code_map': code_id_dict,
            'statistics': {
                'total_count': len(code_id_dict),
                'by_exchange': {
                    'shanghai': {'count': sh_count, 'market_id': 1, 'market_code': 'SH'},
                    'shenzhen': {'count': sz_count, 'market_id': 0, 'market_code': 'SZ'},
                    'beijing': {'count': bj_count, 'market_id': 2, 'market_code': 'BJ'}
                },
                'by_type': type_count
            },
            'metadata': {
                'api_source': '东方财富网',
                'data_fields': ['f12: 股票代码', 'f14: 股票名称', 'f45: 所处行业', 'f57: 上市时间'],
                'update_frequency': '每日更新'
            }
        }
        
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 股票代码映射表已缓存到: {cache_file}")
        print(f"   共 {len(code_id_dict)} 只股票 (上海:{sh_count}, 深圳:{sz_count}, 北京:{bj_count})")
    except Exception as e:
        print(f"⚠️  保存缓存失败: {e}")
    
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
    # 【兼容处理】支持新旧两种格式
    if isinstance(code_id_dict.get(symbol), dict):
        market_id = code_id_dict[symbol]['market_id']
    else:
        market_id = code_id_dict[symbol]  # 旧格式：直接是数字
    
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"{market_id}.{symbol}",
        "beg": start_date,
        "end": end_date,
        "_": "1623766962675",
    }
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
    # 【兼容处理】支持新旧两种格式
    if isinstance(code_id_dict.get(symbol), dict):
        market_id = code_id_dict[symbol]['market_id']
    else:
        market_id = code_id_dict[symbol]  # 旧格式：直接是数字
    
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
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "ndays": "5",
            "iscr": "0",
            "secid": f"{market_id}.{symbol}",
            "_": "1623766962675",
        }
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
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "klt": period,
            "fqt": adjust_map[adjust],
            "secid": f"{market_id}.{symbol}",
            "beg": "0",
            "end": "20500000",
            "_": "1630930917857",
        }
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
    # 【兼容处理】支持新旧两种格式
    if isinstance(code_id_dict.get(symbol), dict):
        market_id = code_id_dict[symbol]['market_id']
    else:
        market_id = code_id_dict[symbol]  # 旧格式：直接是数字
    
    url = "https://push2.eastmoney.com/api/qt/stock/trends2/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "ndays": "1",
        "iscr": "1",
        "iscca": "0",
        "secid": f"{market_id}.{symbol}",
        "_": "1623766962675",
    }
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

    code_id_map_em_df = code_id_map_em()
    print(code_id_map_em_df)

    stock_zh_a_hist_df = stock_zh_a_hist(
        symbol="000001",
        period="daily",
        start_date="20220516",
        end_date="20220722",
        adjust="hfq",
    )
    print(stock_zh_a_hist_df)

    stock_zh_a_hist_min_em_df = stock_zh_a_hist_min_em(symbol="000001", period="1")
    print(stock_zh_a_hist_min_em_df)

    stock_zh_a_hist_pre_min_em_df = stock_zh_a_hist_pre_min_em(symbol="000001")
    print(stock_zh_a_hist_pre_min_em_df)

    stock_zh_a_spot_em_df = stock_zh_a_spot_em()
    print(stock_zh_a_spot_em_df)

    stock_zh_a_hist_min_em_df = stock_zh_a_hist_min_em(
        symbol="000001", period='1'
    )
    print(stock_zh_a_hist_min_em_df)

    stock_zh_a_hist_df = stock_zh_a_hist(
        symbol="000001",
        period="daily",
        start_date="20170301",
        end_date="20211115",
        adjust="hfq",
    )
    print(stock_zh_a_hist_df)

