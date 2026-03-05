#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import socket
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.util.connection import allowed_gai_family
from pathlib import Path
import time
import random
from instock.core.singleton_proxy import proxys

# 强制使用 IPv4，东方财富 API 的 IPv6 地址会返回空响应
import urllib3.util.connection
urllib3.util.connection.allowed_gai_family = lambda: socket.AF_INET

__author__ = 'myh '
__date__ = '2025/12/31 '

# 反爬虫延迟配置（秒）
REQUEST_DELAY_MIN = 4  # 最小延迟时间
REQUEST_DELAY_MAX = 7  # 最大延迟时间
RETRY_DELAY_MIN = 5    # 重试前最小延迟时间
RETRY_DELAY_MAX = 8    # 重试前最大延迟时间

def get_timestamp() -> str:
    """
    生成当前时间戳（毫秒）
    用作API请求的缓存破坏器参数
    """
    return str(int(time.time() * 1000))

class eastmoney_fetcher:
    """
    东方财富网数据获取器
    封装了Cookie管理、会话管理和请求发送功能
    """

    def __init__(self):
        """初始化获取器"""
        self.base_dir = os.path.dirname(os.path.dirname(__file__))
        self.session = self._create_session()
        self.proxies = proxys().get_proxies()

    def _get_cookie(self):
        """
        获取东方财富网的Cookie
        优先级：环境变量 > 文件 > 默认Cookie
        """
        # 1. 尝试从环境变量获取
        cookie = os.environ.get('EAST_MONEY_COOKIE')
        if cookie:
            # print("环境变量中的Cookie: 已设置")
            return cookie

        # 2. 尝试从文件获取
        cookie_file = Path(os.path.join(self.base_dir, 'config', 'eastmoney_cookie.txt'))
        if cookie_file.exists():
            with open(cookie_file, 'r') as f:
                cookie = f.read().strip()
            if cookie:
                # print("文件中的Cookie: 已设置")
                return cookie

        # 3. 默认Cookie（可能过期，仅作为备选）
        return 'st_si=78948464251292; st_psi=20260205091253851-119144370567-1089607836; st_pvi=07789985376191; st_sp=2026-02-05%2009%3A11%3A13; st_inirUrl=https%3A%2F%2Fxuangu.eastmoney.com%2FResult; st_sn=12; st_asi=20260205091253851-119144370567-1089607836-webznxg.dbssk.qxg-1'

    def _create_session(self):
        """创建并配置会话"""
        session = requests.Session()

        # 配置连接池
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "OPTIONS"]
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=50,  # 增加连接池大小
            pool_maxsize=50  # 增加连接池最大大小
        )

        # 为http和https请求添加适配器
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # 设置请求头
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Referrer-Policy': 'no-referrer, strict-origin-when-cross-origin',
        }
        session.headers.update(headers)
        # 设置Cookie
        session.cookies.update({'Cookie': self._get_cookie()})
        return session

    def make_request(self, url, params=None, retry=3, timeout=10):
        """
        发送请求
        :param url: 请求URL
        :param params: 请求参数
        :param retry: 重试次数
        :param timeout: 超时时间
        :return: 响应对象
        """
        for i in range(retry):
            try:
                # 记录请求开始
                start_time = time.time()
                print("\n" + "="*80)
                print(f"🌐 [GET请求] {url}")
                if params:
                    print(f"   📋 参数: {params}")
                
                response = self.session.get(
                    url,
                    proxies=self.proxies,
                    params=params,
                    timeout=timeout
                )
                response.raise_for_status()  # 检查HTTP错误
                
                # 检查响应内容
                if not response.text:
                    print(f"⚠️  警告: 响应内容为空 (状态码: {response.status_code})")
                    print(f"   可能原因: 反爬虫限制、IP被封、Cookie失效")
                    raise ValueError("响应内容为空")
                
                # 计算耗时
                elapsed = time.time() - start_time
                print(f"   ✅ 成功 | 耗时: {elapsed:.2f}秒 | 状态码: {response.status_code} | 内容大小: {len(response.text)}字节")
                
                # 请求成功后延迟，避免请求过快被反爬
                delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
                print(f"   ⏱️  延迟: {delay:.2f}秒（防反爬）")
                print("="*80)
                time.sleep(delay)
                
                return response
            except requests.exceptions.RequestException as e:
                error_msg = str(e).split('\n')[0][:100]  # 只取第一行，最多100字符
                print(f"   ❌ 失败 | {error_msg}")
                print(f"   🔄 重试: 第 {i + 1}/{retry} 次")
                if i < retry - 1:
                    # 指数退避：重试延迟随失败次数递增
                    # 第1次失败: 1-3秒, 第2次: 2-6秒, 第3次: 4-12秒...
                    backoff_factor = 2 ** i
                    retry_delay = random.uniform(
                        RETRY_DELAY_MIN * backoff_factor, 
                        RETRY_DELAY_MAX * backoff_factor
                    )
                    print(f"   ⏱️  延迟: {retry_delay:.2f}秒（第{i+1}次重试，指数退避）")
                    print("="*80)
                    time.sleep(retry_delay)
                else:
                    print("="*80)
                    raise

    def make_post_request(self, url, data=None, json=None, params=None, retry=3, timeout=60):
        """
        发送POST请求
        :param url: 请求URL
        :param data: 请求数据（表单形式）
        :param json: 请求数据（JSON形式）
        :param params: URL参数
        :param retry: 重试次数
        :param timeout: 超时时间
        :return: 响应对象
        """
        for i in range(retry):
            try:
                # 记录请求开始
                start_time = time.time()
                print("\n" + "="*80)
                print(f"🌐 [POST请求] {url}")
                if params:
                    key_params = {k: v for k, v in list(params.items())[:5]}
                    more = f" ...({len(params)-5}个更多参数)" if len(params) > 5 else ""
                    print(f"   📋 参数: {key_params}{more}")
                if data:
                    print(f"   📄 表单数据: {str(data)[:100]}...")
                if json:
                    print(f"   📄 JSON数据: {str(json)[:100]}...")
                
                response = self.session.post(
                    url,
                    proxies=self.proxies,
                    params=params,
                    data=data,
                    json=json,
                    timeout=timeout
                )
                response.raise_for_status()  # 检查HTTP错误
                
                # 检查响应内容
                if not response.text:
                    print(f"⚠️  警告: 响应内容为空 (状态码: {response.status_code})")
                    print(f"   可能原因: 反爬虫限制、IP被封、Cookie失效")
                    raise ValueError("响应内容为空")
                
                # 计算耗时
                elapsed = time.time() - start_time
                print(f"   ✅ 成功 | 耗时: {elapsed:.2f}秒 | 状态码: {response.status_code} | 内容大小: {len(response.text)}字节")
                
                # 请求成功后延迟，避免请求过快被反爬
                delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
                print(f"   ⏱️  延迟: {delay:.2f}秒（防反爬）")
                print("="*80)
                time.sleep(delay)
                
                return response
            except requests.exceptions.RequestException as e:
                error_msg = str(e).split('\n')[0][:100]
                print(f"   ❌ 失败 | {error_msg}")
                print(f"   🔄 重试: 第 {i + 1}/{retry} 次")
                if i < retry - 1:
                    # 指数退避：重试延迟随失败次数递增
                    # 第1次失败: 1-3秒, 第2次: 2-6秒, 第3次: 4-12秒...
                    backoff_factor = 2 ** i
                    retry_delay = random.uniform(
                        RETRY_DELAY_MIN * backoff_factor, 
                        RETRY_DELAY_MAX * backoff_factor
                    )
                    print(f"   ⏱️  延迟: {retry_delay:.2f}秒（第{i+1}次重试，指数退避）")
                    print("="*80)
                    time.sleep(retry_delay)
                else:
                    print("="*80)
                    raise

    def update_cookie(self, new_cookie):
        """
        更新Cookie
        :param new_cookie: 新的Cookie值
        """
        self.session.cookies.update({'Cookie': new_cookie})
