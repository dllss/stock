#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
import time
import random
import urllib3
from instock.core.singleton_proxy import proxys

# 禁用SSL证书验证警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

__author__ = 'myh '
__date__ = '2025/12/31 '

# 单例实例
_instance = None

class eastmoney_fetcher:
    """
    东方财富网数据获取器
    封装了Cookie管理、会话管理和请求发送功能
    """

    def __new__(cls):
        """单例模式"""
        global _instance
        if _instance is None:
            _instance = super().__new__(cls)
            _instance._initialized = False
        return _instance
    
    def __init__(self):
        """初始化获取器(只执行一次)"""
        if self._initialized:
            return
        
        self._initialized = True
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
            return cookie

        # 2. 尝试从文件获取
        cookie_file = Path(os.path.join(self.base_dir, 'config', 'eastmoney_cookie.txt'))
        if cookie_file.exists():
            try:
                with open(cookie_file, 'r', encoding='utf-8') as f:
                    cookie = f.read().strip()
                if cookie:
                    return cookie
            except Exception as e:
                print(f"⚠️ 读取Cookie文件失败: {e}")

        # 3. 默认Cookie（可能过期，仅作为备选）
        return 'st_si=78948464251292; st_psi=20260205091253851-119144370567-1089607836; st_pvi=07789985376191; st_sp=2026-02-05%2009%3A11%3A13; st_inirUrl=https%3A%2F%2Fxuangu.eastmoney.com%2FResult; st_sn=12; st_asi=20260205091253851-119144370567-1089607836-webznxg.dbssk.qxg-1'
    
    def switch_proxy(self):
        """
        切换到下一个代理IP
        
        返回值:
            str: 新切换的代理地址，如果没有可用代理返回None
        """
        new_proxy = proxys().switch_proxy()
        if new_proxy:
            # 更新当前代理配置
            self.proxies = {"http": new_proxy, "https": new_proxy}
            print(f"\n🔄 已切换到代理: {new_proxy}")
            return new_proxy
        return None
    
    def _load_cookies(self):
        """
        重新加载Cookie并更新session
        """
        new_cookie = self._get_cookie()
        self.session.headers.update({'Cookie': new_cookie})

    def _create_session(self):
        """
        创建并配置会话
        """
        session = requests.Session()

        # 配置连接池（禁用自动重试，交给上层make_request处理）
        retry_strategy = Retry(
            total=0,                    # 不自动重试
            backoff_factor=0,
            status_forcelist=[],        # 不对任何状态码重试
            allowed_methods=["HEAD", "GET", "POST", "OPTIONS"],
            raise_on_status=True        # 立即抛出异常
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=50,  # 增加连接池大小
            pool_maxsize=50  # 增加连接池最大大小
        )

        # 为http和https请求添加适配器
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 禁用SSL证书验证(代理环境下常出现证书问题)
        session.verify = False

        # 设置请求头
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            # 'Accept-Encoding': 'gzip, deflate, br, zstd', # 注意：不设置 Accept-Encoding，让 requests 自动处理压缩
            'Connection': 'keep-alive',
            # 'Cache-Control': 'no-cache',
            # 'Pragma': 'no-cache',
            # # Sec-Fetch 系列（浏览器安全策略，必须包含）
            # 'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
            # 'sec-ch-ua-mobile': '?0',
            # 'sec-ch-ua-platform': '"Windows"',
            # 'sec-fetch-dest': 'script',
            # 'sec-fetch-mode': 'no-cors',
            # 'sec-fetch-site': 'same-site',
        }
        session.headers.update(headers)
        # 设置Cookie到请求头
        session.headers.update({'Cookie': self._get_cookie()})
        
        return session

    def _get_verification_url(self, url):
        """
        根据API URL返回对应的验证网页URL
        :param url: API请求URL
        :return: 对应的验证网页URL
        """
        # 数据中心API（龙虎榜、大宗交易等）
        if 'datacenter-web.eastmoney.com' in url:
            return "https://data.eastmoney.com/"
        
        # ETF基金数据
        elif 'fund_etf' in url or 'etf' in url.lower():
            return "https://quote.eastmoney.com/center/gridlist.html#fund_etf"
        
        # 其他所有情况默认返回沪深A股页面
        else:
            return "https://quote.eastmoney.com/center/gridlist.html#hs_a_board"
    
    def _handle_request_error(self, error_msg, url, attempt, request_type="请求"):
        """
        处理请求错误的公共方法
        
        参数:
            error_msg: 错误信息
            url: 请求URL
            attempt: 当前重试次数
            request_type: 请求类型(GET/POST)
        """
        print(f"{'='*80}")
        print(f"⚠️  第 {attempt} 次{request_type}请求失败")
        print(f"❌ 错误信息: {error_msg}")
        
        # 获取对应的验证网页URL
        verification_url = self._get_verification_url(url)
        
        print(f"\n🔒 检测到服务器拒绝连接，可能被识别为爬虫！")
        print(f"\n💡 请执行以下操作解除机器人访问限制：")
        print(f"   1. 打开浏览器访问:")
        print(f"      {verification_url}")
        print(f"   2. 完成机器人验证（验证码、滑块等）")
        print(f"   3. 确保页面能正常显示数据")
        print(f"   4. 保持浏览器打开状态（不要关闭）")
        print(f"\n✅ 完成后，请输入 Y 或 y 继续重试...")
        print(f"💡 提示：输入 P/p 切换到下一个代理IP")
        print(f"💡 提示：输入 C/c + 空格 + Cookie 来更新Cookie")
        print(f"💡 提示：输入 Q/q 直接退出程序")
        
        # 等待用户输入Y/y确认
        while True:
            user_input = input("\n请输入 Y/y 继续: ").strip()
            
            # 检查是否退出
            if user_input.lower() in ['q', 'quit', 'exit']:
                print(f"\n❌ 用户取消操作，立即退出程序")
                import sys
                sys.exit(0)  # 直接终止整个进程
            
            # 检查是否切换代理 (P/p)
            if user_input.lower() == 'p':
                self.switch_proxy()
                print(f"✅ 自动开始重试...\n")
                return 'retry'  # 返回重试信号
            
            # 检查是否更新Cookie (C/c + Cookie)
            if user_input.lower().startswith('c '):
                new_cookie = user_input[2:].strip()  # 提取C后面的Cookie内容
                if new_cookie:
                    # 保存新的Cookie到配置文件
                    cookie_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'eastmoney_cookie.txt')
                    try:
                        with open(cookie_file, 'w', encoding='utf-8') as f:
                            f.write(new_cookie)
                        print(f"✅ Cookie已更新并保存到配置文件")
                        # 重新加载Cookie
                        self._load_cookies()
                        print(f"✅ Cookie已重新加载，开始重试...\n")
                        return 'retry'  # 返回重试信号
                    except Exception as e:
                        print(f"❌ 保存Cookie失败: {e}")
                        continue
                else:
                    print("❌ 无效的Cookie，请输入 C + 空格 + Cookie内容")
                    continue
            
            # 用户确认继续重试
            elif user_input.lower() in ['y', 'yes']:
                print(f"\n✅ 用户确认，开始第 {attempt + 1} 次重试...\n")
                return 'retry'  # 返回重试信号
            else:
                print("❌ 无效输入，请输入 Y/y 继续，Q/q 退出，P/p 切换代理，或 C + 空格 + Cookie更新Cookie")
    
    def make_request(self, url, params=None, timeout=10, show_detail_log=True):
        """
        发送GET请求(用户控制重试)
        :param url: 请求URL
        :param params: 请求参数
        :param timeout: 超时时间
        :param show_detail_log: 是否显示详细日志（默认True，分页获取时可设为False）
        :return: 响应对象
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                # 记录请求信息（只在show_detail_log=True时输出）
                if show_detail_log:
                    if params:
                        print(f"发起GET请求: {url} | 参数: {params}")
                    else:
                        print(f"发起GET请求: {url}")
                
                response = self.session.get(
                    url,
                    proxies=self.proxies,
                    params=params,
                    timeout=timeout
                )
                response.raise_for_status()  # 检查HTTP错误
                
                # 记录成功信息（只在show_detail_log=True时输出）
                if show_detail_log:
                    print(f"✅ 请求成功 (状态码: {response.status_code})")
                return response
            except requests.exceptions.RequestException as e:
                error_msg = str(e)
                logging.error(f"❌ 第{attempt}次请求失败: {error_msg}")
                self._handle_request_error(error_msg, url, attempt, "GET")

    def make_post_request(self, url, data=None, json=None, params=None, timeout=60):
        """
        发送POST请求(用户控制重试)
        :param url: 请求URL
        :param data: 请求数据（表单形式）
        :param json: 请求数据（JSON形式）
        :param params: URL参数
        :param timeout: 超时时间
        :return: 响应对象
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self.session.post(
                    url,
                    proxies=self.proxies,
                    params=params,
                    data=data,
                    json=json,
                    timeout=timeout
                )
                response.raise_for_status()  # 检查HTTP错误
                return response
            except requests.exceptions.RequestException as e:
                error_msg = str(e)
                self._handle_request_error(error_msg, url, attempt, "POST")
