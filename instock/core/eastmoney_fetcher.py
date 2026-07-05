#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
import time
import urllib3
from instock.core.singleton_proxy import proxys

# 禁用SSL证书验证警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

__author__ = 'myh'
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

        # 用户操作计数器
        self.user_action_count = {
            'Y': 0,  # 确认操作次数
            'y': 0,  # 确认操作次数（小写）
            'R': 0,  # 重置IP操作次数
            'r': 0,  # 重置IP操作次数（小写）
        }

        # API请求计数器（用于跟踪距离上一次确认经过了多少次请求）
        self.api_request_count_since_last_confirm = 0  # 距离上一次确认的API请求数
        self.total_api_requests = 0  # 总API请求数
        self.last_confirm_time = None  # 上一次确认的时间戳
        self.session_start_time = time.time()  # 会话开始时间

        # 用户操作日志文件
        self.action_log_file = os.path.join(self.base_dir, 'logs', 'user_actions.log')
        # 确保logs目录存在
        os.makedirs(os.path.dirname(self.action_log_file), exist_ok=True)

        # 注册退出处理钩子
        import atexit

        atexit.register(self.print_final_stats)

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
        cookie_file = Path(
            os.path.join(self.base_dir, 'config', 'eastmoney_cookie.txt')
        )
        if cookie_file.exists():
            try:
                with open(cookie_file, 'r', encoding='utf-8') as f:
                    cookie = f.read().strip()
                if cookie:
                    return cookie
            except Exception as e:
                print(f'⚠️ 读取Cookie文件失败: {e}')

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
            self.proxies = {'http': new_proxy, 'https': new_proxy}
            print(f'\n🔄 已切换到代理: {new_proxy}')
            return new_proxy
        return None

    def log_user_action(self, action):
        """
        记录用户操作并打印日志

        参数:
            action: 用户操作 (Y/y/R/r)
        """
        if action in self.user_action_count:
            self.user_action_count[action] += 1

            # 控制台日志
            print(f'🎯 用户操作: {action} (总计: {self.user_action_count[action]}次)')

            # 实时统计
            totalY = self.user_action_count['Y'] + self.user_action_count['y']
            totalR = self.user_action_count['R'] + self.user_action_count['r']

            # 如果是确认操作（Y/y），打印距离上一次确认的信息
            if action in ['Y', 'y']:
                current_time = time.time()

                if self.last_confirm_time is not None:
                    # 计算距离上一次确认经过的时间
                    time_since_last_confirm = current_time - self.last_confirm_time
                    time_str = self._format_time_duration(time_since_last_confirm)

                    # 打印详细信息
                    print(f'  ⏱️  距离上一次确认: {time_str}')
                    print(
                        f'  📡 距离上一次确认经过的API请求: {self.api_request_count_since_last_confirm}次'
                    )
                else:
                    # 第一次确认
                    session_duration = current_time - self.session_start_time
                    time_str = self._format_time_duration(session_duration)
                    print(f'  ⏱️  会话开始到第一次确认: {time_str}')
                    print(
                        f'  📡 会话开始到第一次确认经过的API请求: {self.api_request_count_since_last_confirm}次'
                    )

                # 重置计数器
                self.api_request_count_since_last_confirm = 0
                self.last_confirm_time = current_time

            if totalY > 0 or totalR > 0:
                print(
                    f'📊 用户操作统计 - 确认操作(Y/y): {totalY}次, 重置IP操作(R/r): {totalR}次'
                )

            # 写入日志文件
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

            # 构建详细的日志条目
            log_entry = f'[{timestamp}] 用户操作: {action}, 累计确认: {totalY}次, 累计重置IP: {totalR}次'

            if action in ['Y', 'y']:
                if self.last_confirm_time is not None:
                    log_entry += f', 距离上一次确认的API请求: {self.api_request_count_since_last_confirm}次'

            log_entry += '\n'

            try:
                with open(self.action_log_file, 'a', encoding='utf-8') as f:
                    f.write(log_entry)
            except Exception as e:
                print(f'⚠️ 写入操作日志失败: {e}')

    def _format_time_duration(self, seconds):
        """
        将秒数转换为易读的时间格式

        参数:
            seconds: 秒数（浮点数）

        返回:
            格式化的时间字符串（例如：1小时30分钟15秒，或45分钟，或30秒）
        """
        seconds = int(seconds)

        if seconds < 60:
            return f'{seconds}秒'
        elif seconds < 3600:
            minutes = seconds // 60
            secs = seconds % 60
            if secs > 0:
                return f'{minutes}分钟{secs}秒'
            else:
                return f'{minutes}分钟'
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            secs = seconds % 60

            time_str = f'{hours}小时'
            if minutes > 0:
                time_str += f'{minutes}分钟'
            if secs > 0:
                time_str += f'{secs}秒'

            return time_str

    def print_final_stats(self):
        """
        打印最终的用户操作统计信息
        """
        totalY = self.user_action_count['Y'] + self.user_action_count['y']
        totalR = self.user_action_count['R'] + self.user_action_count['r']
        total_operations = totalY + totalR

        if total_operations > 0:
            final_stats = f"""
            📊 === 用户操作最终统计 ===
            🔹 确认操作(Y/y): {totalY}次
            🔹 重置IP操作(R/r): {totalR}次  
            🔹 总操作次数: {total_operations}次
            📊 === 统计结束 ===
            """
            print(final_stats)

        # 写入最终统计到日志文件
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        final_log = f'\n[{timestamp}] === 会话结束统计 ===\n'
        final_log += f'确认操作(Y/y): {totalY}次\n'
        final_log += f'重置IP操作(R/r): {totalR}次\n'
        final_log += f'总操作次数: {total_operations}次\n'
        final_log += '=== 会话结束 ===\n\n'

        try:
            with open(self.action_log_file, 'a', encoding='utf-8') as f:
                f.write(final_log)
        except Exception as e:
            print(f'⚠️ 写入最终统计失败: {e}')

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
            total=0,  # 不自动重试
            backoff_factor=0,
            status_forcelist=[],  # 不对任何状态码重试
            allowed_methods=['HEAD', 'GET', 'POST', 'OPTIONS'],
            raise_on_status=True,  # 立即抛出异常
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=50,  # 增加连接池大小
            pool_maxsize=50,  # 增加连接池最大大小
        )

        # 为http和https请求添加适配器
        session.mount('http://', adapter)
        session.mount('https://', adapter)

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
            return 'https://data.eastmoney.com/'

        # ETF基金数据
        elif 'fund_etf' in url or 'etf' in url.lower():
            return 'https://quote.eastmoney.com/center/gridlist.html#fund_etf'

        # 其他所有情况默认返回沪深A股页面
        else:
            return 'https://quote.eastmoney.com/center/gridlist.html#hs_a_board'

    def _handle_request_error(self, error_msg, url, attempt, request_type='请求'):
        """
        处理请求错误的公共方法

        参数:
            error_msg: 错误信息
            url: 请求URL
            attempt: 当前重试次数
            request_type: 请求类型(GET/POST)
        """
        print(f'{"=" * 80}')
        print(f'⚠️  第 {attempt} 次{request_type}请求失败')
        print(f'❌ 错误信息: {error_msg}')

        # 获取对应的验证网页URL
        verification_url = self._get_verification_url(url)

        print('\n🔒 检测到服务器拒绝连接，可能被识别为爬虫！')
        print('\n💡 请执行以下操作解除机器人访问限制：')
        print('   1. 打开浏览器访问:')
        print(f'      {verification_url}')
        print('   2. 完成机器人验证（验证码、滑块等）')
        print('   3. 确保页面能正常显示数据')
        print('   4. 保持浏览器打开状态（不要关闭）')
        print('\n✅ 完成后，请输入 Y 或 y 继续重试...')
        print('💡 提示：输入 R/r 重置IP地址并重新拉取数据')
        print('💡 提示：输入 P/p 切换到下一个代理IP')
        print('💡 提示：输入 C/c + 空格 + Cookie 来更新Cookie')
        print('💡 提示：输入 Q/q 直接退出程序')

        # 等待用户输入
        while True:
            user_input = input('\n请输入操作 (Y/y/R/r/P/p/C/c/Q/q): ').strip()

            # 检查是否退出
            if user_input.lower() in ['q', 'quit', 'exit']:
                print('\n❌ 用户取消操作，立即退出程序')
                # 打印最终统计
                self.print_final_stats()
                import sys

                sys.exit(0)  # 直接终止整个进程

            # 检查是否重置IP (R/r)
            elif user_input.lower() in ['r']:
                print('\n🔄 用户选择重置IP地址')
                self.log_user_action('R')
                self.switch_proxy()
                print('✅ IP重置完成，开始重新拉取数据...\n')
                return 'retry'  # 返回重试信号

            # 检查是否切换代理 (P/p)
            elif user_input.lower() == 'p':
                self.log_user_action('P')  # 记录P操作
                self.switch_proxy()
                print('✅ 自动开始重试...\n')
                return 'retry'  # 返回重试信号

            # 检查是否更新Cookie (C/c + Cookie)
            elif user_input.lower().startswith('c '):
                self.log_user_action('C')  # 记录C操作
                new_cookie = user_input[2:].strip()  # 提取C后面的Cookie内容
                if new_cookie:
                    # 保存新的Cookie到配置文件
                    cookie_file = os.path.join(
                        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        'config',
                        'eastmoney_cookie.txt',
                    )
                    try:
                        with open(cookie_file, 'w', encoding='utf-8') as f:
                            f.write(new_cookie)
                        print('✅ Cookie已更新并保存到配置文件')
                        # 重新加载Cookie
                        self._load_cookies()
                        print('✅ Cookie已重新加载，开始重试...\n')
                        return 'retry'  # 返回重试信号
                    except Exception as e:
                        print(f'❌ 保存Cookie失败: {e}')
                        continue
                else:
                    print('❌ 无效的Cookie，请输入 C + 空格 + Cookie内容')
                    continue

            # 用户确认继续重试
            elif user_input.lower() in ['y', 'yes']:
                self.log_user_action('Y')  # 记录Y操作
                print(f'\n✅ 用户确认，开始第 {attempt + 1} 次重试...\n')
                return 'retry'  # 返回重试信号
            else:
                print('❌ 无效输入，请输入:')
                print('  Y/y - 继续重试')
                print('  R/r - 重置IP地址')
                print('  P/p - 切换代理')
                print('  C + 空格 + Cookie - 更新Cookie')
                print('  Q/q - 退出程序')

    def make_request(self, url, params=None, timeout=10, show_detail_log=True):
        """
        发送GET请求(用户控制重试)
        :param url: 请求URL
        :param params: 请求参数
        :param timeout: 超时时间
        :param show_detail_log: 是否显示详细日志（默认True，分页获取时可设为False）
        :return: 响应对象
        """
        # 增加API请求计数器
        self.api_request_count_since_last_confirm += 1
        self.total_api_requests += 1

        attempt = 0
        while True:
            attempt += 1
            try:
                # 记录请求信息（只在show_detail_log=True时输出）
                if show_detail_log:
                    if params:
                        print(f'发起GET请求: {url} | 参数: {params}')
                    else:
                        print(f'发起GET请求: {url}')

                response = self.session.get(
                    url, proxies=self.proxies, params=params, timeout=timeout
                )
                response.raise_for_status()  # 检查HTTP错误

                # 记录成功信息（只在show_detail_log=True时输出）
                if show_detail_log:
                    print(f'✅ 请求成功 (状态码: {response.status_code})')
                return response
            except requests.exceptions.RequestException as e:
                error_msg = str(e)
                logging.error(f'❌ 第{attempt}次请求失败: {error_msg}')
                self._handle_request_error(error_msg, url, attempt, 'GET')

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
        # 增加API请求计数器
        self.api_request_count_since_last_confirm += 1
        self.total_api_requests += 1

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
                    timeout=timeout,
                )
                response.raise_for_status()  # 检查HTTP错误
                return response
            except requests.exceptions.RequestException as e:
                error_msg = str(e)
                self._handle_request_error(error_msg, url, attempt, 'POST')
