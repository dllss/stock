#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
代理IP管理模块（单例模式）
=========================
这个模块负责管理和提供代理IP，用于数据爬取时避免被封禁。

什么是代理IP？
- 代理IP就像一个"中间人"，你通过它访问网站
- 网站看到的是代理IP，而不是你的真实IP
- 可以避免频繁访问被封禁

什么是单例模式？
- 单例模式确保一个类只有一个实例
- 无论调用多少次，都返回同一个对象
- 好处：节省内存，数据只加载一次
- 例如：代理列表只需要从文件读取一次，全系统共享使用

主要功能：
1. 从配置文件读取代理IP列表
2. 随机选择一个代理IP使用
3. 支持带认证的代理（用户名:密码@IP:端口）
"""

# ==================== 导入必需的库 ====================
import os.path  # 用于处理文件路径
import sys  # 用于修改系统路径
import random  # 用于随机选择代理
from instock.lib.singleton_type import singleton_type  # 单例模式的实现

# ==================== 路径配置 ====================
# 在项目运行时，临时将项目路径添加到环境变量
# os.path.dirname()：获取目录路径
# __file__：当前文件的路径
cpath_current = os.path.dirname(os.path.dirname(__file__))  # 当前模块所在目录的上级目录
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))  # 项目根目录
sys.path.append(cpath)  # 将项目根目录添加到Python搜索路径

# 代理配置文件路径：instock/config/proxy.txt
proxy_filename = os.path.join(cpath_current, 'config', 'proxy.txt')

__author__ = 'myh '
__date__ = '2025/1/6 '


# ==================== 代理管理类（单例模式）====================
class proxys(metaclass=singleton_type):
    """
    代理IP管理类
    
    使用单例模式，确保整个程序只有一个代理管理器实例
    
    属性：
        data (list): 代理IP列表，每个元素是一个代理字符串
        
    代理格式：
        - 普通代理：IP:端口，如"127.0.0.1:7860"
        - 认证代理：用户名:密码@IP:端口，如"user:pass@127.0.0.1:7860"
        
    配置文件格式（proxy.txt）：
        127.0.0.1:7860
        52.13.248.29:3128
        user:pass@35.178.104.4:80
        （每行一个代理）
    """
    
    def __init__(self):
        """
        初始化代理管理器，从配置文件加载代理列表
        
        执行流程：
            1. 打开proxy.txt文件
            2. 读取所有行
            3. 去除空行和重复项
            4. 存储到self.data列表中
            
        异常处理：
            如果文件不存在或读取失败，不会报错，只是不使用代理
        """
        try:
            # with语句：自动处理文件的打开和关闭
            with open(proxy_filename, "r") as file:
                # 列表推导式：从文件中读取并处理每一行
                # file.readlines()：读取所有行，返回列表
                # line.strip()：去除每行首尾的空白字符（空格、换行等）
                # if line.strip()：过滤掉空行
                # set()：去除重复项
                # list()：转换回列表
                self.data = list(set(line.strip() for line in file.readlines() if line.strip()))
        except Exception:
            # 如果文件不存在或读取失败，不做任何处理
            # 这样不使用代理也能正常运行
            pass

    def get_data(self):
        """
        获取所有代理IP列表
        
        返回值：
            list: 代理IP字符串列表
            
        使用示例：
            proxy_mgr = proxys()
            all_proxies = proxy_mgr.get_data()
            print(f"共有{len(all_proxies)}个代理")
        """
        return self.data

    def get_proxies(self):
        """
        随机获取一个可用的代理IP（字典格式）
        
        返回值：
            dict: 代理配置字典，格式为{"http": "代理地址", "https": "代理地址"}
                 如果没有可用代理，返回None
                 
        字典格式说明：
            这是requests库要求的代理格式
            - "http": HTTP协议使用的代理
            - "https": HTTPS协议使用的代理
            - 通常两个值相同
            
        随机选择的原因：
            - 分散请求到不同的代理IP
            - 降低单个代理被封禁的风险
            - 提高系统的稳定性
            
        使用示例：
            import requests
            
            proxy_mgr = proxys()
            proxy_dict = proxy_mgr.get_proxies()
            
            if proxy_dict:
                # 使用代理访问网站
                response = requests.get("https://data.eastmoney.com", proxies=proxy_dict)
            else:
                # 不使用代理直接访问
                response = requests.get("https://data.eastmoney.com")
        """
        # 检查是否有可用代理
        if self.data is None or len(self.data) == 0:
            return None  # 没有代理，返回None

        # random.choice()：从列表中随机选择一个元素
        proxy = random.choice(self.data)
        
        # 返回requests库要求的代理字典格式
        return {"http": proxy, "https": proxy}

"""
    def get_proxies(self):
        if self.data is None:
            return None

        while len(self.data) > 0:
            proxy = random.choice(self.data)
            if https_validator(proxy):
                return {"http": proxy, "https": proxy}
            self.data.remove(proxy)

        return None


from requests import head
def https_validator(proxy):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
               'Accept': '*/*',
               'Connection': 'keep-alive',
               'Accept-Language': 'zh-CN,zh;q=0.8'}
    proxies = {"http": f"{proxy}", "https": f"{proxy}"}
    try:
        r = head("https://data.eastmoney.com", headers=headers, proxies=proxies, timeout=3, verify=False)
        return True if r.status_code == 200 else False
    except Exception as e:
        return False
"""