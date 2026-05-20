#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试代理是否真正隐藏了IP
"""
import requests

def test_ip_without_proxy():
    """不使用代理,查看真实IP"""
    try:
        response = requests.get('http://httpbin.org/ip', timeout=10)
        print(f"❌ 不使用代理 - 真实IP: {response.json()['origin']}")
        return response.json()['origin']
    except Exception as e:
        print(f"⚠️ 测试失败: {e}")
        return None

def test_ip_with_proxy(proxy_dict):
    """使用代理,查看服务器看到的IP"""
    try:
        response = requests.get(
            'http://httpbin.org/ip',
            proxies=proxy_dict,
            timeout=10
        )
        print(f"✅ 使用代理 {proxy_dict['http']} - 服务器看到IP: {response.json()['origin']}")
        return response.json()['origin']
    except Exception as e:
        print(f"❌ 使用代理 {proxy_dict['http']} - 失败: {e}")
        return None

if __name__ == '__main__':
    print("="*80)
    print("测试代理是否真正隐藏IP")
    print("="*80)
    
    # 1. 测试真实IP
    print("\n[步骤1] 获取真实IP...")
    real_ip = test_ip_without_proxy()
    
    # 2. 测试代理IP
    print("\n[步骤2] 测试代理IP...")
    from instock.core.singleton_proxy import proxys
    proxy_mgr = proxys()
    
    if not proxy_mgr.data:
        print("⚠️ proxy.txt 中没有配置代理")
    else:
        for proxy_addr in proxy_mgr.data[:3]:  # 测试前3个代理
            proxy_dict = {"http": proxy_addr, "https": proxy_addr}
            proxy_ip = test_ip_with_proxy(proxy_dict)
            
            # 3. 对比IP
            if real_ip and proxy_ip:
                if real_ip == proxy_ip:
                    print(f"   ⚠️ 警告: 代理未生效!服务器仍看到真实IP")
                else:
                    print(f"   ✅ 代理有效!IP已更换")
            print()
    
    print("="*80)
    print("测试完成")
    print("="*80)
