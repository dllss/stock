#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试代理IP可用性
"""
import requests
from instock.core.singleton_proxy import proxys

def test_proxy(proxy_dict, timeout=10):
    """测试单个代理是否可用"""
    try:
        response = requests.get(
            'http://push2.eastmoney.com/api/qt/clist/get',
            proxies=proxy_dict,
            timeout=timeout,
            params={
                'fid': 'f62',
                'po': '1',
                'pz': '5',
                'pn': '1',
                'np': '1',
                'fltt': '2',
                'invt': '2',
                'ut': 'b2884a393a59ad64002292a3e90d46a5',
                'fs': 'm:0+t:6+f:!2',
                'fields': 'f12,f14,f2,f3'
            }
        )
        if response.status_code == 200:
            return True, f"成功 (状态码: {response.status_code})"
        else:
            return False, f"失败 (状态码: {response.status_code})"
    except Exception as e:
        return False, str(e)

def main():
    print("=" * 80)
    print("开始测试代理IP可用性...")
    print("=" * 80)
    
    proxy_mgr = proxys()
    all_proxies = proxy_mgr.get_data()
    
    if not all_proxies:
        print("❌ 没有可用的代理IP")
        return
    
    print(f"共有 {len(all_proxies)} 个代理需要测试\n")
    
    results = []
    for i, proxy_addr in enumerate(all_proxies, 1):
        proxy_dict = {"http": proxy_addr, "https": proxy_addr}
        print(f"[{i}/{len(all_proxies)}] 测试代理: {proxy_addr}")
        
        success, message = test_proxy(proxy_dict)
        
        if success:
            print(f"  ✅ {message}\n")
            results.append((proxy_addr, "可用"))
        else:
            print(f"  ❌ {message}\n")
            results.append((proxy_addr, f"不可用: {message[:50]}"))
    
    # 打印总结
    print("\n" + "=" * 80)
    print("测试结果总结:")
    print("=" * 80)
    
    available = [r for r in results if r[1] == "可用"]
    unavailable = [r for r in results if r[1] != "可用"]
    
    print(f"\n✅ 可用代理 ({len(available)} 个):")
    for addr, status in available:
        print(f"   - {addr}")
    
    print(f"\n❌ 不可用代理 ({len(unavailable)} 个):")
    for addr, status in unavailable:
        print(f"   - {addr}: {status}")
    
    if available:
        print(f"\n💡 建议: 将以下可用代理保留在 proxy.txt 中:")
        for addr, _ in available:
            print(f"   {addr}")
    else:
        print("\n⚠️ 警告: 所有代理都不可用,请更新 proxy.txt 文件")

if __name__ == "__main__":
    main()
