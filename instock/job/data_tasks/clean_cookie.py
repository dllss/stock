#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cookie清理工具
==============
功能：从原始Cookie中删除追踪字段，只保留必要字段

使用方法：
    python clean_cookie.py
    
或者直接在Python中调用：
    from clean_cookie import clean_cookie
    cleaned = clean_cookie(raw_cookie)
"""

import re


def clean_cookie(cookie_str):
    """
    清理Cookie，删除追踪字段
    
    参数:
        cookie_str: 原始Cookie字符串
    
    返回:
        清理后的Cookie字符串
    """
    # 需要保留的必要字段
    necessary_fields = {
        'qgqp_b_id',   # 浏览器ID
        'st_nvi',      # 会话标识
        'st_si',       # 会话ID
        'st_asi',      # 附加会话信息
        'st_pvi',      # 页面访问ID
        'st_sp',       # 会话开始时间
        'st_inirUrl',  # 初始URL
        'st_sn',       # 会话序号
        'st_psi',      # 页面会话ID
    }
    
    # 需要删除的追踪字段
    tracking_fields = {
        'nid18',              # 设备ID追踪
        'nid18_create_time',  # 设备创建时间
        'gviem',              # 访客ID追踪
        'gviem_create_time',  # 访客创建时间
        'fullscreengg',       # 屏幕状态追踪
        'fullscreengg2',      # 屏幕状态追踪
    }
    
    # 分割Cookie
    cookies = cookie_str.split('; ')
    cleaned = []
    
    for cookie in cookies:
        if '=' in cookie:
            key = cookie.split('=')[0].strip()
            # 只保留必要字段
            if key in necessary_fields:
                cleaned.append(cookie.strip())
        elif cookie.strip():  # 处理没有值的字段
            cleaned.append(cookie.strip())
    
    return '; '.join(cleaned)


def analyze_cookie(cookie_str):
    """
    分析Cookie，显示详细信息
    
    参数:
        cookie_str: Cookie字符串
    """
    print("=" * 20)
    print("Cookie 分析报告")
    print("=" * 20)
    
    cookies = cookie_str.split('; ')
    
    # 统计字段
    total_fields = len(cookies)
    tracking_count = 0
    necessary_count = 0
    
    tracking_fields_found = []
    necessary_fields_found = []
    
    for cookie in cookies:
        if '=' in cookie:
            key = cookie.split('=')[0].strip()
            
            # 检查是否是追踪字段
            if key.startswith('nid') or key.startswith('gviem') or key.startswith('fullscreen'):
                tracking_count += 1
                tracking_fields_found.append(cookie)
            else:
                necessary_count += 1
                necessary_fields_found.append(cookie)
    
    # 提取st_sn
    sn_match = re.search(r'st_sn=(\d+)', cookie_str)
    st_sn = int(sn_match.group(1)) if sn_match else 0
    
    # 输出分析结果
    print(f"\n📊 基本信息:")
    print(f"   总字段数: {total_fields}")
    print(f"   必要字段: {necessary_count}")
    print(f"   追踪字段: {tracking_count}")
    print(f"   st_sn值: {st_sn} ({'新会话' if st_sn < 10 else '旧会话'})")
    
    if tracking_count > 0:
        print(f"\n⚠️  发现追踪字段 (应该删除):")
        for field in tracking_fields_found:
            key = field.split('=')[0]
            value = field.split('=')[1][:20] + '...' if len(field) > 30 else field.split('=')[1]
            print(f"   - {key}: {value}")
    
    print(f"\n✅ 必要字段 (应该保留):")
    for field in necessary_fields_found[:5]:  # 只显示前5个
        key = field.split('=')[0]
        value = field.split('=')[1][:30] + '...' if len(field) > 40 else field.split('=')[1]
        print(f"   - {key}: {value}")
    if len(necessary_fields_found) > 5:
        print(f"   ... 还有 {len(necessary_fields_found) - 5} 个字段")
    
    # 健康评分
    score = 100
    if tracking_count > 0:
        score -= 30
    if st_sn > 20:
        score -= 40
    elif st_sn > 10:
        score -= 20
    
    print(f"\n🎯 健康评分: {score}/100")
    if score >= 80:
        print("   ✅ Cookie状态良好")
    elif score >= 50:
        print("   ⚠️  Cookie需要优化")
    else:
        print("   ❌ Cookie需要更换")
    
    print("\n" + "=" * 20)
    
    return score


if __name__ == '__main__':
    # 示例Cookie
    sample_cookie = "qgqp_b_id=27abdae562b0bfbba8a5940e55c1052c; st_nvi=E3LdmARg6yo5-8yNjYbgzfdc7; st_si=02918531279596; st_asi=delete; nid18=0cca8a925df2af21e27d1da02e0fc350; nid18_create_time=1778294829308; gviem=hl3Zy7y4_v6g3K6ea6N9h07b2; gviem_create_time=1778294829308; st_pvi=59511925326511; st_sp=2026-05-09%2001%3A03%3A38; st_inirUrl=https%3A%2F%2Fquote.eastmoney.com%2Fcenter%2Fgridlist.html; st_sn=2; st_psi=20260509105313241-113200301324-9799435753"
    
    print("原始Cookie:")
    print(sample_cookie)
    print()
    
    # 分析原始Cookie
    analyze_cookie(sample_cookie)
    
    # 清理Cookie
    cleaned = clean_cookie(sample_cookie)
    print("\n清理后的Cookie:")
    print(cleaned)
    print()
    
    # 分析清理后的Cookie
    analyze_cookie(cleaned)
    
    print("\n💡 提示:")
    print("   1. 复制上面的'清理后的Cookie'")
    print("   2. 粘贴到 instock/config/eastmoney_cookie.txt")
    print("   3. 重新运行任务")
