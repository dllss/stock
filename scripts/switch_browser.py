#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
浏览器切换工具
==============
功能：手动切换东方财富爬虫使用的浏览器配置

使用方法：
    1. 运行此脚本
    2. 按 S/s 切换到下一个浏览器
    3. 按 Q/q 退出

配置文件：instock/config/eastmoney_cookies.json
"""

import os
import sys
import json

# 添加项目路径
cpath_current = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, cpath_current)

from instock.core.eastmoney_fetcher import eastmoney_fetcher


def main():
    """主函数"""
    print("=" * 80)
    print("浏览器切换工具")
    print("=" * 80)
    
    # 创建获取器实例
    fetcher = eastmoney_fetcher()
    
    # 显示当前配置
    browsers = fetcher.browser_configs['browsers']
    current = fetcher.current_browser_key
    
    print(f"\n📋 可用的浏览器配置:")
    for i, (key, config) in enumerate(browsers.items(), 1):
        marker = " ← 当前" if key == current else ""
        print(f"   {i}. {config['name']} ({key}){marker}")
    
    print(f"\n💡 操作说明:")
    print(f"   - 按 S/s 切换到下一个浏览器")
    print(f"   - 按 Q/q 退出")
    print(f"   - 当前会话将立即生效")
    
    print("\n" + "=" * 80)
    
    while True:
        try:
            choice = input("\n请选择操作 [S/Q]: ").strip().lower()
            
            if choice == 'q':
                print("\n✅ 退出浏览器切换工具")
                
                # 保存当前浏览器到配置文件
                save_current_browser(fetcher.current_browser_key)
                break
            
            elif choice == 's':
                new_browser = fetcher.switch_browser()
                print(f"\n✅ 已切换到: {new_browser}")
                print(f"   提示: 下次启动任务时将自动使用此浏览器")
            
            else:
                print("⚠️  无效输入，请输入 S/s 或 Q/q")
        
        except KeyboardInterrupt:
            print("\n\n⚠️  用户中断")
            save_current_browser(fetcher.current_browser_key)
            break
        
        except Exception as e:
            print(f"\n❌ 发生错误: {e}")


def save_current_browser(browser_key):
    """
    保存当前浏览器选择到配置文件
    
    参数:
        browser_key: 浏览器键名
    """
    config_file = os.path.join(cpath_current, 'config', 'eastmoney_cookies.json')
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            configs = json.load(f)
        
        configs['current_browser'] = browser_key
        
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(configs, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 已保存浏览器选择: {browser_key}")
    
    except Exception as e:
        print(f"⚠️  保存配置失败: {e}")


if __name__ == '__main__':
    main()
