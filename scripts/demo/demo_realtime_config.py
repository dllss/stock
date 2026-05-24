#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
动态配置实时生效演示
展示修改配置文件后立即生效的效果
"""

import time
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from instock.config.delay_manager import get_DELAY_MIN, get_DELAY_MAX, get_delay_config, save_delay_config

CONFIG_FILE = ROOT_DIR / 'instock' / 'config' / 'delay_config.json'


def demo_realtime_update():
    """演示实时生效"""
    print("=" * 70)
    print("动态配置实时生效演示")
    print("=" * 70)
    
    # 步骤1: 读取当前配置
    print("\n📖 步骤1: 读取当前配置")
    config = get_delay_config()
    print(f"   DELAY_MIN = {config['DELAY_MIN']}")
    print(f"   DELAY_MAX = {config['DELAY_MAX']}")
    
    # 步骤2: 模拟修改配置
    print("\n✏️  步骤2: 修改配置文件 (DELAY_MIN: 9→12, DELAY_MAX: 15→20)")
    new_config = {
        "DELAY_MIN": 12,
        "DELAY_MAX": 20,
        "RETRY_DELAY_MIN": 5,
        "RETRY_DELAY_MAX": 8,
        "SPECIAL_REQUEST_DELAY_MIN": 12,
        "SPECIAL_REQUEST_DELAY_MAX": 18
    }
    save_delay_config(new_config)
    print(f"   ✅ 已保存到文件: {CONFIG_FILE}")
    
    # 步骤3: 立即读取新配置
    print("\n📖 步骤3: 立即读取新配置 (无需重启)")
    time.sleep(0.1)  # 确保文件写入完成
    new_config_read = get_delay_config()
    print(f"   DELAY_MIN = {new_config_read['DELAY_MIN']}")
    print(f"   DELAY_MAX = {new_config_read['DELAY_MAX']}")
    
    # 验证
    if new_config_read['DELAY_MIN'] == 12 and new_config_read['DELAY_MAX'] == 20:
        print("\n✅ 成功! 配置已实时更新,无需重启Python进程!")
    else:
        print("\n❌ 失败! 配置未更新")
    
    # 步骤4: 恢复原配置
    print("\n🔄 步骤4: 恢复原始配置")
    original_config = {
        "DELAY_MIN": 9,
        "DELAY_MAX": 15,
        "RETRY_DELAY_MIN": 5,
        "RETRY_DELAY_MAX": 8,
        "SPECIAL_REQUEST_DELAY_MIN": 12,
        "SPECIAL_REQUEST_DELAY_MAX": 18
    }
    save_delay_config(original_config)
    restored_config = get_delay_config()
    print(f"   DELAY_MIN = {restored_config['DELAY_MIN']}")
    print(f"   DELAY_MAX = {restored_config['DELAY_MAX']}")
    print("   ✅ 已恢复")
    
    print("\n" + "=" * 70)
    print("演示结束")
    print("=" * 70)


def demo_continuous_reading():
    """演示连续读取"""
    print("\n" + "=" * 70)
    print("连续读取测试 - 修改文件后立即反映")
    print("=" * 70)
    
    print("\n请按照以下步骤操作:")
    print("1. 打开文件: instock/config/delay_config.json")
    print("2. 修改 DELAY_MIN 的值")
    print("3. 保存文件")
    print("4. 按回车键,程序会读取最新值")
    print("5. 重复多次测试\n")
    
    for i in range(3):
        input(f"第{i+1}次读取 - 按回车继续...")
        config = get_delay_config()
        print(f"   当前配置: DELAY_MIN={config['DELAY_MIN']}, DELAY_MAX={config['DELAY_MAX']}")
    
    print("\n✅ 测试完成!")


if __name__ == '__main__':
    print("🚀 动态配置管理器 - 实时生效演示\n")
    
    # 运行自动演示
    demo_realtime_update()
    
    # 询问是否运行交互式演示
    print("\n是否运行交互式演示? (需要手动修改文件) y/n: ", end='')
    choice = input().strip().lower()
    if choice == 'y':
        demo_continuous_reading()
