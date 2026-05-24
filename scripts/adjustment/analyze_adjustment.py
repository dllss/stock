#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""分析唯特偶的除权情况"""

# 已知数据
price_before = 133.91  # 2026-05-20 收盘价（除权前）
price_after = 84.84    # 2026-05-21 开盘价（除权后）
change_rate_reported = -7.79  # 数据库中报告的涨跌幅

print("=" * 80)
print("唯特偶 (301319) 除权分析")
print("=" * 80)
print(f"除权前价格 (2026-05-20): {price_before}")
print(f"除权后价格 (2026-05-21): {price_after}")
print(f"数据库报告的涨跌幅: {change_rate_reported}%")
print()

# 计算实际价格变化
actual_change = (price_after - price_before) / price_before * 100
print(f"实际价格变化: {actual_change:.2f}%")
print()

# 如果是复权数据，应该用前一天的复权价来计算涨跌幅
# 假设2026-05-20的价格也应该是复权后的
# 那么复权因子 = 84.84 / 133.91 = 0.6336
adjustment_factor = price_after / price_before
print(f"推测的复权因子: {adjustment_factor:.4f}")
print()

# 验证：如果2026-05-20也用这个复权因子调整
adjusted_price_0520 = price_before * adjustment_factor
print(f"如果2026-05-20也复权: {adjusted_price_0520:.2f}")
print(f"这样2026-05-21的涨跌幅就是: {change_rate_reported}%")
print()

# 反向推算：如果change_rate=-7.79%是正确的，那么前一天的复权价应该是多少
# change_rate = (今日价 - 昨日复权价) / 昨日复权价 * 100
# -7.79 = (84.84 - X) / X * 100
# X = 84.84 / (1 - 0.0779)
expected_prev_price = price_after / (1 + change_rate_reported / 100)
print(f"根据change_rate反推的前一日复权价: {expected_prev_price:.2f}")
print()

# 检查是否匹配
if abs(expected_prev_price - adjusted_price_0520) < 0.01:
    print("✅ 验证通过！2026-05-21的数据确实是复权后的")
    print(f"   复权因子约为: {adjustment_factor:.4f} (即每1股变成 {1/adjustment_factor:.2f} 股)")
else:
    print("❌ 验证失败，数据可能有问题")
    
print()
print("=" * 80)
print("结论分析")
print("=" * 80)
print("1. cn_stock_spot 表中混合了复权和未复权数据")
print("2. 2026-05-20 及之前：未复权价格")
print("3. 2026-05-21 及之后：复权后价格")
print("4. 这导致回测计算出现错误")
