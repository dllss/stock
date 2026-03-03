#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
K线形态识别模块（第四层核心）
==============================
这个模块负责识别61种经典K线形态，帮助判断买卖时机。

什么是K线形态？
- K线：一根蜡烛图，包含开盘、最高、最低、收盘价
- 形态：多根K线组合形成的图案
- 经典形态：经过市场验证的有效形态

61种经典K线形态包括：
1. 看涨形态（买入信号）：
   - 锤头（Hammer）
   - 倒锤头（Inverted Hammer）
   - 晨星（Morning Star）
   - 刺透形态（Piercing Pattern）
   - 三个白兵（Three White Soldiers）
   - 看涨吞没（Bullish Engulfing）
   - ...

2. 看跌形态（卖出信号）：
   - 上吊线（Hanging Man）
   - 射击之星（Shooting Star）
   - 暮星（Evening Star）
   - 乌云盖顶（Dark Cloud Cover）
   - 三只乌鸦（Three Black Crows）
   - 看跌吞没（Bearish Engulfing）
   - ...

3. 中性形态（观望信号）：
   - 十字星（Doji）
   - 纺锤线（Spinning Top）
   - ...

形态识别结果：
- 正值（+100）：看涨形态，买入信号
- 负值（-100）：看跌形态，卖出信号
- 零值（0）：没有识别到该形态

使用TA-Lib识别：
- TA-Lib内置61种形态识别函数
- 准确可靠，经过验证
- 一次性识别所有形态

应用场景：
- 技术分析：判断买卖时机
- 选股：筛选出现特定形态的股票
- 回测：验证形态的有效性
- 实盘：辅助交易决策

注意事项：
- 形态识别是辅助工具，不是绝对信号
- 需要结合趋势、指标、成交量综合判断
- 不同市场环境，形态有效性不同
- 建议回测验证后使用
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录

__author__ = 'myh '
__date__ = '2023/3/24 '


# ==================== 识别所有K线形态 ====================

"""
识别股票的所有K线形态
参数说明：
data (DataFrame): 历史K线数据
必须包含：
- date：日期
- open：开盘价
- high：最高价
- low：最低价
- close：收盘价
stock_column (dict): 形态定义字典
- 键：形态英文名，如'CDL2CROWS'
- 值：字典，包含'func'（TA-Lib函数）
- 从tablestructure.py中获取
end_date (str, 可选): 截止日期
- 格式："YYYY-MM-DD"
- 只识别到这个日期
threshold (int): 返回数据的行数
- 默认120：返回最后120天
calc_threshold (int, 可选): 计算数据的行数
- None：使用全部数据
- 指定值：只用最后N天
返回值：
DataFrame: 包含所有形态识别结果
- 原有列：date, open, high, low, close
- 新增列：61种形态的识别结果
形态识别值：
- 0：没有识别到该形态
- +100：识别到看涨形态
- -100：识别到看跌形态
执行流程：
1. 数据筛选（根据日期）
2. 遍历所有形态
3. 调用TA-Lib函数识别
4. 存储识别结果
5. 返回DataFrame
TA-Lib形态识别函数：
每个函数接收OHLC（开高低收）四个数组
返回一个数组，每个值表示该天的形态识别结果
使用示例：
# stock_column定义形态
patterns = {
'CDL2CROWS': {'func': tl.CDL2CROWS},  # 两只乌鸦
'CDLHAMMER': {'func': tl.CDLHAMMER},  # 锤头
...
}
# 识别形态
result = get_pattern_recognitions(hist_data, patterns)
# 查看结果
print(result[['date', 'CDL2CROWS', 'CDLHAMMER']].tail())
"""
def get_pattern_recognitions(data, stock_column, end_date=None, threshold=120, calc_threshold=None):
    # ==================== 步骤1: 数据筛选 ====================
    isCopy = False  # 标记是否需要复制
    
    # 如果指定了截止日期，筛选数据
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask]
        isCopy = True
    
    # 如果指定了计算阈值，只用最后N天
    if calc_threshold is not None:
        data = data.tail(n=calc_threshold)
        isCopy = True
    
    # 如果进行了筛选，复制数据
    if isCopy:
        data = data.copy()

    # ==================== 步骤2: 遍历所有形态并识别 ====================
    # stock_column是形态定义字典
    for k in stock_column:
        try:
            # 调用TA-Lib函数识别形态
            # stock_column[k]['func']：TA-Lib函数，如tl.CDL2CROWS
            # 传入OHLC四个价格数组
            # 返回识别结果数组
            data.loc[:, k] = stock_column[k]['func'](
                data['open'].values,   # 开盘价数组
                data['high'].values,   # 最高价数组
                data['low'].values,    # 最低价数组
                data['close'].values   # 收盘价数组
            )
        except Exception as e:
            # 单个形态识别失败，跳过（不影响其他形态）
            pass

    # ==================== 步骤3: 检查结果 ====================
    # 检查数据是否有效
    if data is None or len(data.index) == 0:
        return None

    # ==================== 步骤4: 返回指定行数 ====================
    # 如果指定了threshold，只返回最后N行
    if threshold is not None:
        data = data.tail(n=threshold).copy()

    return data


# ==================== 获取单只股票的形态识别 ====================

"""
获取单只股票某一天的形态识别结果
参数说明：
code_name (tuple): 股票信息(date, code, name)
data (DataFrame): 历史K线数据
stock_column (dict): 形态定义字典
date (datetime.date, 可选): 计算日期
calc_threshold (int): 计算用的数据天数
- 默认12：用最后12天数据识别
- K线形态一般不超过12根K线
返回值：
pandas.Series: 形态识别结果
- 只返回识别到形态的股票
- 如果没有识别到任何形态，返回None
功能说明：
1. 调用get_pattern_recognitions()识别形态
2. 检查是否有形态（值!=0）
3. 如果有形态，返回结果
4. 如果没有形态，返回None
为什么只返回有形态的？
- 大部分股票大部分时间没有形态
- 只保存有形态的，节省数据库空间
- 方便筛选和查询
calc_threshold为什么是12？
- K线形态通常由2-12根K线组成
- 大部分形态只需要3-5根K线
- 12天足够识别所有形态
- 减少计算量，提高速度
使用场景：
klinepattern_data_daily_job中调用
并行识别所有股票的形态
"""
def get_pattern_recognition(code_name, data, stock_column, date=None, calc_threshold=12):
    try:
        # ==================== 步骤1: 确定计算日期 ====================
        if date is None:
            end_date = code_name[0]  # 使用stock的日期
        else:
            end_date = date.strftime("%Y-%m-%d")

        code = code_name[1]  # 提取代码
        
        # ==================== 步骤2: 检查数据充足性 ====================
        # 至少需要2天数据
        if len(data.index) <= 1:
            return None

        # ==================== 步骤3: 识别形态 ====================
        # 调用形态识别函数
        # threshold=1：只返回最后1天
        # calc_threshold=12：用最后12天数据计算
        stockStat = get_pattern_recognitions(
            data, 
            stock_column, 
            end_date=end_date, 
            threshold=1,  # 只要最后一天的结果
            calc_threshold=calc_threshold  # 用最后12天数据识别
        )

        # 检查识别结果
        if stockStat is None:
            return None

        # ==================== 步骤4: 检查是否有形态 ====================
        # 遍历所有形态列
        isHas = False  # 标记是否有形态
        for k in stock_column:
            # 检查该形态的值是否不为0
            if stockStat.iloc[0][k] != 0:
                # 识别到形态
                isHas = True
                break  # 找到一个即可，跳出循环

        # ==================== 步骤5: 返回结果 ====================
        if isHas:
            # 有形态，返回结果
            # 添加代码列
            stockStat.loc[:, 'code'] = code
            
            # 返回形态列和代码列
            # iloc[0, -(len(stock_column) + 1):]：
            # - iloc[0, ...]：第一行
            # - -(len(stock_column) + 1)：倒数第N列开始
            # - 包含所有形态列+code列
            return stockStat.iloc[0, -(len(stock_column) + 1):]
        
        # 没有形态，返回None（不保存到数据库）

    except Exception as e:
        logging.error(f"pattern_recognitions.get_pattern_recognition处理异常：{code}代码{e}")

    return None


"""
===========================================
K线形态识别模块使用总结（给Python新手）
===========================================

1. 模块定位
   - 第四层：K线形态层
   - 使用TA-Lib专业库
   - 识别61种经典形态

2. K线形态概念
   什么是K线？
   - 蜡烛图：一根线显示4个价格
   - 实体：开盘价到收盘价
   - 影线：最高最低价
   
   什么是形态？
   - 多根K线的组合图案
   - 反映市场心理
   - 具有预测意义

3. 形态分类
   看涨形态（正值）：
   - 锤头：长下影线，像锤子
   - 晨星：三根K线，黎明前的星
   - 三个白兵：三连阳
   
   看跌形态（负值）：
   - 上吊线：长下影线在高位
   - 暮星：三根K线，黄昏的星
   - 三只乌鸦：三连阴
   
   中性形态（可能为0）：
   - 十字星：开盘等于收盘
   - 纺锤线：实体小，影线长

4. 识别结果
   - +100：看涨形态，买入信号
   - -100：看跌形态，卖出信号
   - 0：没有该形态

5. TA-Lib函数
   形态识别函数：
   - tl.CDLHAMMER()：锤头
   - tl.CDLMORNINGSTAR()：晨星
   - tl.CDL3WHITESOLDIERS()：三个白兵
   - ... 共61种
   
   参数：
   - open, high, low, close：OHLC数组
   
   返回：
   - 数组：每天的识别结果

6. 数据流程
   历史K线 → TA-Lib识别 → 
   形态结果 → 筛选有形态的 → 
   保存数据库 → Web展示

7. 为什么只保存有形态的？
   - 大部分时间没有形态
   - 节省数据库空间
   - 方便查询筛选
   
   示例：
   - 4000只股票
   - 每天可能只有100只出现形态
   - 只保存100条记录

8. calc_threshold=12
   - K线形态通常2-12根K线
   - 12天足够识别所有形态
   - 减少计算量
   - 提高速度

9. 使用场景
   技术分析：
   - 查看股票出现的形态
   - 判断买卖时机
   
   量化选股：
   - 筛选特定形态的股票
   - 如：筛选出现锤头的股票
   
   策略回测：
   - 验证形态的有效性
   - 统计胜率
   
   实盘交易：
   - 辅助决策
   - 提高胜率

10. 形态解读
    锤头（Hammer）：
    - 长下影线，短实体
    - 出现在底部
    - 买入信号
    
    上吊线（Hanging Man）：
    - 长下影线，短实体
    - 出现在顶部
    - 卖出信号
    
    晨星（Morning Star）：
    - 三根K线组合
    - 第一根：长阴线
    - 第二根：小实体（星）
    - 第三根：长阳线
    - 看涨反转信号
    
    暮星（Evening Star）：
    - 三根K线组合
    - 第一根：长阳线
    - 第二根：小实体（星）
    - 第三根：长阴线
    - 看跌反转信号

11. Python知识点
    - 字典遍历：for k in dict
    - 函数调用：func(args)
    - 动态调用：dict[k]['func']
    - 数组操作：.values
    - DataFrame列赋值：data.loc[:, col]

12. 注意事项
    - 形态不是绝对信号
    - 需要趋势确认
    - 需要成交量配合
    - 不同周期形态不同
    - 回测验证有效性

13. 优化建议
    - 只识别常用形态：提高速度
    - 结合趋势：在趋势中使用
    - 结合指标：多重确认
    - 形态组合：多个形态共振

14. 常见形态
    必须掌握的10种：
    1. 锤头/上吊线
    2. 十字星
    3. 晨星/暮星
    4. 吞没形态
    5. 刺透/乌云盖顶
    6. 三个白兵/三只乌鸦
    7. 启明星/黄昏星
    8. 射击之星
    9. 孕线
    10. 缺口形态
"""
