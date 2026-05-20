#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筹码分布计算模块 - CYQ (Chip Distribution)
============================================

功能说明：
本模块实现了股票筹码分布（CYQ）算法，用于分析市场中不同价格位置的筹码堆积情况。
筹码分布是技术分析中的重要工具，可以帮助判断支撑位、压力位和主力成本区域。

核心概念：
- 筹码分布：表示在不同价格位置上投资者持有的股票数量分布
- 获利比例：当前价格下方筹码占总筹码的比例，反映市场获利盘大小
- 平均成本：所有筹码的加权平均价格，代表市场平均持仓成本
- 筹码集中度：特定百分比范围内的筹码价格区间，反映筹码集中程度

算法原理：
1. 选取最近N天的K线数据（默认210天）
2. 将价格区间划分为M个精度等级（默认150级）
3. 对每一天的交易，根据价格区间和换手率分配筹码
4. 考虑时间衰减：早期筹码会按换手率逐渐减少
5. 计算各种指标：获利比例、平均成本、筹码集中度等

数学模型：
- 三角形分布：对于有振幅的K线，筹码在高低点之间呈三角形分布
- 矩形分布：对于一字板（高低点相同），筹码集中在单一价格
- 时间衰减：旧筹码 = 旧筹码 × (1 - 换手率)
- 新筹码分配：根据价格在三角形中的位置按比例分配

使用场景：
- 判断支撑位：筹码密集区通常是强支撑
- 判断压力位：上方筹码密集区是强压力
- 判断主力成本：大量筹码集中的价格可能是主力建仓成本
- 判断市场情绪：获利比例高说明多数人赚钱，情绪乐观

参数说明：
- accuracy_factor：精度因子，价格轴划分的等级数（默认150）
- crange：计算的K线条数（默认120）
- cyq_days：计算筹码分布的交易天数（默认210）

输出指标：
- x：筹码堆叠数据（每个价格等级的筹码数量）
- y：价格分布（每个等级对应的价格）
- benefit_part：获利比例（0-1之间）
- avg_cost：平均成本
- percent_chips：90%和70%筹码的价格区间和集中度
- b：盈亏分界下标
- d：交易日期
- t：交易天数

注意事项：
- 计算量较大，建议缓存结果
- 精度因子越大，计算越精确但速度越慢
- 适用于有明显成交的股票，流动性差的股票参考价值低
- 需要结合其他指标综合判断

依赖关系：
- pandas：数据处理
- numpy：数值计算（间接依赖）

参考资料：
- 筹码分布理论
- 成本分析技术
- 市场微观结构
"""

__author__ = 'myh '
__date__ = '2025/1/6 '


# ==================== 筹码分布计算器 ====================

"""
CYQCalculator - 筹码分布计算器类

功能：
根据K线数据计算指定时间点的筹码分布及相关指标

初始化参数：
kdata (DataFrame): K线图数据
必须包含以下列：
- date：日期
- open：开盘价
- close：收盘价
- high：最高价
- low：最低价
- volume：成交量
- amount：成交额
- amplitude：振幅
- turnover：换手率

accuracy_factor (int): 精度因子
- 默认值：150
- 含义：价格轴划分的等级数
- 影响：值越大，计算越精确，但速度越慢
- 范围：建议50-500之间

crange (int): 计算范围
- 默认值：120
- 含义：从当前K线往前数的K线条数
- 作用：确定计算的起始位置

cyq_days (int): 计算交易天数
- 默认值：210
- 含义：用于计算筹码分布的历史天数
- 说明：通常使用210天（约一年的交易日）

使用示例：
```python
# 准备K线数据
cyq_stock = stock_data.tail(330)  # 需要threshold + cyq_days的数据

# 创建计算器
calculator = CYQCalculator(cyq_stock, accuracy_factor=150, crange=120, cyq_days=210)

# 计算第119根K线的筹码分布
result = calculator.calc(119)

# 获取结果
print(f"平均成本: {result.avg_cost}")
print(f"获利比例: {result.benefit_part}")
print(f"90%筹码区间: {result.percent_chips['90']['priceRange']}")
```

计算流程：
1. 确定计算的时间范围（start到end）
2. 找出价格区间（最高价和最低价）
3. 计算精度（每个等级代表的价格差）
4. 初始化筹码数组
5. 遍历每一天的K线，分配筹码
6. 计算各种指标（获利比例、平均成本等）
7. 返回CYQData对象
"""
class CYQCalculator:
    def __init__(self, kdata, accuracy_factor=150, crange=120, cyq_days=210):
        """
        初始化筹码分布计算器
        
        参数：
        kdata (DataFrame): K线图数据，包含OHLCV等信息
        accuracy_factor (int): 精度因子，价格轴等级数，默认150
        crange (int): 计算范围的K线条数，默认120
        cyq_days (int): 计算筹码分布的交易天数，默认210
        """
        # K图数据：pandas DataFrame格式
        self.klinedata = kdata
        # 精度因子(纵轴刻度数)：决定价格轴的精细程度
        self.fator = accuracy_factor
        # 计算K线条数：从当前K线往前的条数
        self.range = crange
        # 计算筹码分布的交易天数：历史数据的天数
        self.tradingdays = cyq_days

    # ==================== 核心计算方法 ====================
    
    """
    calc - 计算指定索引位置的筹码分布
    
    功能：
    根据给定的K线索引，计算该时间点的筹码分布及各种指标
    
    参数：
    index (int): 当前选中的K线的索引
    - 从0开始计数
    - 通常是用户在前端选择的K线位置
    - 例如：index=119表示第120根K线
    
    返回：
    CYQData对象，包含以下属性：
    - x (list): 筹码堆叠数据，每个元素是对应价格等级的筹码数量
    - y (list): 价格分布，每个元素是对应等级的价格值
    - benefit_part (float): 获利比例，0-1之间
    - avg_cost (str): 平均成本，格式化字符串（保留2位小数）
    - percent_chips (dict): 百分比筹码信息
      - '90': 90%筹码的价格区间和集中度
      - '70': 70%筹码的价格区间和集中度
    - b (int): 筹码堆叠盈亏分界下标
    - d (datetime): 交易日期
    - t (int): 交易天数
    
    执行流程：
    1. 确定计算范围：根据index、range、tradingdays确定start和end
    2. 提取K线数据：从DataFrame中截取需要的数据
    3. 计算价格区间：找出最高价和最低价
    4. 计算精度：确定每个等级代表的价格差
    5. 初始化价格数组：生成yrange（价格分布）
    6. 初始化筹码数组：xdata全部设为0
    7. 遍历K线数据：
       a. 计算当日均价
       b. 计算换手率（限制在0-1之间）
       c. 确定价格区间对应的等级范围（H和L）
       d. 计算G点坐标（分布的中心点）
       e. 时间衰减：所有旧筹码乘以(1-换手率)
       f. 分配新筹码：
          - 一字板：矩形分布（面积是三角形的2倍）
          - 正常K线：三角形分布
    8. 计算总筹码数
    9. 定义内部函数get_cost_by_chip：获取指定筹码处的成本
    10. 创建CYQData对象并填充各项指标
    11. 返回结果
    
    算法详解：
    
    1. 时间衰减机制：
       每天的旧筹码都会减少，减少的比例等于当天的换手率
       新筹码 = 旧筹码 × (1 - 换手率)
       这模拟了股票的流通性，换手率越高，旧筹码消失越快
    
    2. 筹码分配方式：
       
       a) 一字板（high == low）：
          - 所有筹码集中在一个价格
          - 使用矩形分布
          - GPoint[0] = factor - 1（最大值）
          - 分配的筹码量 = GPoint[0] × 换手率 / 2
          - 除以2是因为矩形面积是三角形的2倍
       
       b) 正常K线（high > low）：
          - 筹码在高低点之间呈三角形分布
          - 以均价为中心，向上下递减
          - GPoint[0] = 2 / (high - low)（振幅越小，峰值越高）
          - 上半部分（low到avg）：线性递增
            筹码量 = (curprice - low) / (avg - low) × GPoint[0] × 换手率
          - 下半部分（avg到high）：线性递减
            筹码量 = (high - curprice) / (high - avg) × GPoint[0] × 换手率
    
    3. 获利比例计算：
       遍历所有价格等级，累加当前价格下方的筹码数量
       获利比例 = 下方筹码 / 总筹码
    
    4. 平均成本计算：
       找到累积筹码达到50%时的价格
       这代表市场的中位数成本
    
    5. 百分比筹码计算：
       例如90%筹码：
       - 下限：累积筹码达到5%时的价格
       - 上限：累积筹码达到95%时的价格
       - 集中度 = (上限 - 下限) / (上限 + 下限)
       集中度越小，说明筹码越集中
    
    数学公式：
    
    精度计算：
    accuracy = max(0.01, (maxprice - minprice) / (factor - 1))
    
    价格等级：
    yrange[i] = minprice + accuracy × i
    
    G点坐标：
    GPoint[0] = factor - 1 (if high == low)
              = 2 / (high - low) (otherwise)
    GPoint[1] = (avg - minprice) / accuracy
    
    时间衰减：
    xdata[n] = xdata[n] × (1 - turnover_rate)
    
    三角形分布（上半部分）：
    xdata[j] += (curprice - low) / (avg - low) × GPoint[0] × turnover_rate
    
    三角形分布（下半部分）：
    xdata[j] += (high - curprice) / (high - avg) × GPoint[0] × turnover_rate
    
    注意事项：
    - 使用f"{x:.12g}"格式化避免浮点数精度问题
    - 换手率限制在0-1之间：min(1, turnover / 100)
    - 精度不小于0.01：max(0.01, ...)
    - 处理除零错误：abs(avg - low) < 1e-8时使用特殊逻辑
    """
    def calc(self, index):
        """
        计算指定索引位置的筹码分布
        
        参数：
        index (int): K线索引，从0开始
        
        返回：
        CYQData: 包含筹码分布数据的对象
        """
        
        # ==================== 步骤1: 初始化变量 ====================
        # 初始化最高价和最低价
        maxprice = 0  # 最高价，初始为0
        minprice = 1000000  # 最低价，初始为一个很大的数
        
        # 获取精度因子
        factor = self.fator
        
        # ==================== 步骤2: 确定计算范围 ====================
        # 计算结束位置：当前索引 - 范围 + 1
        end = index - self.range + 1
        
        # 计算起始位置：结束位置 - 交易天数
        start = end - self.tradingdays
        
        # ==================== 步骤3: 提取K线数据 ====================
        # 根据end的值选择不同的数据切片方式
        if end == 0:
            # 如果end为0，取最后tradingdays天的数据
            # tail()方法返回DataFrame的最后N行
            kdata = self.klinedata.tail(self.tradingdays)
        else:
            # 否则，使用iloc切片[start:end]
            # 注意：Python切片是左闭右开区间
            kdata = self.klinedata[start:end]
        
        # ==================== 步骤4: 计算价格区间 ====================
        # 遍历所有K线的最高价和最低价
        # zip()函数将两个列表打包成元组对
        for _high, _low in zip(kdata['high'].values, kdata['low'].values):
            # 更新最高价：取当前最高价和已有最高价的较大值
            maxprice = max(maxprice, _high)
            # 更新最低价：取当前最低价和已有最低价的较小值
            minprice = min(minprice, _low)
        
        # ==================== 步骤5: 计算精度 ====================
        # 精度 = 价格区间 / (等级数 - 1)
        # 最小精度为0.01（产品逻辑要求）
        # max()函数确保精度不会太小
        accuracy = max(0.01, (maxprice - minprice) / (factor - 1))
        
        # 获取当前价格（最后一根K线的收盘价）
        # iloc[-1]表示最后一行
        currentprice = kdata.iloc[-1]['close']
        
        # 初始化盈亏分界下标
        boundary = -1
        
        # ==================== 步骤6: 初始化价格数组 ====================
        # *值域 @ type  {Array. < number >}
        # yrange存储每个等级对应的价格值
        yrange = []
        
        # 遍历所有精度等级，生成价格数组
        for i in range(factor):
            # 计算第i个等级的价格
            # f"{...:.2f}"格式化为2位小数
            # float()转换为浮点数
            _price = float(f"{minprice + accuracy * i:.2f}")
            
            # 添加到价格数组
            yrange.append(_price)
            
            # 找到当前价格所在的等级位置
            # 当价格首次大于等于当前收盘价时，记录这个等级
            if boundary == -1 and _price >= currentprice:
                boundary = i
        
        # ==================== 步骤7: 初始化筹码数组 ====================
        # *横轴数据
        # xdata存储每个价格等级的筹码数量
        # 初始时所有等级的筹码都为0
        # [0] * factor 创建一个长度为factor的全0列表
        xdata = [0] * factor
        
        # ==================== 步骤8: 遍历K线数据，分配筹码 ====================
        # 遍历每一天的K线数据
        # zip()同时遍历多个列
        for open_price, close, high, low, turnover in zip(
            kdata['open'].values,   # 开盘价
            kdata['close'].values,  # 收盘价
            kdata['high'].values,   # 最高价
            kdata['low'].values,    # 最低价
            kdata['turnover'].values  # 换手率
        ):
            # --- 8.1 计算当日均价 ---
            # 均价 = (开盘 + 收盘 + 最高 + 最低) / 4
            # 这是一种简化的均价计算方法
            avg = (open_price + close + high + low) / 4
            
            # --- 8.2 计算换手率 ---
            # 换手率可能超过100%，所以限制在0-1之间
            # min(1, turnover / 100)确保换手率不超过1
            turnover_rate = min(1, turnover / 100)
            
            # --- 8.3 计算价格区间对应的等级范围 ---
            # H：最高价对应的等级索引
            H = int((high - minprice) / accuracy)
            
            # L：最低价对应的等级索引
            # +0.99是为了向上取整的效果
            L = int((low - minprice) / accuracy + 0.99)
            
            # --- 8.4 计算G点坐标 ---
            # G点是筹码分布的中心点
            # GPoint[0]：X坐标（分布的峰值高度）
            # GPoint[1]：Y坐标（分布的中心价格等级）
            
            # 一字板时，X为进度因子（最大值）
            # 正常K线时，X与振幅成反比（振幅越小，峰值越高）
            GPoint = [
                factor - 1 if high == low else 2 / (high - low),
                int((avg - minprice) / accuracy)
            ]
            
            # --- 8.5 时间衰减：旧筹码减少 ---
            # 遍历所有价格等级
            for n in range(len(xdata)):
                # 每个等级的旧筹码都乘以(1 - 换手率)
                # 换手率越高，旧筹码消失越快
                xdata[n] *= (1 - turnover_rate)
            
            # --- 8.6 分配新筹码 ---
            if high == low:
                # === 情况A：一字板（涨停或跌停）===
                # 所有筹码集中在一个价格
                # 画矩形面积是三角形的2倍，所以要除以2
                xdata[GPoint[1]] += GPoint[0] * turnover_rate / 2
            else:
                # === 情况B：正常K线（有振幅）===
                # 遍历从最低价到最高价的所有等级
                for j in range(L, H + 1):
                    # 计算当前等级对应的价格
                    curprice = minprice + accuracy * j
                    
                    if curprice <= avg:
                        # --- 上半三角叠加分布（低价区到均价区）---
                        # 筹码量从0线性增加到峰值
                        
                        # 特殊情况：均价等于最低价（避免除零）
                        if abs(avg - low) < 1e-8:
                            # 直接分配最大筹码量
                            xdata[j] += GPoint[0] * turnover_rate
                        else:
                            # 正常情况：按线性比例分配
                            # 比例 = (当前价格 - 最低价) / (均价 - 最低价)
                            xdata[j] += (curprice - low) / (avg - low) * GPoint[0] * turnover_rate
                    else:
                        # --- 下半三角叠加分布（均价区到高价区）---
                        # 筹码量从峰值线性减少到0
                        
                        # 特殊情况：最高价等于均价（避免除零）
                        if abs(high - avg) < 1e-8:
                            # 直接分配最大筹码量
                            xdata[j] += GPoint[0] * turnover_rate
                        else:
                            # 正常情况：按线性比例分配
                            # 比例 = (最高价 - 当前价格) / (最高价 - 均价)
                            xdata[j] += (high - curprice) / (high - avg) * GPoint[0] * turnover_rate
        
        # ==================== 步骤9: 计算总筹码数 ====================
        # 对所有等级的筹码求和
        # f"{x:.12g}"格式化避免浮点数精度问题
        # .12g表示最多12位有效数字
        total_chips = sum(float(f"{x:.12g}") for x in xdata)
        
        # ==================== 步骤10: 定义内部辅助函数 ====================
        
        """
        get_cost_by_chip - 获取指定筹码处的成本价格
        
        功能：
        从低价到高价累加筹码，找到累积筹码达到指定值时的价格
        
        参数：
        chip (float): 目标筹码数量
        
        返回：
        float: 对应的价格
        
        算法：
        1. 从最低价格等级开始累加筹码
        2. 当累加值超过目标筹码时，返回当前价格
        3. 这类似于寻找累积分布函数的逆函数
        
        应用场景：
        - 计算平均成本：get_cost_by_chip(total_chips * 0.5)
        - 计算90%筹码下限：get_cost_by_chip(total_chips * 0.05)
        - 计算90%筹码上限：get_cost_by_chip(total_chips * 0.95)
        """
        # *获取指定筹码处的成本
        # * @ param {number} chip 堆叠筹码
        def get_cost_by_chip(chip):
            """
            根据筹码数量获取对应的成本价格
            
            参数：
            chip (float): 目标累积筹码数量
            
            返回：
            float: 对应的价格
            """
            result = 0  # 结果价格
            sum_chips = 0  # 累积筹码数
            
            # 从低价到高价遍历所有等级
            for i in range(factor):
                # 获取当前等级的筹码数（格式化避免精度问题）
                x = float(f"{xdata[i]:.12g}")
                
                # 如果累积筹码超过目标值
                if sum_chips + x > chip:
                    # 返回当前等级的价格
                    result = minprice + i * accuracy
                    break  # 找到后立即退出循环
                
                # 累加当前等级的筹码
                sum_chips += x
            
            return result
        
        # ==================== 步骤11: 定义结果数据类 ====================
        
        """
        CYQData - 筹码分布数据类
        
        功能：
        存储筹码分布的计算结果和各种指标
        
        属性：
        x (list): 筹码堆叠数据，每个价格等级的筹码数量
        y (list): 价格分布，每个等级对应的价格
        benefit_part (float): 获利比例，0-1之间
        avg_cost (str): 平均成本，格式化字符串
        percent_chips (dict): 百分比筹码信息
        b (int): 筹码堆叠盈亏分界下标
        d (datetime): 交易日期
        t (int): 交易天数
        
        内部方法：
        compute_percent_chips(percent): 计算指定百分比的筹码区间
        get_benefit_part(price): 获取指定价格的获利比例
        """
        # *筹码分布数据
        class CYQData:
            def __init__(self):
                """
                初始化CYQData对象
                
                所有属性初始为None，在calc方法中赋值
                """
                # 筹码堆叠：每个价格等级的筹码数量
                self.x = None
                # 价格分布：每个等级对应的价格
                self.y = None
                # 获利比例：当前价格下方筹码占总筹码的比例
                self.benefit_part = None
                # 平均成本：所有筹码的加权平均价格
                self.avg_cost = None
                # 百分比筹码：90%和70%筹码的价格区间
                self.percent_chips = None
                # 筹码堆叠亏盈分界下标：当前价格所在的等级
                self.b = None
                # 交易日期：最后一根K线的日期
                self.d = None
                # 交易天数：计算使用的天数
                self.t = None
            
            """
            compute_percent_chips - 计算指定百分比的筹码区间
            
            功能：
            计算市场中指定百分比（如90%）的筹码所在的价格区间
            
            参数：
            percent (float): 百分比，范围0-1
            - 0.9表示90%的筹码
            - 0.7表示70%的筹码
            
            返回：
            dict: 包含价格区间和集中度
            - 'priceRange': [lower, upper] 价格区间的上下限
            - 'concentration': 集中度，计算公式为(upper-lower)/(upper+lower)
            
            算法：
            1. 计算左右边界的位置
               - 左边界：(1-percent)/2的位置
               - 右边界：(1+percent)/2的位置
               例如90%筹码：
               - 左边界：(1-0.9)/2 = 0.05（5%位置）
               - 右边界：(1+0.9)/2 = 0.95（95%位置）
            2. 调用get_cost_by_chip获取对应位置的價格
            3. 计算集中度：(上限-下限)/(上限+下限)
               - 集中度越小，筹码越集中
               - 集中度越大，筹码越分散
            
            异常：
            ValueError: 当percent不在0-1范围内时抛出
            
            使用示例：
            ```python
            # 计算90%筹码区间
            chips_90 = result.compute_percent_chips(0.9)
            print(f"90%筹码区间: {chips_90['priceRange']}")
            print(f"集中度: {chips_90['concentration']:.4f}")
            ```
            """
            # *计算指定百分比的筹码
            # * @ param {number} percent 百分比大于0，小于1
            @staticmethod
            def compute_percent_chips(percent):
                """
                静态方法：计算指定百分比的筹码区间
                
                参数：
                percent (float): 百分比，0-1之间
                
                返回：
                dict: {'priceRange': [lower, upper], 'concentration': value}
                """
                # 参数验证：确保百分比在合理范围内
                if percent > 1 or percent < 0:
                    raise ValueError('argument "percent" out of range')
                
                # 计算左右边界的累积比例
                # ps[0]：左边界（例如5%）
                # ps[1]：右边界（例如95%）
                ps = [(1 - percent) / 2, (1 + percent) / 2]
                
                # 获取左右边界对应的价格
                pr = [
                    get_cost_by_chip(total_chips * ps[0]),  # 左边界价格
                    get_cost_by_chip(total_chips * ps[1])   # 右边界价格
                ]
                
                # 返回结果字典
                return {
                    # 价格区间：格式化为2位小数的字符串
                    'priceRange': [f"{pr[0]:.2f}", f"{pr[1]:.2f}"],
                    # 集中度：(上限-下限)/(上限+下限)
                    # 如果上下限之和为0，集中度为0（避免除零）
                    'concentration': 0 if pr[0] + pr[1] == 0 else (pr[1] - pr[0]) / (pr[0] + pr[1])
                }
            
            """
            get_benefit_part - 获取指定价格的获利比例
            
            功能：
            计算在当前价格下方有多少比例的筹码，即获利盘比例
            
            参数：
            price (float): 参考价格
            
            返回：
            float: 获利比例，0-1之间
            - 0表示没有人获利（价格在最低点）
            - 1表示所有人获利（价格在最高点）
            - 0.5表示一半人获利（价格在中位数成本）
            
            算法：
            1. 遍历所有价格等级
            2. 累加价格低于参考价的筹码数量
            3. 获利比例 = 下方筹码 / 总筹码
            
            应用场景：
            - 判断市场情绪：获利比例高说明多数人赚钱
            - 判断抛压：获利比例突然下降可能有大量抛售
            - 判断支撑：获利比例在某个水平稳定说明有支撑
            
            使用示例：
            ```python
            # 计算当前价格的获利比例
            benefit = result.get_benefit_part(currentprice)
            print(f"获利比例: {benefit:.2%}")
            
            # 判断市场情绪
            if benefit > 0.8:
                print("市场情绪乐观，多数人获利")
            elif benefit < 0.2:
                print("市场情绪悲观，多数人亏损")
            ```
            """
            # *获取指定价格的获利比例
            # * @ param {number} price 价格
            @staticmethod
            def get_benefit_part(price):
                """
                静态方法：计算指定价格的获利比例
                
                参数：
                price (float): 参考价格
                
                返回：
                float: 获利比例，0-1之间
                """
                below = 0  # 下方筹码数量
                
                # 遍历所有价格等级
                for i in range(factor):
                    # 获取当前等级的筹码数
                    x = float(f"{xdata[i]:.12g}")
                    
                    # 如果参考价高于当前等级的价格
                    # 说明这个等级的筹码是获利的
                    if price >= minprice + i * accuracy:
                        below += x  # 累加到下方筹码
                
                # 计算获利比例
                # 如果总筹码为0，返回0（避免除零）
                return 0 if total_chips == 0 else below / total_chips
        
        # ==================== 步骤12: 创建并填充结果对象 ====================
        # 创建CYQData对象
        result = CYQData()
        
        # 填充筹码堆叠数据
        result.x = xdata
        
        # 填充价格分布
        result.y = yrange
        
        # 填充盈亏分界下标（+1是因为boundary从-1开始）
        result.b = boundary + 1
        
        # 填充交易日期（最后一根K线的日期）
        result.d = kdata.iloc[-1]['date']
        
        # 填充交易天数
        result.t = self.tradingdays
        
        # 计算并填充获利比例
        result.benefit_part = result.get_benefit_part(currentprice)
        
        # 计算并填充平均成本（50%筹码位置的价格）
        result.avg_cost = f"{get_cost_by_chip(total_chips * 0.5):.2f}"
        
        # 计算并填充百分比筹码信息
        result.percent_chips = {
            '90': result.compute_percent_chips(0.9),  # 90%筹码区间
            '70': result.compute_percent_chips(0.7)   # 70%筹码区间
        }
        
        # ==================== 步骤13: 返回结果 ====================
        return result

# ==================== 测试代码（已注释）====================
# if __name__ == "__main__":
#     # 使用示例（需要取消注释并准备数据）
#     # import instock.core.kline.cyq as cyq
#     # c = cyq.CYQCalculator(cyq_stock)
#     # r = c.calc(119)
#     # print(f"平均成本: {r.avg_cost}")
#     # print(f"获利比例: {r.benefit_part}")
