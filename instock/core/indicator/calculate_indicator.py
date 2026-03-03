#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
技术指标计算核心模块（第三层核心）
===================================
这是系统最重要的模块之一，负责计算32种技术指标。

什么是技术指标？
- 基于价格和成交量的数学计算
- 用于判断买卖时机和趋势
- 量化投资的基础工具

本模块计算的32种指标：

1. 趋势类指标（判断方向）：
   - MACD：指数平滑异同移动平均线
   - DMA：平行线差
   - TEMA：三重指数移动平均线
   
2. 摆动类指标（判断超买超卖）：
   - KDJ：随机指标
   - RSI：相对强弱指标
   - CCI：顺势指标
   - WR：威廉指标
   - STOCHRSI：随机相对强弱指标
   
3. 通道类指标（判断支撑压力）：
   - BOLL：布林带
   - ENE：轨道线
   
4. 能量类指标（判断买卖力量）：
   - CR：能量指标
   - VR：成交量比率
   - BRAR：情绪指标
   - MFI：资金流量指标
   
5. 波动类指标（判断风险）：
   - ATR：真实波幅
   - DPO：区间震荡线
   - VHF：纵横指标
   
6. 趋向类指标（判断趋势强度）：
   - DMI：趋向指标（PDI、MDI、ADX、ADXR）
   - TRIX：三重指数平滑移动平均
   
7. 其他指标：
   - OBV：能量潮
   - SAR：抛物线转向
   - PSY：心理线
   - EMV：简易波动指标
   - BIAS：乖离率
   - ROC：变动率
   - RVI：相对活力指数
   - FI：力度指标
   - VOL：成交量均线
   - MA：移动平均线
   - VWMA：成交量加权移动平均
   - PPO：价格震荡百分比
   - WT：威廉指标变种
   - Supertrend：超级趋势

技术要点：
1. 使用TA-Lib库：专业的技术分析库
2. NumPy计算：高效的数值计算
3. Pandas处理：灵活的数据操作
4. 异常处理：NaN、Inf值的处理

计算原理：
- 移动平均：平滑价格波动
- 指数平滑：重视近期数据
- 相对强弱：比较涨跌力量
- 随机摆动：判断相对位置
- 通道突破：判断支撑压力

为什么结果和通达信/同花顺一致？
- 使用相同的计算公式
- 部分指标调整了参数
- 经过大量测试验证

使用TA-Lib的好处：
- 计算准确：经过验证的实现
- 运行高效：C语言编写
- 接口简单：易于使用
- 功能全面：150+种指标
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import pandas as pd  # 数据处理
import numpy as np  # 数值计算
import talib as tl  # TA-Lib技术分析库

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 主要计算函数 ====================

"""
计算所有技术指标（核心函数）
参数说明：
data (DataFrame): 历史K线数据，必须包含：
- date：日期
- open：开盘价
- high：最高价
- low：最低价
- close：收盘价
- volume：成交量
- amount：成交额
- p_change：涨跌幅
end_date (str, 可选): 截止日期，只计算到这个日期
- 格式："YYYY-MM-DD"
- None表示计算到最后一天
- 用于回测某个历史时点的指标
threshold (int): 返回数据的行数
- 默认120：返回最后120天的数据
- 用于控制输出数据量
calc_threshold (int, 可选): 计算数据的行数
- None：使用全部数据计算
- 指定值：只用最后N天数据计算
- 用于提高计算速度
返回值：
DataFrame: 包含所有指标的数据
- 原有列：date, open, high, low, close, volume, amount
- 新增列：32种指标的计算结果
- 返回最后threshold行数据
计算流程：
1. 数据筛选：根据end_date和calc_threshold
2. 计算指标：调用TA-Lib函数
3. 处理异常：NaN和Inf值替换为0
4. 返回结果：最后threshold行
性能优化：
- NumPy向量化计算：快
- TA-Lib C库：快
- 避免循环：快
异常处理：
- NaN（Not a Number）：计算不出来的值
- Inf（Infinity）：除以0的结果
- 都替换为0.0，避免后续错误
使用示例：
# 计算某只股票的指标
hist_data = fetch_stock_hist(...)
indicators = get_indicators(hist_data)
print(indicators[['date', 'macd', 'kdj', 'rsi']])
"""
def get_indicators(data, end_date=None, threshold=120, calc_threshold=None):
    try:
        # ==================== 步骤1: 数据筛选 ====================
        isCopy = False  # 标记是否需要复制数据
        
        # 如果指定了截止日期，过滤数据
        if end_date is not None:
            # 创建布尔掩码：date <= end_date
            mask = (data['date'] <= end_date)
            # 应用掩码筛选数据
            data = data.loc[mask]
            isCopy = True  # 标记需要复制
        
        # 如果指定了计算阈值，只用最后N天数据
        if calc_threshold is not None:
            # tail(n)：取最后n行
            data = data.tail(n=calc_threshold)
            isCopy = True
        
        # 如果进行了筛选，复制数据（避免修改原数据）
        if isCopy:
            data = data.copy()

        # ==================== 步骤2: 异常值处理环境 ====================
        # with np.errstate()：临时改变NumPy的错误处理方式
        # divide='ignore'：忽略除以0的警告
        # invalid='ignore'：忽略无效操作的警告
        # 这些警告很常见，但我们会手动处理
        with np.errstate(divide='ignore', invalid='ignore'):

            # ==================== 指标1: MACD ====================
            """
            MACD - 指数平滑异同移动平均线
            
            原理：
            - 快线（DIF）：12日EMA - 26日EMA
            - 慢线（DEA）：快线的9日EMA
            - 柱状图（MACD）：快线 - 慢线
            
            用途：
            - 判断趋势：快线在慢线上方为多头
            - 寻找拐点：柱状图由负转正为买入信号
            - 金叉死叉：快慢线交叉
            
            参数：
            - fastperiod=12：快线周期
            - slowperiod=26：慢线周期
            - signalperiod=9：信号线周期
            """
            data.loc[:, 'macd'], data.loc[:, 'macds'], data.loc[:, 'macdh'] = tl.MACD(
                data['close'].values,  # 收盘价数组
                fastperiod=12,  # 快线周期
                slowperiod=26,  # 慢线周期
                signalperiod=9  # 信号线周期
            )
            # 将NaN值替换为0
            data['macd'].values[np.isnan(data['macd'].values)] = 0.0
            data['macds'].values[np.isnan(data['macds'].values)] = 0.0
            data['macdh'].values[np.isnan(data['macdh'].values)] = 0.0

            # ==================== 指标2: KDJ ====================
            """
            KDJ - 随机指标
            
            原理：
            - K值：当前价格在N日内的相对位置
            - D值：K值的移动平均
            - J值：3K - 2D（更敏感）
            
            用途：
            - 判断超买超卖：
              K > 80：超买
              K < 20：超卖
            - 金叉死叉：K线穿越D线
            - J值预警：J > 100或J < 0
            
            参数：
            - fastk_period=9：K值周期
            - slowk_period=5：K值平滑
            - slowd_period=5：D值平滑
            """
            data.loc[:, 'kdjk'], data.loc[:, 'kdjd'] = tl.STOCH(
                data['high'].values,  # 最高价
                data['low'].values,  # 最低价
                data['close'].values,  # 收盘价
                fastk_period=9,  # K值周期
                slowk_period=5,  # K值平滑
                slowk_matype=1,  # 平滑类型（EMA）
                slowd_period=5,  # D值平滑
                slowd_matype=1  # 平滑类型（EMA）
            )
            # 处理NaN
            data['kdjk'].values[np.isnan(data['kdjk'].values)] = 0.0
            data['kdjd'].values[np.isnan(data['kdjd'].values)] = 0.0
            # 计算J值：3K - 2D
            data.loc[:, 'kdjj'] = 3 * data['kdjk'].values - 2 * data['kdjd'].values

            # ==================== 指标3: BOLL ====================
            """
            BOLL - 布林带（Bollinger Bands）
            
            原理：
            - 中轨：N日移动平均线
            - 上轨：中轨 + 2倍标准差
            - 下轨：中轨 - 2倍标准差
            
            用途：
            - 判断超买超卖：
              价格触及上轨：超买
              价格触及下轨：超卖
            - 判断波动：
              带宽变窄：波动减小
              带宽变宽：波动增大
            - 寻找突破：价格突破上下轨
            
            参数：
            - timeperiod=20：周期
            - nbdevup=2：上轨标准差倍数
            - nbdevdn=2：下轨标准差倍数
            """
            data.loc[:, 'boll_ub'], data.loc[:, 'boll'], data.loc[:, 'boll_lb'] = tl.BBANDS(
                data['close'].values,  # 收盘价
                timeperiod=20,  # 周期
                nbdevup=2,  # 上轨标准差
                nbdevdn=2,  # 下轨标准差
                matype=0  # 移动平均类型（SMA）
            )
            # 处理NaN
            data['boll_ub'].values[np.isnan(data['boll_ub'].values)] = 0.0
            data['boll'].values[np.isnan(data['boll'].values)] = 0.0
            data['boll_lb'].values[np.isnan(data['boll_lb'].values)] = 0.0

            # ==================== 指标4: TRIX ====================
            """
            TRIX - 三重指数平滑移动平均
            
            原理：
            - 对收盘价进行三次指数平滑
            - 计算变化率
            
            用途：
            - 过滤短期波动
            - 判断长期趋势
            - TRIX上穿0：买入信号
            - TRIX下穿0：卖出信号
            """
            data.loc[:, 'trix'] = tl.TRIX(data['close'].values, timeperiod=12)
            data['trix'].values[np.isnan(data['trix'].values)] = 0.0
            data.loc[:, 'trix_20_sma'] = tl.MA(data['trix'].values, timeperiod=20)
            data['trix_20_sma'].values[np.isnan(data['trix_20_sma'].values)] = 0.0

            # ==================== 指标5: CR ====================
            """
            CR - 能量指标（带货能力）
            
            原理：
            - 比较最高价和前收盘价的关系
            - 比较最低价和前收盘价的关系
            - CR = (上涨能量 / 下跌能量) * 100
            
            用途：
            - CR < 40：超卖
            - CR > 300：超买
            - 配合均线使用
            
            计算过程：
            1. 计算均价：成交额/成交量
            2. 计算H-M和M-L
            3. 求和并相除
            4. 乘以100转换为百分比
            """
            # 计算均价
            data.loc[:, 'm_price'] = data['amount'].values / data['volume'].values
            # 前一日均价（shift函数向前移动1位）
            data.loc[:, 'm_price_sf1'] = data['m_price'].shift(1, fill_value=0.0).values
            # H-M：最高价与前收盘价的差值（取较大者）
            data.loc[:, 'h_m'] = data['high'].values - data[['m_price_sf1', 'high']].values.min(axis=1)
            # M-L：前收盘价与最低价的差值（取较小者）
            data.loc[:, 'm_l'] = data['m_price_sf1'].values - data[['m_price_sf1', 'low']].values.min(axis=1)
            # 26日求和
            data.loc[:, 'h_m_sum'] = tl.SUM(data['h_m'].values, timeperiod=26)
            data.loc[:, 'm_l_sum'] = tl.SUM(data['m_l'].values, timeperiod=26)
            # CR = (H-M求和 / M-L求和) * 100
            data.loc[:, 'cr'] = data['h_m_sum'].values / data['m_l_sum'].values
            # 处理异常值
            data['cr'].values[np.isnan(data['cr'].values)] = 0.0
            data['cr'].values[np.isinf(data['cr'].values)] = 0.0
            data['cr'] = data['cr'].values * 100
            # CR的移动平均线
            data.loc[:, 'cr-ma1'] = tl.MA(data['cr'].values, timeperiod=5)
            data['cr-ma1'].values[np.isnan(data['cr-ma1'].values)] = 0.0
            data.loc[:, 'cr-ma2'] = tl.MA(data['cr'].values, timeperiod=10)
            data['cr-ma2'].values[np.isnan(data['cr-ma2'].values)] = 0.0
            data.loc[:, 'cr-ma3'] = tl.MA(data['cr'].values, timeperiod=20)
            data['cr-ma3'].values[np.isnan(data['cr-ma3'].values)] = 0.0

            # ==================== 指标6: RSI ====================
            """
            RSI - 相对强弱指标
            
            原理：
            - 比较N日内涨跌的力量
            - RSI = 上涨平均值 / (上涨平均值 + 下跌平均值) * 100
            
            用途：
            - RSI > 80：超买
            - RSI < 20：超卖
            - RSI金叉死叉
            
            常用周期：
            - RSI_6：短期（灵敏）
            - RSI_12：中期
            - RSI_14：标准
            - RSI_24：长期
            """
            data.loc[:, 'rsi'] = tl.RSI(data['close'].values, timeperiod=14)
            data['rsi'].values[np.isnan(data['rsi'].values)] = 0.0
            data.loc[:, 'rsi_6'] = tl.RSI(data['close'].values, timeperiod=6)
            data['rsi_6'].values[np.isnan(data['rsi_6'].values)] = 0.0
            data.loc[:, 'rsi_12'] = tl.RSI(data['close'].values, timeperiod=12)
            data['rsi_12'].values[np.isnan(data['rsi_12'].values)] = 0.0
            data.loc[:, 'rsi_24'] = tl.RSI(data['close'].values, timeperiod=24)
            data['rsi_24'].values[np.isnan(data['rsi_24'].values)] = 0.0

            # ==================== 指标7: VR ====================
            """
            VR - 成交量比率（Volume Ratio）
            
            原理：
            - 比较上涨日和下跌日的成交量
            - VR = [(上涨量 + 平盘量/2) / (下跌量 + 平盘量/2)] * 100
            
            用途：
            - VR < 40：超卖，买入时机
            - VR > 160：超买，卖出时机
            - VR配合价格判断量价关系
            
            计算步骤：
            1. 分离上涨日、下跌日、平盘日的成交量
            2. 分别求和
            3. 按公式计算
            """
            # 上涨日成交量（涨跌幅>0）
            data.loc[:, 'av'] = np.where(data['p_change'].values > 0, data['volume'].values, 0)
            data.loc[:, 'avs'] = tl.SUM(data['av'].values, timeperiod=26)
            # 下跌日成交量（涨跌幅<0）
            data.loc[:, 'bv'] = np.where(data['p_change'].values < 0, data['volume'].values, 0)
            data.loc[:, 'bvs'] = tl.SUM(data['bv'].values, timeperiod=26)
            # 平盘日成交量（涨跌幅=0）
            data.loc[:, 'cv'] = np.where(data['p_change'].values == 0, data['volume'].values, 0)
            data.loc[:, 'cvs'] = tl.SUM(data['cv'].values, timeperiod=26)
            # VR公式
            data.loc[:, 'vr'] = (data['avs'].values + data['cvs'].values / 2) / (data['bvs'].values + data['cvs'].values / 2)
            # 处理异常值
            data['vr'].values[np.isnan(data['vr'].values)] = 0.0
            data['vr'].values[np.isinf(data['vr'].values)] = 0.0
            data['vr'] = data['vr'].values * 100
            # VR的6日均线
            data.loc[:, 'vr_6_sma'] = tl.MA(data['vr'].values, timeperiod=6)
            data['vr_6_sma'].values[np.isnan(data['vr_6_sma'].values)] = 0.0

            # ==================== 指标8: ATR ====================
            """
            ATR - 真实波幅（Average True Range）
            
            原理：
            - 真实波幅 = MAX(H-L, |H-前C|, |前C-L|)
            - ATR = 真实波幅的N日平均
            
            用途：
            - 衡量价格波动程度
            - ATR越大，波动越大，风险越高
            - 用于设置止损位
            - 用于判断突破有效性
            """
            # 前一日收盘价
            data.loc[:, 'prev_close'] = data['close'].shift(1, fill_value=0.0).values
            # H-L：当日最高最低价差
            data.loc[:, 'h_l'] = data['high'].values - data['low'].values
            # H-前C：最高价与前收盘价差
            data.loc[:, 'h_cy'] = data['high'].values - data['prev_close'].values
            # 前C-L：前收盘价与最低价差
            data.loc[:, 'cy_l'] = data['prev_close'].values - data['low'].values
            # 取绝对值
            data.loc[:, 'h_cy_a'] = abs(data['h_cy'].values)
            data.loc[:, 'cy_l_a'] = abs(data['cy_l'].values)
            # TR = MAX(H-L, |H-前C|, |前C-L|)
            data.loc[:, 'tr'] = data.loc[:, ['h_l', 'h_cy_a', 'cy_l_a']].T.max().values
            data['tr'].values[np.isnan(data['tr'].values)] = 0.0
            # ATR = TR的14日平均
            data.loc[:, 'atr'] = tl.ATR(data['high'].values, data['low'].values, data['close'].values, timeperiod=14)
            data['atr'].values[np.isnan(data['atr'].values)] = 0.0

            # ==================== 指标9: DMI ====================
            """
            DMI - 趋向指标（Directional Movement Index）
            
            包含4个指标：
            - PDI（+DI）：上涨动向指标
            - MDI（-DI）：下跌动向指标
            - ADX：平均趋向指数（趋势强度）
            - ADXR：ADX的平滑（趋势确认）
            
            原理：
            - 比较上涨和下跌的力量
            - ADX衡量趋势强度，不管方向
            
            用途：
            - PDI > MDI：多头市场
            - PDI < MDI：空头市场
            - ADX > 25：趋势强劲
            - ADX < 20：盘整
            
            注意：
            这里使用的是stockstats的计算方法
            与TA-Lib标准方法略有不同
            """
            # 计算高低差
            data.loc[:, 'high_delta'] = np.insert(np.diff(data['high'].values), 0, 0.0)
            data.loc[:, 'high_m'] = (data['high_delta'].values + abs(data['high_delta'].values)) / 2
            data.loc[:, 'low_delta'] = np.insert(-np.diff(data['low'].values), 0, 0.0)
            data.loc[:, 'low_m'] = (data['low_delta'].values + abs(data['low_delta'].values)) / 2
            # PDM：上涨动向
            data.loc[:, 'pdm'] = tl.EMA(np.where(data['high_m'].values > data['low_m'].values, data['high_m'].values, 0), timeperiod=14)
            data['pdm'].values[np.isnan(data['pdm'].values)] = 0.0
            # PDI = PDM / ATR * 100
            data.loc[:, 'pdi'] = data['pdm'].values / data['atr'].values
            data['pdi'].values[np.isnan(data['pdi'].values)] = 0.0
            data['pdi'].values[np.isinf(data['pdi'].values)] = 0.0
            data['pdi'] = data['pdi'].values * 100
            # MDM：下跌动向
            data.loc[:, 'mdm'] = tl.EMA(np.where(data['low_m'].values > data['high_m'].values, data['low_m'].values, 0), timeperiod=14)
            data['mdm'].values[np.isnan(data['mdm'].values)] = 0.0
            # MDI = MDM / ATR * 100
            data.loc[:, 'mdi'] = data['mdm'].values / data['atr'].values
            data['mdi'].values[np.isnan(data['mdi'].values)] = 0.0
            data['mdi'].values[np.isinf(data['mdi'].values)] = 0.0
            data['mdi'] = data['mdi'].values * 100
            # DX = |PDI - MDI| / (PDI + MDI) * 100
            data.loc[:, 'dx'] = abs(data['pdi'].values - data['mdi'].values) / (data['pdi'].values + data['mdi'].values)
            data['dx'].values[np.isnan(data['dx'].values)] = 0.0
            data['dx'].values[np.isinf(data['dx'].values)] = 0.0
            data['dx'] = data['dx'].values * 100
            # ADX = DX的EMA（趋势强度）
            data.loc[:, 'adx'] = tl.EMA(data['dx'].values, timeperiod=6)
            data['adx'].values[np.isnan(data['adx'].values)] = 0.0
            # ADXR = ADX的EMA（趋势确认）
            data.loc[:, 'adxr'] = tl.EMA(data['adx'].values, timeperiod=6)
            data['adxr'].values[np.isnan(data['adxr'].values)] = 0.0

            # ==================== 指标10: WR ====================
            """
            WR - 威廉指标（Williams %R）
            
            原理：
            - WR = (最高价 - 收盘价) / (最高价 - 最低价) * (-100)
            - 值域：-100 到 0
            
            用途：
            - WR > -20：超买
            - WR < -80：超卖
            - 结合K线形态判断
            
            常用周期：
            - WR_6：短期（快速）
            - WR_10：中期
            - WR_14：标准
            """
            data.loc[:, 'wr_6'] = tl.WILLR(data['high'].values, data['low'].values, data['close'].values, timeperiod=6)
            data['wr_6'].values[np.isnan(data['wr_6'].values)] = 0.0
            data.loc[:, 'wr_10'] = tl.WILLR(data['high'].values, data['low'].values, data['close'].values, timeperiod=10)
            data['wr_10'].values[np.isnan(data['wr_10'].values)] = 0.0
            data.loc[:, 'wr_14'] = tl.WILLR(data['high'].values, data['low'].values, data['close'].values, timeperiod=14)
            data['wr_14'].values[np.isnan(data['wr_14'].values)] = 0.0

            # ==================== 指标11: CCI ====================
            """
            CCI - 顺势指标（Commodity Channel Index）
            
            原理：
            - 比较当前价格与平均价格的偏离程度
            - CCI = (典型价格 - 移动平均) / (0.015 * 平均绝对偏差)
            
            用途：
            - CCI > 100：超买
            - CCI < -100：超卖
            - CCI突破±100：趋势开始
            - CCI回归±100内：趋势减弱
            
            周期：
            - CCI_14：标准周期
            - CCI_84：长期周期
            """
            data.loc[:, 'cci'] = tl.CCI(data['high'].values, data['low'].values, data['close'].values, timeperiod=14)
            data['cci'].values[np.isnan(data['cci'].values)] = 0.0
            data.loc[:, 'cci_84'] = tl.CCI(data['high'].values, data['low'].values, data['close'].values, timeperiod=84)
            data['cci_84'].values[np.isnan(data['cci_84'].values)] = 0.0

            # ==================== 指标12-32 ====================
            # 以下是其他20种指标的计算
            # 为了简洁，只列出关键代码，详细说明见各指标注释

            # DMA：平行线差
            data.loc[:, 'ma10'] = tl.MA(data['close'].values, timeperiod=10)
            data['ma10'].values[np.isnan(data['ma10'].values)] = 0.0
            data.loc[:, 'ma50'] = tl.MA(data['close'].values, timeperiod=50)
            data['ma50'].values[np.isnan(data['ma50'].values)] = 0.0
            data.loc[:, 'dma'] = data['ma10'].values - data['ma50'].values
            data.loc[:, 'dma_10_sma'] = tl.MA(data['dma'].values, timeperiod=10)
            data['dma_10_sma'].values[np.isnan(data['dma_10_sma'].values)] = 0.0

            # TEMA：三重指数移动平均
            data.loc[:, 'tema'] = tl.TEMA(data['close'].values, timeperiod=14)
            data['tema'].values[np.isnan(data['tema'].values)] = 0.0

            # MFI：资金流量指标
            data.loc[:, 'mfi'] = tl.MFI(data['high'].values, data['low'].values, data['close'].values, data['volume'].values, timeperiod=14)
            data['mfi'].values[np.isnan(data['mfi'].values)] = 0.0
            data.loc[:, 'mfisma'] = tl.MA(data['mfi'].values, timeperiod=6)

            # VWMA：成交量加权移动平均
            data.loc[:, 'tpv_14'] = tl.SUM(data['amount'].values, timeperiod=14)
            data.loc[:, 'vol_14'] = tl.SUM(data['volume'].values, timeperiod=14)
            data.loc[:, 'vwma'] = data['tpv_14'].values / data['vol_14'].values
            data['vwma'].values[np.isnan(data['vwma'].values)] = 0.0
            data['vwma'].values[np.isinf(data['vwma'].values)] = 0.0
            data.loc[:, 'mvwma'] = tl.MA(data['vwma'].values, timeperiod=6)

            # PPO：价格震荡百分比
            data.loc[:, 'ppo'] = tl.PPO(data['close'].values, fastperiod=12, slowperiod=26, matype=1)
            data['ppo'].values[np.isnan(data['ppo'].values)] = 0.0
            data.loc[:, 'ppos'] = tl.EMA(data['ppo'].values, timeperiod=9)
            data['ppos'].values[np.isnan(data['ppos'].values)] = 0.0
            data.loc[:, 'ppoh'] = data['ppo'].values - data['ppos'].values

            # STOCHRSI：随机相对强弱指标
            data.loc[:, 'rsi_min'] = tl.MIN(data['rsi'].values, timeperiod=14)
            data.loc[:, 'rsi_max'] = tl.MAX(data['rsi'].values, timeperiod=14)
            data.loc[:, 'stochrsi_k'] = (data['rsi'].values - data['rsi_min'].values) / (data['rsi_max'].values - data['rsi_min'].values)
            data['stochrsi_k'].values[np.isnan(data['stochrsi_k'].values)] = 0.0
            data['stochrsi_k'].values[np.isinf(data['stochrsi_k'].values)] = 0.0
            data['stochrsi_k'] = data['stochrsi_k'].values * 100
            data.loc[:, 'stochrsi_d'] = tl.MA(data['stochrsi_k'].values, timeperiod=3)

            # WT：威廉指标变种
            data.loc[:, 'esa'] = tl.EMA(data['m_price'].values, timeperiod=10)
            data['esa'].values[np.isnan(data['esa'].values)] = 0.0
            data.loc[:, 'esa_d'] = tl.EMA(abs(data['m_price'].values - data['esa'].values), timeperiod=10)
            data.loc[:, 'esa_ci'] = (data['m_price'].values - data['esa'].values) / (0.015 * data['esa_d'].values)
            data['esa_ci'].values[np.isnan(data['esa_ci'].values)] = 0.0
            data['esa_ci'].values[np.isinf(data['esa_ci'].values)] = 0.0
            data.loc[:, 'wt1'] = tl.EMA(data['esa_ci'].values, timeperiod=21)
            data['wt1'].values[np.isnan(data['wt1'].values)] = 0.0
            data.loc[:, 'wt2'] = tl.MA(data['wt1'].values, timeperiod=4)
            data['wt2'].values[np.isnan(data['wt2'].values)] = 0.0

            # Supertrend：超级趋势
            data.loc[:, 'm_atr'] = data['atr'].values * 3
            data.loc[:, 'hl_avg'] = (data['high'].values + data['low'].values) / 2.0
            data.loc[:, 'b_ub'] = data['hl_avg'].values + data['m_atr'].values
            data.loc[:, 'b_lb'] = data['hl_avg'].values - data['m_atr'].values
            size = len(data.index)
            ub = np.empty(size, dtype=np.float64)
            lb = np.empty(size, dtype=np.float64)
            st = np.empty(size, dtype=np.float64)
            for i in range(size):
                if i == 0:
                    ub[i] = data['b_ub'].iloc[i]
                    lb[i] = data['b_lb'].iloc[i]
                    if data['close'].iloc[i] <= ub[i]:
                        st[i] = ub[i]
                    else:
                        st[i] = lb[i]
                    continue

                last_close = data['close'].iloc[i - 1]
                curr_close = data['close'].iloc[i]
                last_ub = ub[i - 1]
                last_lb = lb[i - 1]
                last_st = st[i - 1]
                curr_b_ub = data['b_ub'].iloc[i]
                curr_b_lb = data['b_lb'].iloc[i]

                # 计算当前上轨
                if curr_b_ub < last_ub or last_close > last_ub:
                    ub[i] = curr_b_ub
                else:
                    ub[i] = last_ub

                # 计算当前下轨
                if curr_b_lb > last_lb or last_close < last_lb:
                    lb[i] = curr_b_lb
                else:
                    lb[i] = last_lb

                # 计算超级趋势
                if last_st == last_ub:
                    if curr_close <= ub[i]:
                        st[i] = ub[i]
                    else:
                        st[i] = lb[i]
                elif last_st == last_lb:
                    if curr_close > lb[i]:
                        st[i] = lb[i]
                    else:
                        st[i] = ub[i]

            data.loc[:, 'supertrend_ub'] = ub
            data.loc[:, 'supertrend_lb'] = lb
            data.loc[:, 'supertrend'] = st
            data = data.copy()

            # ==================== 以下是stockstats没有的指标 ====================
            
            # ROC：变动率
            data.loc[:, 'roc'] = tl.ROC(data['close'].values, timeperiod=12)
            data['roc'].values[np.isnan(data['roc'].values)] = 0.0
            data.loc[:, 'rocma'] = tl.MA(data['roc'].values, timeperiod=6)
            data['rocma'].values[np.isnan(data['rocma'].values)] = 0.0
            data.loc[:, 'rocema'] = tl.EMA(data['roc'].values, timeperiod=9)
            data['rocema'].values[np.isnan(data['rocema'].values)] = 0.0

            # OBV：能量潮
            data.loc[:, 'obv'] = tl.OBV(data['close'].values, data['volume'].values)
            data['obv'].values[np.isnan(data['obv'].values)] = 0.0

            # SAR：抛物线转向
            data.loc[:, 'sar'] = tl.SAR(data['high'].values, data['low'].values)
            data['sar'].values[np.isnan(data['sar'].values)] = 0.0

            # PSY：心理线
            data.loc[:, 'price_up'] = 0.0
            data.loc[data['close'].values > data['prev_close'].values, 'price_up'] = 1.0
            data.loc[:, 'price_up_sum'] = tl.SUM(data['price_up'].values, timeperiod=12)
            data.loc[:, 'psy'] = data['price_up_sum'].values / 12.0
            data['psy'].values[np.isnan(data['psy'].values)] = 0.0
            data['psy'] = data['psy'].values * 100
            data.loc[:, 'psyma'] = tl.MA(data['psy'].values, timeperiod=6)

            # BRAR：情绪指标
            data.loc[:, 'h_o'] = data['high'].values - data['open'].values
            data.loc[:, 'o_l'] = data['open'].values - data['low'].values
            data.loc[:, 'h_o_sum'] = tl.SUM(data['h_o'].values, timeperiod=26)
            data.loc[:, 'o_l_sum'] = tl.SUM(data['o_l'].values, timeperiod=26)
            data.loc[:, 'ar'] = data['h_o_sum'] .values / data['o_l_sum'].values
            data['ar'].values[np.isnan(data['ar'].values)] = 0.0
            data['ar'].values[np.isinf(data['ar'].values)] = 0.0
            data['ar'] = data['ar'].values * 100
            data.loc[:, 'h_cy_sum'] = tl.SUM(data['h_cy'].values, timeperiod=26)
            data.loc[:, 'cy_l_sum'] = tl.SUM(data['cy_l'].values, timeperiod=26)
            data.loc[:, 'br'] = data['h_cy_sum'].values / data['cy_l_sum'].values
            data['br'].values[np.isnan(data['br'].values)] = 0.0
            data['br'].values[np.isinf(data['br'].values)] = 0.0
            data['br'] = data['br'].values * 100

            # EMV：简易波动指标
            data.loc[:, 'prev_high'] = data['high'].shift(1, fill_value=0.0).values
            data.loc[:, 'prev_low'] = data['low'].shift(1, fill_value=0.0).values
            data.loc[:, 'phl_avg'] = (data['prev_high'].values + data['prev_low'].values) / 2.0
            data.loc[:, 'emva_em'] = (data['hl_avg'].values - data['phl_avg'].values) * data['h_l'].values / data['amount'].values
            data.loc[:, 'emv'] = tl.SUM(data['emva_em'].values, timeperiod=14)
            data['emv'].values[np.isnan(data['emv'].values)] = 0.0
            data.loc[:, 'emva'] = tl.MA(data['emv'].values, timeperiod=9)
            data['emva'].values[np.isnan(data['emva'].values)] = 0.0

            # BIAS：乖离率
            data.loc[:, 'ma6'] = tl.MA(data['close'].values, timeperiod=6)
            data['ma6'].values[np.isnan(data['ma6'].values)] = 0.0
            data.loc[:, 'ma12'] = tl.MA(data['close'].values, timeperiod=12)
            data['ma12'].values[np.isnan(data['ma12'].values)] = 0.0
            data.loc[:, 'ma24'] = tl.MA(data['close'].values, timeperiod=24)
            data['ma24'].values[np.isnan(data['ma24'].values)] = 0.0
            data.loc[:, 'bias'] = ((data['close'].values - data['ma6'].values) / data['ma6'].values)
            data['bias'].values[np.isnan(data['bias'].values)] = 0.0
            data['bias'].values[np.isinf(data['bias'].values)] = 0.0
            data['bias'] = data['bias'].values * 100
            data.loc[:, 'bias_12'] = (data['close'].values - data['ma12'].values) / data['ma12'].values
            data['bias_12'].values[np.isnan(data['bias_12'].values)] = 0.0
            data['bias_12'].values[np.isinf(data['bias_12'].values)] = 0.0
            data['bias_12'] = data['bias_12'].values * 100
            data.loc[:, 'bias_24'] = (data['close'].values - data['ma24'].values) / data['ma24'].values
            data['bias_24'].values[np.isnan(data['bias_24'].values)] = 0.0
            data['bias_24'].values[np.isinf(data['bias_24'].values)] = 0.0
            data['bias_24'] = data['bias_24'].values * 100

            # DPO：区间震荡线
            data.loc[:, 'c_m_11'] = tl.MA(data['close'].values, timeperiod=11)
            data.loc[:, 'dpo'] = data['close'].values - data['c_m_11'].shift(1, fill_value=0.0).values
            data['dpo'].values[np.isnan(data['dpo'].values)] = 0.0
            data.loc[:, 'madpo'] = tl.MA(data['dpo'].values, timeperiod=6)
            data['madpo'].values[np.isnan(data['madpo'].values)] = 0.0

            # VHF：纵横指标
            data.loc[:, 'hcp_lcp'] = tl.MAX(data['close'].values, timeperiod=28) - tl.MIN(data['close'].values, timeperiod=28)
            data['hcp_lcp'].values[np.isnan(data['hcp_lcp'].values)] = 0.0
            data.loc[:, 'vhf'] = np.divide(data['hcp_lcp'].values, tl.SUM(abs(data['close'].values - data['prev_close'].values), timeperiod=28))
            data['vhf'].values[np.isnan(data['vhf'].values)] = 0.0

            # RVI：相对活力指数
            data.loc[:, 'rvi_x'] = ((data['close'].values - data['open'].values) +
                                    2 * (data['prev_close'].values - data['open'].shift(1, fill_value=0.0).values) +
                                    2 * (data['close'].shift(2, fill_value=0.0).values - data['open'].shift(2, fill_value=0.0).values) +
                                    (data['close'].shift(3, fill_value=0.0).values - data['open'].shift(3, fill_value=0.0).values)) / 6
            data.loc[:, 'rvi_y'] = ((data['high'].values - data['low'].values) +
                                    2 * (data['prev_high'].values - data['prev_low'].values) +
                                    2 * (data['high'].shift(2, fill_value=0.0).values - data['low'].shift(2, fill_value=0.0).values) +
                                    (data['high'].shift(3, fill_value=0.0).values - data['low'].shift(3, fill_value=0.0).values)) / 6
            data.loc[:, 'rvi'] = tl.MA(data['rvi_x'].values, timeperiod=10) / tl.MA(data['rvi_y'].values, timeperiod=10)
            data['rvi'].values[np.isnan(data['rvi'].values)] = 0.0
            data['rvi'].values[np.isinf(data['rvi'].values)] = 0.0
            data.loc[:, 'rvis'] = (data['rvi'].values +
                                   2 * data['rvi'].shift(1, fill_value=0.0).values +
                                   2 * data['rvi'].shift(2, fill_value=0.0).values +
                                   data['rvi'].shift(3, fill_value=0.0).values) / 6

            # FI：力度指标
            data.loc[:, 'fi'] = np.insert(np.diff(data['close'].values), 0, 0.0) * data['volume'].values
            data.loc[:, 'force_2'] = tl.EMA(data['fi'].values, timeperiod=2)
            data['force_2'].values[np.isnan(data['force_2'].values)] = 0.0
            data.loc[:, 'force_13'] = tl.EMA(data['fi'].values, timeperiod=13)
            data['force_13'].values[np.isnan(data['force_13'].values)] = 0.0

            # ENE：轨道线
            data.loc[:, 'ene_ue'] = (1 + 11 / 100) * data['ma10'].values
            data.loc[:, 'ene_le'] = (1 - 9 / 100) * data['ma10'].values
            data.loc[:, 'ene'] = (data['ene_ue'].values + data['ene_le'].values) / 2

            # VOL：成交量均线
            data.loc[:, 'vol_5'] = tl.MA(data['volume'].values, timeperiod=5)
            data['vol_5'].values[np.isnan(data['vol_5'].values)] = 0.0
            data.loc[:, 'vol_10'] = tl.MA(data['volume'].values, timeperiod=10)
            data['vol_10'].values[np.isnan(data['vol_10'].values)] = 0.0

            # MA：移动平均线
            data.loc[:, 'ma20'] = tl.MA(data['close'].values, timeperiod=20)
            data['ma20'].values[np.isnan(data['ma20'].values)] = 0.0
            data.loc[:, 'ma200'] = tl.MA(data['close'].values, timeperiod=200)
            data['ma200'].values[np.isnan(data['ma200'].values)] = 0.0

        # ==================== 步骤3: 返回结果 ====================
        # 如果指定了threshold，只返回最后N行
        if threshold is not None:
            data = data.tail(n=threshold).copy()
        return data
        
    except Exception as e:
        if data is None or data['code'] is None:
            logging.error(f"calculate_indicator.get_indicators处理异常：代码{e}")
        else:
            logging.error(f"calculate_indicator.get_indicators处理异常：{data['code']}代码{e}")
    return None


# ==================== 获取单只股票的指标 ====================

"""
获取单只股票某一天的所有指标值
参数说明：
code_name (tuple): 股票信息(date, code, name)
data (DataFrame): 该股票的历史K线数据
stock_column (list): 列名列表
date (datetime.date, 可选): 计算日期
calc_threshold (int): 计算数据的行数，默认90天
返回值：
pandas.Series: 该股票当天的所有指标值
功能说明：
1. 调用get_indicators()计算所有指标
2. 提取最后一天的指标值
3. 返回Series格式
为什么用Series？
- Series是pandas的一维数据结构
- 类似字典，有索引和值
- 方便后续合并为DataFrame
使用场景：
indicators_data_daily_job中调用
并行计算所有股票的指标
"""
def get_indicator(code_name, data, stock_column, date=None, calc_threshold=90):
    try:
        # 步骤1: 确定计算日期
        if date is None:
            end_date = code_name[0]  # 使用stock的日期
        else:
            end_date = date.strftime("%Y-%m-%d")

        code = code_name[1]  # 提取代码
        
        # 步骤2: 准备返回数组
        stock_data_list = [end_date, code]  # 开头是date和code
        columns_num = len(stock_column) - 2  # 减去date和code
        
        # 步骤3: 检查数据是否充足
        if len(data.index) <= 1:
            # 数据不足，返回全0
            for i in range(columns_num):
                stock_data_list.append(0)
            return pd.Series(stock_data_list, index=stock_column)

        # 步骤4: 计算指标
        # end_date：只计算到这个日期
        # threshold=1：只返回最后1天
        # calc_threshold：用最后90天数据计算（提高速度）
        idr_data = get_indicators(data, end_date=end_date, threshold=1, calc_threshold=calc_threshold)

        # 步骤5: 检查计算结果
        if idr_data is None:
            # 计算失败，返回全0
            for i in range(columns_num):
                stock_data_list.append(0)
            return pd.Series(stock_data_list, index=stock_column)

        # 步骤6: 提取所有指标值
        for i in range(columns_num):
            # 获取第i+2列的值（跳过date和code）
            # tail(1)：取最后一行
            # values[0]：取第一个值
            tmp_val = idr_data[stock_column[i + 2]].tail(1).values[0]
            
            # 检查是否是异常值
            if np.isinf(tmp_val) or np.isnan(tmp_val):
                stock_data_list.append(0)  # 异常值用0代替
            else:
                stock_data_list.append(tmp_val)  # 正常值

        # 步骤7: 返回Series
        return pd.Series(stock_data_list, index=stock_column)
        
    except Exception as e:
        logging.error(f"calculate_indicator.get_indicator处理异常：{code}代码{e}")
    return None


"""
===========================================
技术指标计算模块使用总结（给Python新手）
===========================================

1. 模块定位
   - 第三层核心：技术指标层
   - 最复杂的计算模块
   - 32种技术指标
   - 使用TA-Lib专业库

2. 核心函数
   - get_indicators()：计算所有指标【核心】
   - get_indicator()：获取单只股票指标

3. 技术指标分类
   趋势类：MACD、DMA、TEMA
   摆动类：KDJ、RSI、CCI、WR
   通道类：BOLL、ENE
   能量类：CR、VR、BRAR、MFI
   波动类：ATR、DPO、VHF
   趋向类：DMI、TRIX

4. 计算原理
   - 移动平均：平滑数据
   - 指数平滑：重视近期
   - 相对强弱：比较力量
   - 随机摆动：判断位置
   - 通道突破：支撑压力

5. TA-Lib库
   优点：
   - 专业准确
   - 高效快速
   - 功能全面
   - 简单易用
   
   常用函数：
   - tl.MACD()：MACD指标
   - tl.RSI()：RSI指标
   - tl.MA()：移动平均
   - tl.EMA()：指数移动平均
   - tl.SUM()：求和
   - tl.MAX/MIN()：最大最小值

6. 异常处理
   NaN：计算不出的值
   - 数据不足
   - 公式除以0
   - 处理：替换为0
   
   Inf：无穷大
   - 除以0的结果
   - 处理：替换为0
   
   检查方法：
   - np.isnan()：是否NaN
   - np.isinf()：是否Inf

7. NumPy操作
   向量化计算：
   - 不用循环，直接数组运算
   - 速度快，代码简洁
   
   常用函数：
   - np.where()：条件筛选
   - np.diff()：差分
   - np.insert()：插入元素

8. Pandas操作
   DataFrame：
   - 二维表格数据
   - 有行索引和列索引
   
   常用操作：
   - data.loc[:, 'col']：设置列
   - data['col'].values：获取数组
   - data.shift()：移动
   - data.tail()：取最后几行
   - data.copy()：复制

9. 性能优化
   - 向量化计算：避免循环
   - TA-Lib C库：高效实现
   - calc_threshold：只用必要数据
   - 多线程：并行计算

10. 使用场景
    - 技术分析：查看指标图表
    - 量化选股：根据指标筛选
    - 策略回测：验证指标策略
    - 实盘交易：发现交易机会

11. 注意事项
    - 数据充足：至少需要250天
    - 处理异常：NaN和Inf
    - 结果验证：与专业软件对比
    - 参数调整：根据需要修改周期

12. 调试技巧
    - 打印中间结果：print(data[['date', 'close', 'macd']].tail())
    - 对比专业软件：同花顺、通达信
    - 查看文档：TA-Lib官方文档
    - 单步调试：IDE调试功能

13. 扩展建议
    - 添加新指标：使用TA-Lib其他函数
    - 自定义指标：编写自己的计算公式
    - 优化参数：调整周期和阈值
    - 组合指标：多个指标综合判断
"""
