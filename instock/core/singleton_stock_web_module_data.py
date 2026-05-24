#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import instock.core.tablestructure as tbs
from instock.lib.singleton_type import singleton_type
import instock.core.web_module_data as wmd

__author__ = 'myh '
__date__ = '2023/3/10 '

# 策略详细说明字典
MODULE_DESCRIPTIONS = {
    # ==================== 综合选股 ====================
    'cn_stock_selection': '综合选股：\n\n基于200+个选股指标的综合筛选系统，涵盖基本面、技术面、资金面等多个维度。\n\n核心功能：\n• 市盈率、市净率等估值指标\n• ROE、净利润增长率等财务指标\n• MACD、KDJ等技术指标\n• 主力资金流向\n• 机构持股情况\n• 龙虎榜数据\n\n使用方法：\n通过设置各指标的筛选条件，快速找到符合要求的股票。',
    
    # ==================== 股票基本数据 ====================
    'cn_stock_spot': '每日股票数据：\n\n展示A股市场的实时行情数据，包括价格、成交量、涨跌幅等基础信息。\n\n核心字段：\n• 代码、名称\n• 最新价、涨跌幅、涨跌额\n• 成交量、成交额\n• 开盘价、最高价、最低价\n• 昨收价\n• 换手率、量比\n• 市盈率、市净率\n• 总市值、流通市值\n\n数据来源：东方财富网实时行情',
    
    'cn_stock_chip_race_open': '早盘抢筹：\n\n监控早盘阶段（9:30-10:00）主力资金的抢筹行为，捕捉短线机会。\n\n核心要点：\n• 监测时间段：开盘后30分钟\n• 关注大单买入比例\n• 结合股价走势判断\n• 适合短线交易\n\n注意：需要盘中实时数据支持',
    
    'cn_stock_chip_race_end': '尾盘抢筹：\n\n监控尾盘阶段（14:30-15:00）主力资金的抢筹行为，为次日行情做准备。\n\n核心要点：\n• 监测时间段：收盘前30分钟\n• 关注尾盘放量拉升\n• 结合全天走势判断\n• 适合隔日交易\n\n注意：这是收盘后数据，非实时',
    
    'cn_stock_limitup_reason': '涨停原因：\n\n记录每只涨停股票的详细原因和概念题材，帮助理解涨停背后的逻辑。\n\n核心内容：\n• 涨停时间\n• 涨停原因分类\n• 所属概念板块\n• 相关消息面\n• 连板情况\n\n用途：分析市场热点和资金偏好',
    
    'cn_stock_fund_flow': '个股资金流向：\n\n展示每只股票的资金流入流出情况，包括超大单、大单、中单、小单的分布。\n\n核心指标：\n• 主力净流入\n• 超大单净流入\n• 大单净流入\n• 中单净流入\n• 小单净流入\n• 净流入占比\n\n用途：判断主力资金动向',
    
    'cn_stock_bonus': '分红配送：\n\n记录上市公司的分红送股信息，包括现金分红、送股、转增等。\n\n核心内容：\n• 每股分红金额\n• 送股比例\n• 转增比例\n• 股权登记日\n• 除权除息日\n• 红利发放日\n\n用途：价值投资参考',
    
    'cn_stock_lhb': '龙虎榜明细：\n\n展示每日登上龙虎榜的股票的详细交易数据，包括买卖前五的营业部。\n\n核心内容：\n• 上榜原因\n• 买入金额前五营业部\n• 卖出金额前五营业部\n• 机构专用席位\n• 游资动向\n\n用途：跟踪主力和游资操作',
    
    'cn_stock_blocktrade': '大宗交易：\n\n记录上市公司的大宗交易信息，通常是大股东或机构的批量买卖。\n\n核心内容：\n• 成交价格\n• 成交数量\n• 成交金额\n• 买方营业部\n• 卖方营业部\n• 溢价率\n\n用途：判断大股东态度',
    
    'cn_stock_fund_flow_industry': '行业资金流向：\n\n展示各行业的资金流入流出情况，帮助判断板块热度。\n\n核心指标：\n• 行业名称\n• 主力净流入\n• 领涨股票\n• 行业涨跌幅\n• 资金流入排名\n\n用途：把握板块轮动节奏',
    
    'cn_stock_fund_flow_concept': '概念资金流向：\n\n展示各概念板块的资金流入流出情况，追踪市场热点。\n\n核心指标：\n• 概念名称\n• 主力净流入\n• 领涨股票\n• 概念涨跌幅\n• 资金流入排名\n\n用途：发现市场主线题材',
    
    'cn_etf_spot': 'ETF实时行情：\n\n展示交易所交易基金(ETF)的实时行情数据。\n\n核心字段：\n• ETF代码、名称\n• 最新价、涨跌幅\n• 成交量、成交额\n• 净值、溢价率\n• 持仓规模\n\n用途：指数投资和套利交易',
    
    # ==================== 股票指标数据 ====================
    'cn_stock_indicators': '技术指标：\n\n计算并展示75种常用技术分析指标，包括趋势、震荡、成交量等各类指标。\n\n主要指标：\n• MA均线系统（5/10/20/60/250日）\n• MACD（平滑异同移动平均线）\n• KDJ（随机指标）\n• RSI（相对强弱指标）\n• BOLL（布林带）\n• VOL（成交量指标）\n• CCI（顺势指标）\n• DMA（平均线差）\n• OBV（能量潮）\n• SAR（抛物线转向）\n\n用途：技术分析和买卖点判断',
    
    'cn_stock_indicators_buy': '指标买入信号：\n\n基于技术指标发出的买入信号汇总，多个指标同时发出买入信号时可靠性更高。\n\n筛选条件（8个指标同时超买）：\n• KDJ_K ≥ 80, KDJ_D ≥ 70, KDJ_J ≥ 100\n• RSI_6 ≥ 80\n• CCI ≥ 100\n• CR ≥ 300\n• WR_6 ≥ -20\n• VR ≥ 160\n\n新增功能：\n✅ 显示当日涨跌幅\n✅ 显示所处行业\n✅ 回测收益率（rate_1 ~ rate_100）\n\n用途：寻找技术面买点，短线追涨策略',
    
    'cn_stock_indicators_sell': '指标卖出信号：\n\n基于技术指标发出的卖出信号汇总，及时止盈止损。\n\n筛选条件（8个指标同时超卖）：\n• KDJ_K < 20, KDJ_D < 30, KDJ_J < 10\n• RSI_6 < 20\n• CCI < -100\n• CR < 40\n• WR_6 < -80\n• VR < 40\n\n新增功能：\n✅ 显示当日涨跌幅\n✅ 显示所处行业\n✅ 回测收益率（rate_1 ~ rate_100）\n\n用途：寻找技术面卖点，超跌反弹策略',
    
    # ==================== 股票K线形态 ====================
    'cn_stock_pattern': 'K线形态识别：\n\n自动识别61种经典K线形态，包括反转形态、持续形态等。\n\n主要形态：\n• 早晨之星/黄昏之星\n• 红三兵/黑三兵\n• 十字星系列\n• 吞没形态\n• 锤子线/吊颈线\n• 乌云盖顶/曙光初现\n• 身怀六甲\n• 三只乌鸦/三个白兵\n\n用途：形态分析和趋势判断',
    
    # ==================== 股票策略数据 ====================
    'cn_stock_spot_buy': '股票指标买入：\n\n综合各项指标和技术形态，筛选出具有买入机会的股票。\n\n筛选条件：\n• 技术指标发出买入信号\n• K线形态看涨\n• 资金面支持\n• 基本面良好\n\n用途：一站式选股参考',
    
    'cn_stock_strategy_enter': '放量上涨策略：\n\n当股票成交量显著放大，同时价格出现明显上涨时触发。这种形态通常预示着主力资金开始介入，是短线买入的良机。\n\n核心要点：\n• 成交量较前几日放大50%以上\n• 价格上涨超过3%\n• 配合技术指标确认',
    
    'cn_stock_strategy_keep_increasing': '均线多头排列策略：\n\n当短期、中期、长期均线呈现多头排列（短>中>长）时触发。这表明股票处于上升趋势中，适合中线持有。\n\n核心要点：\n• 5日均线 > 10日均线 > 20日均线\n• 均线系统向上发散\n• 股价站稳均线之上',
    
    'cn_stock_strategy_parking_apron': '停机坪策略：\n\n股票在上涨过程中出现短暂横盘整理，形成类似“停机坪”的形态后继续上涨。这是强势股的典型特征。\n\n核心要点：\n• 前期有明显上涨\n• 横盘整理时间较短\n• 整理期间成交量萎缩\n• 突破整理平台后继续上涨',
    
    'cn_stock_strategy_backtrace_ma250': '回踩年线策略：\n\n股价回调至250日均线（年线）附近获得支撑后反弹。年线是重要的长期支撑位，回踩不破是买入机会。\n\n核心要点：\n• 股价回落至250日均线附近\n• 在年线处获得支撑\n• 成交量萎缩后再次放大\n• 中长期投资价值较高',
    
    'cn_stock_strategy_breakthrough_platform': '突破平台策略：\n\n股价在某一区间内长时间横盘整理后，突然放量突破平台高点。这通常意味着新的上涨行情即将开始。\n\n核心要点：\n• 横盘整理时间较长（至少2周）\n• 突破时成交量明显放大\n• 突破幅度超过3%\n• 突破后站稳平台上方',
    
    'cn_stock_strategy_low_backtrace_increase': '低位回踩上涨策略：\n\n股票在相对低位出现回踩确认后重新上涨。这是一种稳健的买入策略，风险相对较低。\n\n核心要点：\n• 股价处于相对低位\n• 回踩确认支撑有效\n• 成交量温和放大\n• 技术指标出现底背离',
    
    'cn_stock_strategy_turtle_trade': '海龟交易策略：\n\n基于经典的海龟交易法则，当股价突破N日高点时买入。这是一种趋势跟踪策略，适合捕捉大行情。\n\n核心要点：\n• 突破20日或55日高点\n• 严格的风险控制\n• 金字塔式加仓\n• 趋势反转时止损',
    
    'cn_stock_strategy_high_tight_flag': '高紧旗形策略：\n\n股票快速上涨后出现短暂的紧密整理，形成旗形形态。这是强势股的延续信号。\n\n核心要点：\n• 前期涨幅较大（通常>30%）\n• 整理时间短（5-10天）\n• 整理幅度小（通常<15%）\n• 成交量在整理期萎缩',
    
    'cn_stock_strategy_climax_limitdown': '跌停板抄底策略：\n\n股票出现恐慌性跌停后，在低位企稳反弹。这是一种逆向投资策略，风险较高但收益潜力大。\n\n核心要点：\n• 出现非理性跌停\n• 基本面未发生恶化\n• 跌停后成交量萎缩\n• 出现止跌企稳信号',
    
    'cn_stock_strategy_low_atr': '低波动率策略：\n\n选择ATR（平均真实波幅）较低的股票，这类股票波动较小，适合稳健型投资者。\n\n核心要点：\n• ATR值处于历史低位\n• 股价波动幅度小\n• 适合长线持有\n• 风险控制较好'
}



class stock_web_module_data(metaclass=singleton_type):
    def __init__(self):
        _data = {}
        self.data_list = [wmd.web_module_data(
            mode="query",
            type="综合选股",
            ico="fa fa-desktop",
            name=tbs.TABLE_CN_STOCK_SELECTION['cn'],
            table_name=tbs.TABLE_CN_STOCK_SELECTION['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_SELECTION['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_SELECTION['columns']),
            primary_key=[],
            is_realtime=False,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_SELECTION['name']}`.`code`) AS `cdatetime`",
            order_by=" `cdatetime` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_SELECTION['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_SPOT['cn'],
            table_name=tbs.TABLE_CN_STOCK_SPOT['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_SPOT['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_SPOT['columns']),
            primary_key=[],
            is_realtime=True,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_SPOT['name']}`.`code`) AS `cdatetime`",
            order_by=" `cdatetime` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_SPOT['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['cn'],
            table_name=tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['columns']),
            primary_key=[],
            is_realtime=True,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['name']}`.`code`) AS `cdatetime`",
            order_by=" `bid_trust_amount` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_CHIP_RACE_END['cn'],
            table_name=tbs.TABLE_CN_STOCK_CHIP_RACE_END['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_CHIP_RACE_END['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_CHIP_RACE_END['columns']),
            primary_key=[],
            is_realtime=False,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_CHIP_RACE_END['name']}`.`code`) AS `cdatetime`",
            order_by=" `bid_trust_amount` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_CHIP_RACE_END['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_LIMITUP_REASON['cn'],
            table_name=tbs.TABLE_CN_STOCK_LIMITUP_REASON['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_LIMITUP_REASON['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_LIMITUP_REASON['columns']),
            primary_key=[],
            is_realtime=True,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_LIMITUP_REASON['name']}`.`code`) AS `cdatetime`",
            order_by=" `cdatetime` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_LIMITUP_REASON['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_FUND_FLOW['cn'],
            table_name=tbs.TABLE_CN_STOCK_FUND_FLOW['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_FUND_FLOW['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_FUND_FLOW['columns']),
            primary_key=[],
            is_realtime=True,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_FUND_FLOW['name']}`.`code`) AS `cdatetime`",
            order_by=" `cdatetime` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_FUND_FLOW['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_BONUS['cn'],
            table_name=tbs.TABLE_CN_STOCK_BONUS['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_BONUS['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_BONUS['columns']),
            primary_key=[],
            is_realtime=True,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_BONUS['name']}`.`code`) AS `cdatetime`",
            order_by=" `cdatetime` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_BONUS['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_LHB['cn'],
            table_name=tbs.TABLE_CN_STOCK_LHB['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_LHB['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_LHB['columns']),
            primary_key=[],
            is_realtime=True,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_LHB['name']}`.`code`) AS `cdatetime`",
            order_by=" `cdatetime`,`ranking_date` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_LHB['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_BLOCKTRADE['cn'],
            table_name=tbs.TABLE_CN_STOCK_BLOCKTRADE['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_BLOCKTRADE['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_BLOCKTRADE['columns']),
            primary_key=[],
            is_realtime=False,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_BLOCKTRADE['name']}`.`code`) AS `cdatetime`",
            order_by=" `cdatetime` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_BLOCKTRADE['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['cn'],
            table_name=tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['columns']),
            primary_key=[],
            is_realtime=True,
            order_by=" `fund_amount` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT['cn'],
            table_name=tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT['columns']),
            primary_key=[],
            is_realtime=True,
            order_by=" `fund_amount` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_ETF_SPOT['cn'],
            table_name=tbs.TABLE_CN_ETF_SPOT['name'],
            columns=tuple(tbs.TABLE_CN_ETF_SPOT['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_ETF_SPOT['columns']),
            primary_key=[],
            is_realtime=True,
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_ETF_SPOT['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_CN_STOCK_INDICATORS['cn'],
            table_name=tbs.TABLE_CN_STOCK_INDICATORS['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_INDICATORS['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_INDICATORS['columns']),
            primary_key=[],
            is_realtime=False,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_INDICATORS['name']}`.`code`) AS `cdatetime`",
            order_by=" `cdatetime` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_INDICATORS['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_CN_STOCK_INDICATORS_BUY['cn'],
            table_name=tbs.TABLE_CN_STOCK_INDICATORS_BUY['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_INDICATORS_BUY['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_INDICATORS_BUY['columns']),
            primary_key=[],
            is_realtime=False,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_INDICATORS_BUY['name']}`.`code`) AS `cdatetime`",
            order_by=" `cdatetime` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_INDICATORS_BUY['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_CN_STOCK_INDICATORS_SELL['cn'],
            table_name=tbs.TABLE_CN_STOCK_INDICATORS_SELL['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_INDICATORS_SELL['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_INDICATORS_SELL['columns']),
            primary_key=[],
            is_realtime=False,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_INDICATORS_SELL['name']}`.`code`) AS `cdatetime`",
            order_by=" `cdatetime` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_INDICATORS_SELL['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票K线形态",
            ico="fa fa-tag",
            name=tbs.TABLE_CN_STOCK_KLINE_PATTERN['cn'],
            table_name=tbs.TABLE_CN_STOCK_KLINE_PATTERN['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_KLINE_PATTERN['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_KLINE_PATTERN['columns']),
            primary_key=[],
            is_realtime=False,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_KLINE_PATTERN['name']}`.`code`) AS `cdatetime`",
            order_by=" `cdatetime` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_KLINE_PATTERN['name'], '')
        ), wmd.web_module_data(
            mode="query",
            type="股票策略数据",
            ico="fa fa-check-square-o",
            name=tbs.TABLE_CN_STOCK_SPOT_BUY['cn'],
            table_name=tbs.TABLE_CN_STOCK_SPOT_BUY['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_SPOT_BUY['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_SPOT_BUY['columns']),
            primary_key=[],
            is_realtime=False,
            order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_SPOT_BUY['name']}`.`code`) AS `cdatetime`",
            order_by=" `cdatetime` DESC",
            description=MODULE_DESCRIPTIONS.get(tbs.TABLE_CN_STOCK_SPOT_BUY['name'], '')
        )]

        for table in tbs.TABLE_CN_STOCK_STRATEGIES:
            # 获取策略说明
            description = MODULE_DESCRIPTIONS.get(table['name'], '')
            
            self.data_list.append(
                wmd.web_module_data(
                    mode="query",
                    type="股票策略数据",
                    ico="fa fa-check-square-o",
                    name=table['cn'],
                    table_name=table['name'],
                    columns=tuple(table['columns']),
                    column_names=tbs.get_field_cns(table['columns']),
                    primary_key=[],
                    is_realtime=False,
                    order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{table['name']}`.`code`) AS `cdatetime`",
                    order_by=" `cdatetime` DESC",
                    description=description  # 添加策略说明
                )
            )
        for tmp in self.data_list:
            _data[tmp.table_name] = tmp
        self.data = _data

    def get_data_list(self):
        return self.data_list

    def get_data(self, name):
        return self.data[name]
