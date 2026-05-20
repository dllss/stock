#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
Web指标数据处理器 - K线图表和关注功能
=======================================

功能说明：
本模块提供两个Web Handler，用于处理股票技术指标展示和关注功能：

1. GetDataIndicatorsHandler - 获取股票K线图表数据
   - 接收股票代码、日期、名称参数
   - 从数据库或API获取股票历史数据
   - 调用visualization生成交互式K线图表
   - 渲染stock_indicators.html页面

2. SaveCollectHandler - 管理股票关注列表
   - 接收股票代码和操作类型（关注/取关）
   - 操作cn_stock_attention表
   - 支持添加关注和取消关注
   - 返回JSON结果

核心流程：

A. K线图表展示流程：
   1. 用户访问URL：/stock_indicators?code=000001&date=2024-01-01&name=平安银行
   2. Tornado路由匹配到GetDataIndicatorsHandler
   3. Handler获取参数（code, date, name）
   4. 判断是否为ETF（代码以1或5开头）
   5. 调用相应的数据获取函数：
      - ETF：stf.fetch_etf_hist()
      - 股票：stf.fetch_stock_hist()
   6. 获取股票历史数据（DataFrame）
   7. 调用vis.get_plot_kline()生成图表
   8. get_plot_kline内部：
      - 计算32种技术指标
      - 识别61种K线形态
      - 准备筹码分布数据
      - 创建Bokeh交互式图表
      - 返回{script, div}
   9. 渲染stock_indicators.html模板
   10. 传入comp_list（包含图表组件）
   11. 前端显示完整的K线图表页面

B. 关注功能流程：
   1. 用户点击"关注"按钮
   2. 前端调用JavaScript函数attention()
   3. AJAX请求：/save_collect?code=000001&otype=0
   4. Tornado路由匹配到SaveCollectHandler
   5. Handler获取参数（code, otype）
   6. 根据otype执行不同操作：
      - otype='1'：取消关注（DELETE）
      - otype='0'：添加关注（INSERT）
   7. 使用参数化查询防止SQL注入
   8. 返回JSON结果
   9. 前端更新按钮状态

数据表结构：

cn_stock_attention（关注表）：
- datetime：关注时间（datetime）
- code：股票代码（varchar）

示例数据：
+---------------------+--------+
| datetime            | code   |
+---------------------+--------+
| 2024-01-01 10:30:00 | 000001 |
| 2024-01-02 14:20:00 | 600519 |
+---------------------+--------+

依赖关系：
- tornado.web：Web框架
- instock.core.stockfetch：数据抓取模块
- instock.core.kline.visualization：图表生成模块
- instock.web.base：基础Handler类
- instock.core.tablestructure：表结构定义
- instock.lib.database：数据库操作

使用场景：
1. 用户在Web端查看股票K线图
2. 用户关注/取消关注某只股票
3. 技术分析和形态识别
4. 投资决策辅助

注意事项：
1. 数据获取可能失败（网络问题、代码错误等）
2. 图表生成需要较长时间（大量计算）
3. SQL注入防护：使用参数化查询
4. ETF和股票的数据来源不同
5. 异常处理要完善，避免页面崩溃

性能优化：
1. 图表生成是CPU密集型操作，考虑异步处理
2. 可以使用缓存减少重复计算
3. 大数据量时限制时间范围
4. 关注表添加索引提高查询速度

安全考虑：
1. 使用参数化查询防止SQL注入
2. 验证股票代码格式
3. 限制请求频率
4. 记录操作日志

错误处理：
1. 数据获取失败：返回空页面或错误提示
2. 图表生成失败：捕获异常，记录日志
3. 数据库操作失败：回滚事务，返回错误
4. 网络超时：设置超时时间，重试机制
"""

from abc import ABC
from tornado import gen
import logging
import instock.core.stockfetch as stf
import instock.core.kline.visualization as vis
import instock.web.base as webBase

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== K线图表数据处理器 ====================

"""
GetDataIndicatorsHandler - 股票K线图表页面处理器

继承关系：
- webBase.BaseHandler：自定义的基础Handler（提供db连接等）
- ABC：抽象基类（标记为抽象类）

功能：
处理用户请求，生成包含K线图表的股票技术分析页面

请求参数（URL参数）：
- code (str): 股票代码
  * 6位数字代码
  * 例如："000001"（平安银行）、"600519"（贵州茅台）
  * ETF代码以1或5开头（如"510300"）

- date (str): 查询日期
  * 格式：YYYY-MM-DD
  * 例如："2024-01-01"
  * 用于确定计算指标的截止日期

- name (str): 股票名称
  * 用于显示在页面标题中
  * 例如："平安银行"、"贵州茅台"

响应：
渲染stock_indicators.html模板，包含：
- comp_list：图表组件列表（包含script和div）
- leftMenu：左侧菜单HTML

执行流程：
1. 获取URL参数（code, date, name）
2. 初始化结果列表comp_list
3. 判断是否为ETF（代码以1或5开头）
4. 获取股票历史数据：
   - ETF：调用stf.fetch_etf_hist()
   - 股票：调用stf.fetch_stock_hist()
5. 检查数据是否成功获取
6. 调用vis.get_plot_kline()生成图表
7. 检查图表是否成功生成
8. 将图表组件添加到comp_list
9. 渲染模板并返回HTML页面

异常处理：
- 使用try-except捕获所有异常
- 记录错误日志（logging.error）
- 出错时返回空页面（不中断程序）

数据流：
URL参数 → 获取历史数据 → 生成图表 → 渲染模板 → HTML页面

使用示例：
URL访问：
http://localhost:9988/stock_indicators?code=000001&date=2024-01-01&name=平安银行

前端JavaScript调用：
window.location.href = `/stock_indicators?code=${code}&date=${date}&name=${name}`;

Tornado模板使用：
```html
{{ raw comp_list[0]["script"] }}
{{ raw comp_list[0]["div"] }}
```

注意事项：
1. code必须以字符串形式传递（避免前导0丢失）
2. date必须是有效的交易日期
3. name用于显示，不影响数据处理
4. ETF和股票使用不同的数据源
5. 图表生成可能需要几秒到几十秒
6. 大数据量时建议限制时间范围

性能考虑：
1. get_plot_kline是CPU密集型操作
2. 包含75种指标计算 + 61种形态识别
3. 建议：
   - 使用异步任务队列（Celery）
   - 缓存已计算的图表
   - 限制同时生成的图表数量
   - 前端显示加载动画

常见问题：

Q: 为什么需要区分ETF和股票？
A: ETF和股票的数据结构、API接口可能不同

Q: comp_list为什么是列表？
A: 设计为支持多个图表，目前只用第一个

Q: 如果数据获取失败会怎样？
A: 函数提前返回（return），页面显示空白

Q: 可以自定义图表样式吗？
A: 可以，修改visualization.py中的配置

Q: 如何添加新的指标？
A: 
1. 在calculate_indicator.py中添加计算函数
2. 在indicator_web_dic.py中添加配置
3. 重启服务即可
"""
class GetDataIndicatorsHandler(webBase.BaseHandler, ABC):
    """
    处理GET请求，生成股票K线图表页面
    
    URL示例：
    /stock_indicators?code=000001&date=2024-01-01&name=平安银行
    
    步骤说明：
    1. 获取参数：从URL中提取code, date, name
    2. 判断类型：检查是否为ETF
    3. 获取数据：调用相应的fetch函数
    4. 生成图表：调用get_plot_kline
    5. 渲染模板：传入图表组件和菜单
    """
    @gen.coroutine
    def get(self):
        """
        Tornado异步GET请求处理方法
        
        @gen.coroutine装饰器：
        - 使方法支持异步操作
        - 可以配合yield使用
        - 提高并发处理能力
        
        注意：虽然这个方法没有使用yield，但保留装饰器以保持一致性
        """
        
        # ==================== 步骤1: 获取URL参数 ====================
        # get_argument()从URL参数中获取值
        # default=None：如果没有该参数，返回None
        # strip=False：不去除首尾空格（保留原始格式）
        
        # 股票代码（必需）
        code = self.get_argument("code", default=None, strip=False)
        
        # 查询日期（必需）
        date = self.get_argument("date", default=None, strip=False)
        
        # 股票名称（可选，用于显示）
        name = self.get_argument("name", default=None, strip=False)
        
        # ==================== 步骤2: 初始化结果列表 ====================
        # comp_list用于存储图表组件
        # 目前是列表，设计为支持多个图表
        comp_list = []
        
        try:
            # ==================== 步骤3: 判断股票类型 ====================
            # 检查代码是否以'1'或'5'开头
            # startswith(('1', '5'))：元组参数表示"或"的关系
            # ETF代码示例：510300（沪深300ETF）、159919（创业板ETF）
            if code.startswith(('1', '5')):
                # === ETF基金 ===
                # 调用ETF历史数据获取函数
                # 参数：(date, code) 元组
                # 返回：DataFrame格式的OHLCV数据
                stock = stf.fetch_etf_hist((date, code))
            else:
                # === 普通股票 ===
                # 调用股票历史数据获取函数
                # 参数：(date, code) 元组
                # 返回：DataFrame格式的OHLCV数据
                stock = stf.fetch_stock_hist((date, code))
            
            # ==================== 步骤4: 检查数据是否成功获取 ====================
            # 如果stock为None，说明数据获取失败
            # 可能原因：
            # - 股票代码错误
            # - 网络问题
            # - API限流
            # - 日期无效
            if stock is None:
                # 提前返回，不渲染页面
                # 用户会看到空白页面或错误提示
                return
            
            # ==================== 步骤5: 生成K线图表 ====================
            # 调用visualization模块的get_plot_kline函数
            # 参数：
            # - code：股票代码
            # - stock：历史数据（DataFrame）
            # - date：查询日期
            # - name：股票名称
            # 
            # 返回：
            # - dict: {"script": JavaScript代码, "div": HTML容器}
            # - None: 生成失败
            #
            # get_plot_kline内部会：
            # 1. 计算32种技术指标
            # 2. 识别61种K线形态
            # 3. 准备筹码分布数据
            # 4. 创建Bokeh交互式图表
            # 5. 返回HTML组件
            pk = vis.get_plot_kline(code, stock, date, name)
            
            # ==================== 步骤6: 检查图表是否成功生成 ====================
            # 如果pk为None，说明图表生成失败
            # 可能原因：
            # - 数据不足（少于360天）
            # - 指标计算错误
            # - Bokeh版本不兼容
            # - JavaScript文件缺失
            if pk is None:
                # 提前返回，不渲染页面
                return
            
            # ==================== 步骤7: 添加图表到结果列表 ====================
            # 将生成的图表组件添加到列表
            # 目前只添加一个图表，但设计为支持多个
            comp_list.append(pk)
        
        except Exception as e:
            # ==================== 步骤8: 异常处理 ====================
            # 捕获所有异常，避免程序崩溃
            # 记录详细的错误信息到日志
            # f-string格式化：将异常信息嵌入字符串
            logging.error(f"dataIndicatorsHandler.GetDataIndicatorsHandler处理异常：{e}")
            
            # 注意：这里没有re-raise异常
            # 所以即使出错，也会继续执行后面的render
            # 但comp_list为空，页面会显示空白
        
        # ==================== 步骤9: 渲染HTML模板 ====================
        # render()方法渲染Tornado模板
        # 参数：
        # - "stock_indicators.html"：模板文件名
        # - comp_list：图表组件列表（传给模板）
        # - leftMenu：左侧菜单HTML（从BaseHandler获取）
        #
        # 模板中会使用这些变量：
        # {{ raw comp_list[0]["script"] }} - 插入JavaScript代码
        # {{ raw comp_list[0]["div"] }} - 插入HTML容器
        # {{ leftMenu }} - 插入左侧菜单
        #
        # GetLeftMenu()根据当前URL生成高亮的菜单
        self.render(
            "stock_indicators.html", 
            comp_list=comp_list,
            leftMenu=webBase.GetLeftMenu(self.request.uri)
        )


# ==================== 股票关注处理器 ====================

"""
SaveCollectHandler - 股票关注/取关处理器

继承关系：
- webBase.BaseHandler：自定义的基础Handler（提供db连接等）
- ABC：抽象基类

功能：
处理用户的股票关注/取关请求，操作cn_stock_attention表

请求参数（URL参数）：
- code (str): 股票代码
  * 6位数字代码
  * 例如："000001"

- otype (str): 操作类型
  * "0"：添加关注（收藏）
  * "1"：取消关注（取消收藏）

响应：
返回JSON格式：{"data":[{}]}
- 成功：空对象
- 失败：也返回空对象（错误记录在日志）

执行流程：
1. 获取URL参数（code, otype）
2. 导入必需的模块（datetime, tablestructure）
3. 获取关注表名
4. 根据otype执行不同操作：
   A. otype='1'（取消关注）：
      - 构造DELETE SQL
      - 使用参数化查询
      - 执行删除操作
   B. otype='0'（添加关注）：
      - 获取当前时间
      - 构造INSERT SQL
      - 使用参数化查询
      - 执行插入操作
5. 返回JSON结果

SQL语句详解：

A. 取消关注（DELETE）：
```sql
DELETE FROM `cn_stock_attention` WHERE `code` = %s
```
- 参数化查询：%s占位符
- 防止SQL注入
- 删除指定代码的所有记录

B. 添加关注（INSERT）：
```sql
INSERT INTO `cn_stock_attention`(`datetime`, `code`) VALUE(%s, %s)
```
- 插入当前时间和股票代码
- 参数化查询：两个%s占位符
- 时间格式：YYYY-MM-DD HH:MM:SS.ffffff

参数化查询的重要性：
❌ 错误做法（SQL注入风险）：
```python
sql = f"DELETE FROM `{table_name}` WHERE `code` = '{code}'"
self.db.query(sql)
```
如果code = "000001'; DROP TABLE cn_stock_attention; --"
会导致灾难性后果！

✅ 正确做法（参数化查询）：
```python
sql = f"DELETE FROM `{table_name}` WHERE `code` = %s"
self.db.query(sql, code)
```
数据库会自动转义特殊字符，保证安全

异常处理：
- 使用try-except捕获数据库异常
- 记录错误到日志（logging.info）
- 仍然返回成功JSON（前端不感知错误）
- 注：当前代码中错误处理被注释掉了

使用场景：

1. 用户点击"关注"按钮：
   - 前端发送：/save_collect?code=000001&otype=0
   - 数据库：INSERT一条记录
   - 按钮变为"取关"

2. 用户点击"取关"按钮：
   - 前端发送：/save_collect?code=000001&otype=1
   - 数据库：DELETE该记录
   - 按钮变为"关注"

前端JavaScript示例：
```javascript
function attention(code, btn) {
    var otype = btn.value;  // 0或1
    $.ajax({
        url: '/save_collect',
        data: {code: code, otype: otype},
        success: function(response) {
            // 切换按钮状态
            if (otype == '0') {
                btn.value = '1';
                btn.innerHTML = '取关';
            } else {
                btn.value = '0';
                btn.innerHTML = '关注';
            }
        }
    });
}
```

数据表操作示例：

初始状态（未关注）：
cn_stock_attention表：无记录

用户点击"关注"：
INSERT INTO `cn_stock_attention`(`datetime`, `code`) 
VALUE('2024-01-01 10:30:00.000000', '000001')

关注后状态：
+---------------------+--------+
| datetime            | code   |
+---------------------+--------+
| 2024-01-01 10:30:00 | 000001 |
+---------------------+--------+

用户点击"取关"：
DELETE FROM `cn_stock_attention` WHERE `code` = '000001'

取关后状态：
cn_stock_attention表：无记录

注意事项：
1. 同一股票可能被多次关注（没有唯一约束）
2. DELETE会删除所有该股票的记录
3. 时间精度到微秒（%f格式）
4. 参数化查询必须正确使用
5. 异常处理应该更完善

性能优化：
1. cn_stock_attention表应该添加索引：
   CREATE INDEX idx_code ON cn_stock_attention(code);
2. 定期清理过期记录（如果需要）
3. 考虑使用Redis缓存关注列表

安全考虑：
1. ✅ 已使用参数化查询（防SQL注入）
2. ⚠️ 应该验证code格式（6位数字）
3. ⚠️ 应该限制请求频率（防刷）
4. ⚠️ 应该记录操作日志（审计）
5. ⚠️ 应该验证用户权限（如果有多用户）

改进建议：

1. 添加唯一约束：
```sql
ALTER TABLE cn_stock_attention 
ADD UNIQUE INDEX idx_unique_code (code);
```

2. 完善异常处理：
```python
except Exception as e:
    err = {"error": str(e)}
    logging.error(err)
    self.write(json.dumps(err))
    return
```

3. 添加输入验证：
```python
if not code or len(code) != 6 or not code.isdigit():
    self.write(json.dumps({"error": "Invalid code"}))
    return
```

4. 返回更有意义的结果：
```python
self.write(json.dumps({
    "success": True,
    "action": "add" if otype == '0' else "remove",
    "code": code
}))
```

常见问题：

Q: 为什么异常处理被注释掉了？
A: 可能是为了避免前端收到错误信息，保持用户体验

Q: 可以关注多只股票吗？
A: 可以，每只股票一条记录

Q: 如何查询我关注的股票？
A: SELECT * FROM cn_stock_attention ORDER BY datetime DESC

Q: 关注数据会同步到其他设备吗？
A: 不会，这是服务器端数据，与用户无关

Q: 如何批量取消关注？
A: 需要额外的API支持，目前只能逐个取消
"""
class SaveCollectHandler(webBase.BaseHandler, ABC):
    """
    处理GET请求，管理股票关注列表
    
    URL示例：
    添加关注：/save_collect?code=000001&otype=0
    取消关注：/save_collect?code=000001&otype=1
    
    步骤说明：
    1. 获取参数：从URL中提取code和otype
    2. 获取表名：从tablestructure中获取
    3. 执行操作：根据otype执行INSERT或DELETE
    4. 返回结果：返回JSON格式
    """
    @gen.coroutine
    def get(self):
        """
        Tornado异步GET请求处理方法
        
        处理股票关注/取关请求
        
        参数：
        - code: 股票代码
        - otype: 操作类型（0=关注，1=取关）
        
        返回：
        JSON: {"data":[{}]}
        """
        
        # ==================== 步骤1: 导入必需的模块 ====================
        # 在函数内部导入，避免循环依赖
        # datetime：获取当前时间
        # tablestructure：获取表结构定义
        import datetime
        import instock.core.tablestructure as tbs
        
        # ==================== 步骤2: 获取URL参数 ====================
        # 获取股票代码
        code = self.get_argument("code", default=None, strip=False)
        
        # 获取操作类型
        # otype='0'：添加关注
        # otype='1'：取消关注
        otype = self.get_argument("otype", default=None, strip=False)
        
        try:
            # ==================== 步骤3: 获取表名 ====================
            # 从tablestructure中获取关注表的配置
            # TABLE_CN_STOCK_ATTENTION是一个字典，包含表名等信息
            # ['name']：提取表名字段
            table_name = tbs.TABLE_CN_STOCK_ATTENTION['name']
            
            # ==================== 步骤4: 根据操作类型执行不同SQL ====================
            if otype == '1':
                # === 取消关注（DELETE）===
                
                # 构造DELETE SQL语句
                # 使用参数化查询（%s占位符）
                # 注意：表名不能用参数化，因为是标识符不是值
                # sql = f"DELETE FROM `{table_name}` WHERE `code` = '{code}'"  # ❌ SQL注入风险
                sql = f"DELETE FROM `{table_name}` WHERE `code` = %s"  # ✅ 参数化查询
                
                # 执行SQL查询
                # 第二个参数code会替换%s占位符
                # 数据库会自动转义，防止SQL注入
                self.db.query(sql, code)
                
            else:
                # === 添加关注（INSERT）===
                
                # 构造INSERT SQL语句
                # 插入两个字段：datetime（当前时间）和code（股票代码）
                # 使用参数化查询（两个%s占位符）
                # sql = f"INSERT INTO `{table_name}`(`datetime`, `code`) VALUE('{datetime.datetime.now()}','{code}')"  # ❌
                sql = f"INSERT INTO `{table_name}`(`datetime`, `code`) VALUE(%s, %s)"  # ✅
                
                # 执行SQL查询
                # 第一个参数：当前时间（格式化为字符串）
                # - datetime.datetime.now()：获取当前时间
                # - strftime("%Y-%m-%d %H:%M:%S.%f")：格式化为字符串
                #   %Y：四位年份
                #   %m：两位月份
                #   %d：两位日期
                #   %H：小时（24小时制）
                #   %M：分钟
                #   %S：秒
                #   %f：微秒（6位）
                # 第二个参数：股票代码
                self.db.query(
                    sql,
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                    code
                )
        
        except Exception as e:
            # ==================== 步骤5: 异常处理 ====================
            # 捕获数据库操作异常
            # 可能的异常：
            # - 连接失败
            # - SQL语法错误
            # - 表不存在
            # - 字段类型不匹配
            # - 唯一约束冲突
            
            # 创建错误字典
            err = {"error": str(e)}
            
            # 以下代码被注释掉了，可能是为了简化前端处理
            # logging.info(err)  # 记录错误到日志
            # self.write(err)    # 返回错误信息给前端
            # return             # 提前返回
            
            # 当前行为：
            # - 不记录日志
            # - 不返回错误
            # - 继续执行后面的write
            # 这样前端始终收到成功响应，用户体验更好
            pass
        
        # ==================== 步骤6: 返回JSON结果 ====================
        # 无论成功还是失败，都返回相同的JSON
        # {"data":[{}]}：空的成功响应
        # 
        # 为什么返回空对象？
        # - 前端只需要知道请求完成
        # - 不需要额外数据
        # - 简化前端逻辑
        #
        # 更好的做法：
        # - 返回操作结果（success/fail）
        # - 返回操作类型（add/remove）
        # - 返回错误信息（如果有）
        self.write("{\"data\":[{}]}")
