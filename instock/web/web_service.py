#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
Web服务主程序（第十层核心）
===========================
这是系统的Web服务入口，提供可视化界面。

什么是Web服务？
- 基于Tornado框架的Web应用
- 提供HTTP服务
- 浏览器访问：http://localhost:9988/
- 展示所有数据和功能

Tornado框架：
- 异步Web框架
- 高性能、可扩展
- 适合实时数据展示
- Python编写

系统架构：
┌─────────────────────────────────────┐
│  浏览器（用户界面）                    │
│  - Chrome/Firefox/Safari/Edge        │
└──────────────┬──────────────────────┘
               │ HTTP请求/响应
               ↓
┌─────────────────────────────────────┐
│  Web服务（本文件）                    │
│  - Tornado HTTP Server               │
│  - 路由处理                           │
│  - 页面渲染                           │
└──────────────┬──────────────────────┘
               │ SQL查询
               ↓
┌─────────────────────────────────────┐
│  MySQL数据库                         │
│  - 存储所有数据                       │
│  - 提供查询服务                       │
└─────────────────────────────────────┘

主要功能：
1. 首页展示：导航菜单、系统信息
2. 数据表格：展示各种数据
3. 指标图表：K线图、技术指标
4. 股票关注：添加/删除关注
5. 实时刷新：开盘期间实时更新

路由映射（URL → Handler）：
- /                    → HomeHandler（首页）
- /instock/            → HomeHandler（首页）
- /instock/data        → GetStockHtmlHandler（数据页面）
- /instock/api_data    → GetStockDataHandler（数据接口）
- /instock/data/indicators → GetDataIndicatorsHandler（指标数据）
- /instock/control/attention → SaveCollectHandler（关注功能）

Web页面展示：
- 每日股票数据
- ETF数据
- 资金流向
- 龙虎榜
- 技术指标
- K线形态
- 策略选股
- 回测结果
- ...

技术栈：
- 后端：Python + Tornado
- 前端：HTML + JavaScript + jQuery
- 数据库：MySQL
- 图表：SpreadJS（商业控件）
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import os.path  # 路径操作
import sys  # 系统操作
from abc import ABC  # 抽象基类

# Tornado Web框架
import tornado.escape  # 编码处理
import tornado.httpserver  # HTTP服务器
import tornado.ioloop  # 事件循环
import tornado.options  # 选项配置
from tornado import gen  # 协程支持

# ==================== 路径配置 ====================
# 在项目运行时，临时将项目路径添加到环境变量
cpath_current = os.path.dirname(os.path.dirname(__file__))  # web目录的上级
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))  # 项目根目录
sys.path.append(cpath)

# 配置日志目录
log_path = os.path.join(cpath_current, 'log')
if not os.path.exists(log_path):
    os.makedirs(log_path)

# 配置日志系统
# filename：日志文件 stock_web.log（专门用于Web服务）
logging.basicConfig(
    format='%(asctime)s %(message)s', 
    filename=os.path.join(log_path, 'stock_web.log')
)
logging.getLogger().setLevel(logging.ERROR)  # 只记录ERROR级别

# ==================== 导入项目模块 ====================
import instock.lib.torndb as torndb  # Tornado数据库封装
import instock.lib.database as mdb  # 数据库配置
import instock.lib.version as version  # 版本信息
import instock.web.dataTableHandler as dataTableHandler  # 数据表格处理
import instock.web.dataIndicatorsHandler as dataIndicatorsHandler  # 指标数据处理
import instock.web.base as webBase  # Web基础类

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== Tornado应用类 ====================
class Application(tornado.web.Application):
    """
    Tornado Web应用主类
    
    功能说明：
        1. 配置所有路由（URL映射）
        2. 配置模板和静态文件路径
        3. 初始化数据库连接
        
    继承：
        tornado.web.Application：Tornado的应用基类
        
    作用：
        - 管理所有请求处理器（Handler）
        - 配置应用参数
        - 管理全局资源（如数据库连接）
    """
    
    def __init__(self):
        """
        初始化Web应用
        
        配置内容：
            1. handlers：路由配置
            2. settings：应用设置
            3. db：数据库连接
        """
        # ==================== 路由配置 ====================
        # handlers：URL路由列表
        # 格式：(正则表达式, 处理器类)
        handlers = [
            # 首页路由
            (r"/", HomeHandler),  # 根路径
            (r"/instock/", HomeHandler),  # instock路径
            
            # 数据表格模块
            (r"/instock/api_data", dataTableHandler.GetStockDataHandler),  # API接口：返回JSON数据
            (r"/instock/data", dataTableHandler.GetStockHtmlHandler),  # HTML页面：返回数据表格页面
            
            # 指标图表模块
            (r"/instock/data/indicators", dataIndicatorsHandler.GetDataIndicatorsHandler),  # 指标数据接口
            
            # 关注功能模块
            (r"/instock/control/attention", dataIndicatorsHandler.SaveCollectHandler),  # 添加/删除关注
        ]
        
        # ==================== 应用设置 ====================
        # settings：应用配置字典
        settings = dict(
            # 模板路径：HTML模板文件位置
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            
            # 静态文件路径：CSS、JS、图片等
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            
            # XSRF保护：跨站请求伪造保护
            xsrf_cookies=False,  # 关闭（简化开发）
            # 生产环境建议设为True
            
            # Cookie加密密钥
            # 用于加密session数据
            cookie_secret="027bb1b670eddf0392cdda8709268a17b58b7",
            
            # 调试模式
            debug=True,  # 开发时开启，自动重载代码
            # 生产环境应设为False
        )
        
        # ==================== 调用父类初始化 ====================
        # super()：调用父类方法
        # 传入handlers和settings
        super(Application, self).__init__(handlers, **settings)
        
        # ==================== 初始化数据库连接 ====================
        # Have one global connection to the blog DB across all handlers
        # self.db：全局数据库连接对象
        # 所有Handler都可以通过self.application.db访问
        # torndb.Connection：Tornado的MySQL封装
        # **mdb.MYSQL_CONN_TORNDB：解包配置字典
        self.db = torndb.Connection(**mdb.MYSQL_CONN_TORNDB)


# ==================== 首页处理器 ====================
class HomeHandler(webBase.BaseHandler, ABC):
    """
    首页请求处理器
    
    功能说明：
        处理首页（/）的GET请求
        渲染index.html模板
        
    继承：
        - webBase.BaseHandler：基础Handler，提供通用功能
        - ABC：抽象基类（标记）
        
    方法：
        get()：处理GET请求
        
    什么是Handler？
        - 请求处理器
        - 每个URL对应一个Handler
        - 处理HTTP请求，返回响应
    """
    
    @gen.coroutine
    def get(self):
        """
        处理首页GET请求
        
        装饰器：
            @gen.coroutine：协程装饰器
            - 支持异步处理
            - 提高性能
            
        功能说明：
            渲染首页模板，传入数据
            
        传入模板的数据：
            - stockVersion：系统版本号
            - leftMenu：左侧菜单HTML
            
        执行流程：
            1. 获取系统版本
            2. 生成左侧菜单
            3. 渲染index.html模板
            4. 返回HTML给浏览器
            
        self.render()：
            Tornado的模板渲染函数
            - 参数1：模板文件名
            - 其他参数：传给模板的变量
        """
        # 渲染首页模板
        # index.html：首页模板文件（在templates目录）
        # stockVersion：系统版本号（显示在页面上）
        # leftMenu：左侧导航菜单的HTML代码
        self.render("index.html",
                    stockVersion=version.__version__,  # 版本号
                    leftMenu=webBase.GetLeftMenu(self.request.uri))  # 左侧菜单


# ==================== 主函数 ====================

"""
Web服务启动函数
功能说明：
1. 创建Tornado应用
2. 创建HTTP服务器
3. 监听端口
4. 启动事件循环
执行流程：
1. 配置日志
2. 创建Application实例
3. 创建HTTPServer
4. 监听9988端口
5. 启动IOLoop（事件循环）
端口说明：
9988：系统默认端口
- 可以修改为其他端口
- 确保端口未被占用
- 防火墙允许访问
启动后：
- 浏览器访问：http://localhost:9988/
- 看到系统首页
- 可以使用所有功能
停止服务：
- Ctrl+C：优雅停止
- 关闭终端：强制停止
"""
def main():
    # ==================== 步骤1: 配置Tornado ====================
    # tornado.options.parse_command_line()  # 解析命令行参数（注释掉）
    tornado.options.options.logging = None  # 禁用Tornado自带日志

    # ==================== 步骤2: 创建HTTP服务器 ====================
    # Application()：创建Web应用实例
    # HTTPServer()：创建HTTP服务器，传入应用实例
    http_server = tornado.httpserver.HTTPServer(Application())
    
    # ==================== 步骤3: 监听端口 ====================
    port = 9988  # 端口号
    # listen()：开始监听指定端口
    # 接收来自该端口的HTTP请求
    http_server.listen(port)

    # ==================== 步骤4: 输出启动信息 ====================
    # 在终端显示
    print(f"服务已启动，web地址 : http://localhost:{port}/")
    # 在日志文件记录
    logging.error(f"服务已启动，web地址 : http://localhost:{port}/")
    # 注意：这里用的是logging.error，但内容不是错误
    # 这样做是为了确保一定记录（日志级别设为ERROR）

    # ==================== 步骤5: 启动事件循环 ====================
    # IOLoop：Tornado的事件循环
    # current()：获取当前IOLoop实例
    # start()：启动事件循环，开始处理请求
    # 注意：start()会阻塞，程序停在这里等待请求
    # 按Ctrl+C可以停止
    tornado.ioloop.IOLoop.current().start()


# ==================== 程序入口 ====================
if __name__ == "__main__":
    """
    直接运行此脚本时的入口
    
    运行方式：
        python instock/web/web_service.py
        或
        python run_web.bat（Windows）
        bash run_web.sh（Linux/Mac）
        
    启动后：
        1. 终端显示："服务已启动..."
        2. 浏览器访问：http://localhost:9988/
        3. 看到系统首页
        
    使用功能：
        - 查看每日股票数据
        - 查看技术指标
        - 查看K线形态
        - 查看策略选股
        - 查看回测结果
        - 添加股票关注
        - ...
        
    停止服务：
        - 按Ctrl+C
        - 或关闭终端窗口
    """
    main()


"""
===========================================
Web服务模块使用总结（给Python新手）
===========================================

1. 模块定位
   - 第十层：Web展示层
   - 系统的用户界面
   - 基于Tornado框架

2. Tornado框架
   什么是Tornado？
   - Python Web框架
   - 异步非阻塞
   - 高性能
   - 适合实时应用
   
   核心概念：
   - Application：应用
   - Handler：请求处理器
   - Template：模板
   - IOLoop：事件循环

3. MVC模式
   Model（模型）：
   - 数据库表
   - 数据逻辑
   
   View（视图）：
   - HTML模板
   - JavaScript
   - CSS样式
   
   Controller（控制器）：
   - Handler类
   - 处理请求
   - 返回响应

4. 路由系统
   定义：
   - URL → Handler映射
   - 正则表达式匹配
   
   示例：
   - (r"/", HomeHandler)
   - 访问/时，调用HomeHandler
   
   RESTful：
   - GET：获取数据
   - POST：提交数据
   - PUT：更新数据
   - DELETE：删除数据

5. 模板系统
   模板文件：
   - templates/index.html：首页
   - templates/stock_web.html：数据页面
   - templates/layout/：布局模板
   - templates/common/：公共组件
   
   模板语法：
   - {{ variable }}：输出变量
   - {% if %}：条件判断
   - {% for %}：循环
   
   渲染：
   - self.render("模板", 变量=值)
   - 模板+数据 → HTML
   - 返回给浏览器

6. 数据库连接
   self.db：
   - 全局数据库连接
   - 所有Handler共享
   - torndb封装
   
   查询方法：
   - self.db.query()：查询多行
   - self.db.get()：查询一行
   - self.db.execute()：执行SQL

7. 静态文件
   CSS：
   - bootstrap.min.css：UI框架
   - ace.min.css：主题样式
   
   JavaScript：
   - jquery.min.js：基础库
   - spread.sheets.all.min.js：表格控件
   - echarts.min.js：图表库
   
   字体：
   - fontawesome：图标字体

8. 页面功能
   首页：
   - 左侧菜单：所有功能导航
   - 顶部：系统信息、日期选择
   - 中间：数据展示区域
   
   数据页面：
   - 表格：显示股票数据
   - 排序：点击表头排序
   - 筛选：条件筛选
   - 导出：导出Excel
   
   指标页面：
   - K线图：股票走势
   - 技术指标：MACD、KDJ等
   - 买卖信号：标注
   - 筹码分布：成本分布图

9. 实时更新
   开盘期间：
   - 前端定时请求数据
   - 后端查询最新数据
   - 页面自动刷新
   
   实现方式：
   - JavaScript setInterval
   - Ajax请求
   - 无需刷新页面

10. 性能优化
    连接池：
    - 复用数据库连接
    - 避免频繁创建
    
    缓存：
    - 静态文件缓存
    - 模板缓存
    
    异步：
    - 异步I/O
    - 不阻塞

11. 使用说明
    启动服务：
    ```bash
    python web_service.py
    ```
    
    访问系统：
    - 浏览器打开：http://localhost:9988/
    - 建议浏览器：Chrome、Firefox
    
    停止服务：
    - 终端按Ctrl+C
    - 优雅停止

12. 常见问题
    Q: 端口被占用？
    A: 修改port变量，改为其他端口
    
    Q: 无法访问？
    A: 检查防火墙，检查端口监听
    
    Q: 页面显示不正常？
    A: 清除浏览器缓存，检查静态文件
    
    Q: 数据不更新？
    A: 检查数据库，检查数据任务

13. 扩展建议
    - 添加用户认证：登录系统
    - 添加权限控制：不同用户不同权限
    - 添加WebSocket：实时推送数据
    - 添加API接口：供第三方调用
    - 添加移动端：响应式设计

14. 安全建议
    生产环境：
    - debug=False：关闭调试
    - xsrf_cookies=True：开启XSRF保护
    - 使用HTTPS：加密传输
    - 设置强密码：cookie_secret
    - 限制访问IP：防火墙规则

15. Python知识点
    - 类继承：class A(B)
    - 装饰器：@decorator
    - 协程：@gen.coroutine
    - 字典解包：**dict
    - 路径操作：os.path
"""
