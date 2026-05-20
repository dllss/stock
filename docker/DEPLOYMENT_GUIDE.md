# InStock Docker 部署指南

## 📋 当前状态

✅ **已完成：**
- Docker Desktop 已启动
- docker-compose.yml 已配置为本地构建模式
- Dockerfile 已优化（合并 pip install 命令）
- 自动化部署脚本已创建

⏳ **进行中：**
- 基础镜像 python:3.11-slim-bullseye 正在下载（速度较慢）

## 🚀 快速开始（明天继续）

### 方法一：使用自动化脚本（推荐）

```powershell
# 1. 打开 PowerShell
cd D:\WorkProject\stock\docker

# 2. 运行自动部署脚本
.\auto-deploy.ps1
```

脚本会自动完成：
- ✅ 检查 Docker 状态
- ✅ 从多个镜像源下载基础镜像
- ✅ 构建 Docker 镜像（10-20分钟）
- ✅ 创建必要的数据目录
- ✅ 启动所有服务

### 方法二：手动执行

```powershell
# 进入 docker 目录
cd D:\WorkProject\stock\docker

# 如果基础镜像还没下载完，先完成下载
docker pull docker.m.daocloud.io/library/python:3.11-slim-bullseye
docker tag docker.m.daocloud.io/library/python:3.11-slim-bullseye python:3.11-slim-bullseye

# 构建镜像
docker compose build

# 启动服务
docker compose up -d

# 查看日志
docker compose logs -f
```

## 📊 预计时间

| 步骤 | 预计时间 | 说明 |
|------|---------|------|
| 下载基础镜像 | 5-15分钟 | 取决于网络速度 |
| 首次构建 | 10-20分钟 | 编译 TA-Lib、安装依赖 |
| 后续构建 | 2-3分钟 | 使用缓存加速 |
| 服务启动 | 1-2分钟 | 数据库初始化 |

**总计：首次约 20-40 分钟**

## 🌐 访问地址

启动成功后访问：
- **Web 界面**: http://localhost:9988
- **Supervisor 管理**: http://localhost:9001

## 📝 常用命令

```powershell
# 查看容器状态
docker compose ps

# 查看实时日志
docker compose logs -f

# 重启服务
docker compose restart

# 停止服务
docker compose down

# 重新构建（代码修改后）
docker compose build --no-cache
docker compose up -d

# 清理无用数据
docker system prune -a
```

## ⚠️ 常见问题

### 1. Docker 未启动
**错误**: `The system cannot find the file specified`
**解决**: 启动 Docker Desktop，等待图标变绿

### 2. 网络连接失败
**错误**: `dial tcp ... connectex: A connection attempt failed`
**解决**: 
- 配置 Docker 镜像源（Docker Desktop → Settings → Docker Engine）
- 添加 registry-mirrors: ["https://docker.m.daocloud.io"]
- 重启 Docker Desktop

### 3. 构建失败
**解决**: 
- 查看日志: `type docker-build.log`
- 清理缓存: `docker system prune -a`
- 重新构建: `docker compose build --no-cache`

### 4. 端口被占用
**错误**: `port is already allocated`
**解决**: 
```powershell
# 查找占用端口的进程
netstat -ano | findstr "9988"
# 结束进程或修改 docker-compose.yml 中的端口映射
```

## 📂 文件说明

- `docker-compose.yml` - Docker Compose 配置文件
- `Dockerfile` - Docker 镜像构建文件
- `auto-deploy.ps1` - PowerShell 自动部署脚本
- `auto-deploy.bat` - 批处理自动部署脚本
- `docker-build.log` - 构建日志文件（自动生成）

## 🔧 数据持久化

以下目录会被挂载到宿主机，数据不会丢失：

- `D:\docker\mariadb\data` - 数据库文件
- `D:\docker\instock\logs` - 应用日志
- `D:\docker\instock\proxy.txt` - 代理配置

## 💡 提示

1. **首次构建较慢是正常的**，请耐心等待
2. 构建过程中可以查看 `docker-build.log` 了解进度
3. 如果中断了，重新执行命令会从断点继续（有缓存）
4. Cron 定时任务已配置：工作日 17:30 自动执行数据更新

## 📞 需要帮助？

如果遇到问题，请提供：
1. 完整的错误信息
2. `docker compose logs` 的输出
3. `docker-build.log` 的内容

---

**祝你晚安！明天醒来就能看到运行中的系统了 😊**
