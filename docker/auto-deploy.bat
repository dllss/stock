@echo off
chcp 65001 >nul
echo ========================================
echo InStock Docker 自动部署脚本
echo ========================================
echo.

cd /d "%~dp0"

echo [1/5] 检查 Docker 状态...
docker info >nul 2>&1
if errorlevel 1 (
    echo [错误] Docker 未启动，请先启动 Docker Desktop
    pause
    exit /b 1
)
echo [成功] Docker 运行正常
echo.

echo [2/5] 拉取基础镜像（这可能需要几分钟）...
docker pull docker.m.daocloud.io/library/python:3.11-slim-bullseye
if errorlevel 1 (
    echo [警告] 镜像拉取失败，尝试其他镜像源...
    docker pull huecker.io/library/python:3.11-slim-bullseye
)
echo.

echo [3/5] 标记镜像...
docker tag docker.m.daocloud.io/library/python:3.11-slim-bullseye python:3.11-slim-bullseye 2>nul
echo.

echo [4/5] 构建 Docker 镜像（首次构建需要 10-20 分钟）...
echo 这可能需要较长时间，请耐心等待...
echo 可以查看 docker-build.log 文件了解详细进度
echo.
docker compose build > docker-build.log 2>&1
if errorlevel 1 (
    echo [错误] 构建失败，请查看 docker-build.log 了解详情
    type docker-build.log
    pause
    exit /b 1
)
echo [成功] 镜像构建完成
echo.

echo [5/5] 启动服务...
docker compose up -d
if errorlevel 1 (
    echo [错误] 启动失败
    pause
    exit /b 1
)
echo [成功] 服务已启动
echo.

echo ========================================
echo 部署完成！
echo ========================================
echo.
echo 访问地址：
echo   Web 界面: http://localhost:9988
echo   Supervisor: http://localhost:9001
echo.
echo 查看日志：
echo   docker compose logs -f
echo.
echo 停止服务：
echo   docker compose down
echo.
pause
