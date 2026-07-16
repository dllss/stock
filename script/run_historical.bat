@echo off
REM ========================================
REM 历史数据抓取任务启动脚本
REM ========================================

echo.
echo ========================================
echo 历史数据抓取任务
echo ========================================
echo.

REM 检查参数
if "%1"=="" (
    echo 用法: run_historical.bat ^<日期^> [结束日期] [--test]
    echo.
    echo 示例:
    echo   run_historical.bat 2026-05-05              REM 单日
    echo   run_historical.bat 2026-05-05 --test       REM 测试模式
    echo   run_historical.bat 2023-05-05 2026-05-05  REM 日期区间
    echo.
    pause
    exit /b 1
)

REM 切换到项目根目录
cd /d %~dp0

echo 📅 开始执行历史数据抓取...
echo 📂 工作目录: %CD%
echo.

REM 执行Python脚本
python -m instock.job.historical_data_job %*

echo.
echo ========================================
echo 任务执行完成
echo ========================================
echo.
pause
