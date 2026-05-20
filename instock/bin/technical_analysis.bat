@echo off
chcp 65001 >nul
REM ==========================================
REM 技术分析脚本 - Windows版本
REM ==========================================
REM 
REM 功能：仅运行技术分析和选股任务（不包含基础数据抓取）
REM 适用场景：已有基础数据，只需要计算指标和选股
REM 预计耗时：30-50分钟
REM
REM 前置条件：
REM   - 已运行 execute_daily_job.py（基础数据）
REM   - 已运行 historical_data_job.py（历史K线数据）
REM
REM 使用方法：
REM   1. 双击运行此脚本
REM   2. 或在命令行中执行：instock\bin\technical_analysis.bat
REM
REM ==========================================

cd /d %~dp0\..

echo.
echo ==========================================
echo    开始技术分析（不含基础数据）
echo ==========================================
echo.
echo 注意：此脚本假设基础数据和历史K线已存在
echo 如果尚未获取基础数据，请先运行 quick_update.bat
echo.
pause

REM 记录开始时间
set START_TIME=%time%
echo 开始时间: %START_TIME%
echo.

REM ==========================================
REM 步骤1：技术指标计算
REM ==========================================
echo [1/3] 计算技术指标...
echo ------------------------------------------
python instock/job/indicators_data_daily_job.py
if errorlevel 1 (
    echo.
    echo ?? 技术指标计算失败（可继续）
    echo 请检查日志文件：instock\log\stock_execute_job.log
    echo.
    choice /C YN /M "是否继续执行后续任务"
    if errorlevel 2 exit /b 1
)
echo ? 技术指标计算完成
echo.

REM ==========================================
REM 步骤2：K线形态识别
REM ==========================================
echo [2/3] 识别K线形态...
echo ------------------------------------------
python instock/job/klinepattern_data_daily_job.py
if errorlevel 1 (
    echo.
    echo ?? K线形态识别失败（可继续）
    echo 请检查日志文件：instock\log\stock_execute_job.log
    echo.
    choice /C YN /M "是否继续执行后续任务"
    if errorlevel 2 exit /b 1
)
echo ? K线形态识别完成
echo.

REM ==========================================
REM 步骤3：策略选股
REM ==========================================
echo [3/3] 策略选股...
echo ------------------------------------------
python instock/job/strategy_data_daily_job.py
if errorlevel 1 (
    echo.
    echo ?? 策略选股失败
    echo 请检查日志文件：instock\log\stock_execute_job.log
    pause
    exit /b 1
)
echo ? 策略选股完成
echo.

REM ==========================================
REM 完成
REM ==========================================
set END_TIME=%time%
echo ==========================================
echo    ? 技术分析完成！
echo ==========================================
echo.
echo 开始时间: %START_TIME%
echo 结束时间: %END_TIME%
echo.
echo 详细日志：instock\log\stock_execute_job.log
echo.
pause