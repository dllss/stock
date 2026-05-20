@echo off
chcp 65001 >nul
echo ========================================
echo 批量执行数据任务
echo ========================================
echo.

echo [1/5] 执行尾盘抢筹...
python instock/job/data_tasks/chip_race_end_job.py
echo.

echo [2/5] 执行个股资金流向...
python instock/job/data_tasks/stock_fund_flow_job.py
echo.

echo [3/5] 执行分红配送...
python instock/job/data_tasks/bonus_job.py
echo.

echo [4/5] 执行行业资金流向...
python instock/job/data_tasks/industry_fund_flow_job.py
echo.

echo [5/5] 执行概念资金流向...
python instock/job/data_tasks/concept_fund_flow_job.py
echo.

echo ========================================
echo 所有任务执行完成!
echo ========================================
pause
