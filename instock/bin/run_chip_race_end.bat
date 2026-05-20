@echo off
chcp 65001 >nul
echo ========================================
echo 执行尾盘抢筹数据任务 (指定日期)
echo ========================================
echo.

set /p target_date="请输入日期 (格式: YYYY-MM-DD, 例如 2026-05-08): "

python -c "from instock.job.data_tasks import chip_race_end_job; from datetime import datetime; date_obj = datetime.strptime('%target_date%', '%%Y-%%m-%%d').date(); chip_race_end_job.save_chip_race_end_data(date_obj, before=False)"

echo.
echo ========================================
echo 任务执行完成!
echo ========================================
pause
