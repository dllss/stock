# ========================================
# 历史数据抓取任务启动脚本 (PowerShell)
# ========================================

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "历史数据抓取任务" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 检查参数
if ($args.Count -eq 0) {
    Write-Host "用法: .\run_historical.ps1 <日期> [结束日期] [--test]" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "示例:" -ForegroundColor Green
    Write-Host "  .\run_historical.ps1 2026-05-05              # 单日" 
    Write-Host "  .\run_historical.ps1 2026-05-05 --test       # 测试模式"
    Write-Host "  .\run_historical.ps1 2023-05-05 2026-05-05  # 日期区间"
    Write-Host ""
    Read-Host "按回车键退出"
    exit 1
}

# 切换到项目根目录
Set-Location $PSScriptRoot

Write-Host "📅 开始执行历史数据抓取..." -ForegroundColor Green
Write-Host "📂 工作目录: $(Get-Location)" -ForegroundColor Green
Write-Host ""

# 执行Python脚本
python -m instock.job.historical_data_job $args

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "任务执行完成" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Read-Host "按回车键退出"
