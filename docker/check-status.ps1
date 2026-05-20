# Docker 部署状态检查脚本
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "InStock Docker 部署状态检查" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Set-Location $PSScriptRoot

# 检查 Docker 状态
Write-Host "[1/4] Docker 状态:" -ForegroundColor Yellow
try {
    $dockerInfo = docker info 2>&1 | Select-String "Server Version"
    if ($dockerInfo) {
        Write-Host "  ✓ Docker 正在运行" -ForegroundColor Green
        Write-Host "  $($dockerInfo.ToString().Trim())" -ForegroundColor Gray
    } else {
        Write-Host "  ✗ Docker 未启动" -ForegroundColor Red
    }
} catch {
    Write-Host "  ✗ Docker 未启动，请先启动 Docker Desktop" -ForegroundColor Red
}
Write-Host ""

# 检查镜像
Write-Host "[2/4] Docker 镜像:" -ForegroundColor Yellow
$images = docker images | Select-String "python"
if ($images) {
    Write-Host "  ✓ Python 基础镜像已下载" -ForegroundColor Green
    $images | ForEach-Object { Write-Host "    $_" -ForegroundColor Gray }
} else {
    Write-Host "  ✗ Python 基础镜像未下载" -ForegroundColor Yellow
    Write-Host "    正在下载中或需要手动执行: docker pull docker.m.daocloud.io/library/python:3.11-slim-bullseye" -ForegroundColor Gray
}
Write-Host ""

# 检查容器
Write-Host "[3/4] 运行中的容器:" -ForegroundColor Yellow
$containers = docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
if ($containers) {
    Write-Host "  ✓ 容器正在运行" -ForegroundColor Green
    Write-Host $containers -ForegroundColor Gray
} else {
    Write-Host "  ✗ 没有运行中的容器" -ForegroundColor Yellow
    Write-Host "    需要执行: .\auto-deploy.ps1" -ForegroundColor Gray
}
Write-Host ""

# 检查构建日志
Write-Host "[4/4] 构建日志:" -ForegroundColor Yellow
$logFile = Join-Path $PSScriptRoot "docker-build.log"
if (Test-Path $logFile) {
    $logContent = Get-Content $logFile -Tail 10
    Write-Host "  最近10行日志:" -ForegroundColor Gray
    $logContent | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
    
    if (Select-String -Path $logFile -Pattern "Successfully built" -Quiet) {
        Write-Host "  ✓ 构建成功" -ForegroundColor Green
    } elseif (Select-String -Path $logFile -Pattern "ERROR|error|failed" -Quiet) {
        Write-Host "  ✗ 构建失败，请查看完整日志" -ForegroundColor Red
    } else {
        Write-Host "  ⏳ 构建进行中或尚未开始" -ForegroundColor Yellow
    }
} else {
    Write-Host "  - 尚无构建日志" -ForegroundColor Gray
    Write-Host "    首次构建后会生成此文件" -ForegroundColor Gray
}
Write-Host ""

# 访问地址
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "访问地址（如果服务已启动）:" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Web 界面: http://localhost:9988" -ForegroundColor White
Write-Host "  Supervisor: http://localhost:9001" -ForegroundColor White
Write-Host ""

# 下一步建议
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "下一步操作建议:" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

if (-not $containers) {
    Write-Host "  1. 运行自动部署脚本:" -ForegroundColor Yellow
    Write-Host "     .\auto-deploy.ps1" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  2. 或者手动执行:" -ForegroundColor Yellow
    Write-Host "     docker compose build" -ForegroundColor Gray
    Write-Host "     docker compose up -d" -ForegroundColor Gray
} else {
    Write-Host "  ✓ 系统已在运行中！" -ForegroundColor Green
    Write-Host "  查看日志: docker compose logs -f" -ForegroundColor Gray
}
Write-Host ""

Read-Host "Press Enter to exit"
