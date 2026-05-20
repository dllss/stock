# 后台监控脚本 - 等待基础镜像下载完成并自动开始构建

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "InStock Docker 后台监控" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "此脚本将监控基础镜像下载进度" -ForegroundColor Yellow
Write-Host "下载完成后会自动开始构建" -ForegroundColor Yellow
Write-Host "可以安全关闭此窗口，明天继续" -ForegroundColor Gray
Write-Host ""

$targetImage = "python:3.11-slim-bullseye"
$maxWaitTime = 3600  # 最多等待 1 小时
$startTime = Get-Date

while ($true) {
    # 检查是否已超时
    $elapsed = (Get-Date) - $startTime
    if ($elapsed.TotalSeconds -gt $maxWaitTime) {
        Write-Host "[超时] 等待时间超过 1 小时，请手动检查" -ForegroundColor Red
        break
    }
    
    # 检查镜像是否存在
    $imageExists = docker images --format "{{.Repository}}:{{.Tag}}" | Select-String $targetImage
    
    if ($imageExists) {
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Green
        Write-Host "✓ 基础镜像下载完成！" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
        Write-Host ""
        Write-Host "开始构建 Docker 镜像..." -ForegroundColor Yellow
        Write-Host "这可能需要 10-20 分钟，请耐心等待" -ForegroundColor Gray
        Write-Host ""
        
        # 开始构建
        Set-Location $PSScriptRoot
        docker compose build 2>&1 | Tee-Object -FilePath "docker-build.log"
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host ""
            Write-Host "========================================" -ForegroundColor Green
            Write-Host "✓ 构建成功！" -ForegroundColor Green
            Write-Host "========================================" -ForegroundColor Green
            Write-Host ""
            Write-Host "正在启动服务..." -ForegroundColor Yellow
            
            # 创建必要的目录
            New-Item -ItemType Directory -Force -Path "D:\docker\mariadb\data" | Out-Null
            New-Item -ItemType Directory -Force -Path "D:\docker\instock\logs" | Out-Null
            if (-not (Test-Path "D:\docker\instock\proxy.txt")) {
                New-Item -ItemType File -Force -Path "D:\docker\instock\proxy.txt" | Out-Null
            }
            
            # 启动服务
            docker compose up -d
            
            Write-Host ""
            Write-Host "========================================" -ForegroundColor Green
            Write-Host "🎉 部署完成！系统已在运行！" -ForegroundColor Green
            Write-Host "========================================" -ForegroundColor Green
            Write-Host ""
            Write-Host "访问地址：" -ForegroundColor Yellow
            Write-Host "  Web 界面: http://localhost:9988" -ForegroundColor White
            Write-Host "  Supervisor: http://localhost:9001" -ForegroundColor White
            Write-Host ""
            
            # 显示容器状态
            Write-Host "容器状态：" -ForegroundColor Yellow
            docker compose ps
            
            break
        } else {
            Write-Host ""
            Write-Host "========================================" -ForegroundColor Red
            Write-Host "✗ 构建失败" -ForegroundColor Red
            Write-Host "========================================" -ForegroundColor Red
            Write-Host ""
            Write-Host "请查看 docker-build.log 了解详情" -ForegroundColor Yellow
            Write-Host "或明天手动运行: .\auto-deploy.ps1" -ForegroundColor Gray
            break
        }
    } else {
        # 显示进度
        $progress = docker pull docker.m.daocloud.io/library/python:3.11-slim-bullseye 2>&1 | Out-String
        
        # 每 30 秒检查一次
        Start-Sleep -Seconds 30
        Write-Host "$(Get-Date -Format 'HH:mm:ss') - 仍在下载中..." -ForegroundColor Gray
    }
}

Write-Host ""
Read-Host "Press Enter to exit"
