# InStock Docker 自动部署脚本 (PowerShell版本)
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "InStock Docker 自动部署脚本" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Set-Location $PSScriptRoot

# 步骤1: 检查 Docker 状态
Write-Host "[1/5] 检查 Docker 状态..." -ForegroundColor Yellow
try {
    docker info | Out-Null
    Write-Host "[成功] Docker 运行正常" -ForegroundColor Green
} catch {
    Write-Host "[错误] Docker 未启动，请先启动 Docker Desktop" -ForegroundColor Red
    Read-Host "按回车键退出"
    exit 1
}
Write-Host ""

# 步骤2: 拉取基础镜像
Write-Host "[2/5] 拉取基础镜像（这可能需要几分钟）..." -ForegroundColor Yellow
$mirrors = @(
    "docker.m.daocloud.io",
    "huecker.io",
    "dockerhub.timeweb.cloud",
    "noohub.ru"
)

$imagePulled = $false
foreach ($mirror in $mirrors) {
    Write-Host "  尝试从 $mirror 下载..." -ForegroundColor Gray
    try {
        docker pull "$mirror/library/python:3.11-slim-bullseye" 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0) {
            docker tag "$mirror/library/python:3.11-slim-bullseye" "python:3.11-slim-bullseye"
            $imagePulled = $true
            Write-Host "  [成功] 镜像下载完成" -ForegroundColor Green
            break
        }
    } catch {
        Write-Host "  [失败] 从此镜像源下载失败" -ForegroundColor Red
    }
}

if (-not $imagePulled) {
    Write-Host "[警告] 所有镜像源都失败了，将尝试直接构建（可能很慢）" -ForegroundColor Yellow
}
Write-Host ""

# 步骤3: 构建 Docker 镜像
Write-Host "[3/5] 构建 Docker 镜像（首次构建需要 10-20 分钟）..." -ForegroundColor Yellow
Write-Host "  这可能需要较长时间，请耐心等待..." -ForegroundColor Gray
Write-Host "  可以查看 docker-build.log 文件了解详细进度" -ForegroundColor Gray
Write-Host ""

$logFile = Join-Path $PSScriptRoot "docker-build.log"
docker compose build 2>&1 | Tee-Object -FilePath $logFile

if ($LASTEXITCODE -ne 0) {
    Write-Host "[错误] 构建失败，请查看 $logFile 了解详情" -ForegroundColor Red
    Get-Content $logFile | Select-Object -Last 50
    Read-Host "按回车键退出"
    exit 1
}
Write-Host "[成功] 镜像构建完成" -ForegroundColor Green
Write-Host ""

# 步骤4: 创建必要的目录
Write-Host "[4/5] 创建数据目录..." -ForegroundColor Yellow
$dirs = @(
    "D:\docker\mariadb\data",
    "D:\docker\instock\logs",
    "D:\docker\instock"
)

foreach ($dir in $dirs) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        Write-Host "  已创建: $dir" -ForegroundColor Gray
    }
}

# 创建代理配置文件
$proxyFile = "D:\docker\instock\proxy.txt"
if (-not (Test-Path $proxyFile)) {
    New-Item -ItemType File -Force -Path $proxyFile | Out-Null
    Write-Host "  已创建: $proxyFile" -ForegroundColor Gray
}
Write-Host "[成功] 目录创建完成" -ForegroundColor Green
Write-Host ""

# 步骤5: 启动服务
Write-Host "[5/5] 启动服务..." -ForegroundColor Yellow
docker compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "[错误] 启动失败" -ForegroundColor Red
    Read-Host "按回车键退出"
    exit 1
}
Write-Host "[成功] 服务已启动" -ForegroundColor Green
Write-Host ""

# 等待服务启动
Write-Host "等待服务初始化..." -ForegroundColor Gray
Start-Sleep -Seconds 10

# 显示服务状态
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "部署完成！" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "访问地址：" -ForegroundColor Yellow
Write-Host "  Web 界面: http://localhost:9988" -ForegroundColor White
Write-Host "  Supervisor: http://localhost:9001" -ForegroundColor White
Write-Host ""
Write-Host "查看日志：" -ForegroundColor Yellow
Write-Host "  docker compose logs -f" -ForegroundColor Gray
Write-Host ""
Write-Host "停止服务：" -ForegroundColor Yellow
Write-Host "  docker compose down" -ForegroundColor Gray
Write-Host ""
Write-Host "容器状态：" -ForegroundColor Yellow
docker compose ps
Write-Host ""

Read-Host "按回车键退出"
