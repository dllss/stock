# PowerShell Wrapper for run_job.bat
# 使用方法: .\instock\bin\run_job.ps1

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$batFile = Join-Path $PSScriptRoot "run_job.bat"
& cmd.exe /c ""$batFile""
