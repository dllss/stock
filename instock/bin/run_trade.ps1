# PowerShell Wrapper for run_trade.bat
# 使用方法: .\instock\bin\run_trade.ps1

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$batFile = Join-Path $PSScriptRoot "run_trade.bat"
& cmd.exe /c ""$batFile""
