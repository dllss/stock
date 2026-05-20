# PowerShell Wrapper for run_web.bat
# 使用方法: .\instock\bin\run_web.ps1

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$batFile = Join-Path $PSScriptRoot "run_web.bat"
& cmd.exe /c ""$batFile""
