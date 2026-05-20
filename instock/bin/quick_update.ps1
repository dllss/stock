# PowerShell Wrapper for quick_update.bat
# 使用方法: .\instock\bin\quick_update.ps1

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$batFile = Join-Path $PSScriptRoot "quick_update.bat"
& cmd.exe /c ""$batFile""
