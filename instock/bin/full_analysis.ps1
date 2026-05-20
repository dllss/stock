# PowerShell Wrapper for full_analysis.bat
# 使用方法: .\instock\bin\full_analysis.ps1

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$batFile = Join-Path $PSScriptRoot "full_analysis.bat"
& cmd.exe /c ""$batFile""
