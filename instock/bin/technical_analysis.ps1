# PowerShell Wrapper for technical_analysis.bat
# 使用方法: .\instock\bin\technical_analysis.ps1

# 设置UTF-8编码
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

# 获取bat文件的完整路径
$batFile = Join-Path $PSScriptRoot "technical_analysis.bat"

# 调用bat文件 (bat文件内部会处理目录切换)
& cmd.exe /c ""$batFile""
