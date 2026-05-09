$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Get-Command python -ErrorAction Stop

Set-Location $scriptDir

Write-Host "Starting ST web app from $scriptDir"
Write-Host "Using Python: $($python.Source)"
Write-Host "URL: http://127.0.0.1:8000"

python app.py
