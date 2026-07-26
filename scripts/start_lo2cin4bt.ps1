param(
    [int]$Port = 2424,
    [switch]$NoBrowser,
    [int]$WaitSeconds = 90,
    [int]$RetryCount = 3
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$SystemPython = (Get-Command python -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty Source)
$PreferredPython = [string]($env:LO2CIN4BT_PYTHON)
$LogDir = Join-Path $RepoRoot "logs"
$LauncherLog = Join-Path $LogDir "launcher.log"
$ServerOutLog = Join-Path $LogDir "launcher-server.out.log"
$ServerErrLog = Join-Path $LogDir "launcher-server.err.log"
$MainLog = Join-Path $LogDir "main.log"
$Url = "http://127.0.0.1:$Port/"
$HealthUrl = "http://127.0.0.1:$Port/api/app/health"

Set-Location $RepoRoot
$Host.UI.RawUI.WindowTitle = "lo2cin4bt"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Write-LauncherLog {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -LiteralPath $LauncherLog -Value $line -Encoding UTF8
    Write-Host $Message
}

function Test-Lo2cin4btHealth {
    try {
        $response = Invoke-RestMethod -Uri $HealthUrl -TimeoutSec 1 -ErrorAction Stop
        return $response.status -eq "ok"
    } catch {
        return $false
    }
}

function Open-Lo2cin4btBrowser {
    if (-not $NoBrowser) {
        Start-Process $Url | Out-Null
    }
}

function Get-Lo2cin4btServerProcesses {
    Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine -and
        $_.CommandLine -match "(\-m\s+app\.server|main\.py|start_app_background\.py)" -and
        $_.CommandLine -match "--port\s+$Port"
    }
}

function Resolve-Lo2cin4btPython {
    if ($PreferredPython -and (Test-Path $PreferredPython)) {
        return $PreferredPython
    }
    if ($SystemPython -and (Test-Path $SystemPython)) {
        return $SystemPython
    }
    if (Test-Path $VenvPython) {
        return $VenvPython
    }
    return $null
}

function Start-Lo2cin4btDetachedServer {
    param([string]$PythonPath)
    $arguments = @(
        "-X"
        "faulthandler"
        "-u"
        "-m"
        "app.server"
        "--port"
        "$Port"
        "--no-browser"
    )
    $process = Start-Process `
        -FilePath $PythonPath `
        -ArgumentList $arguments `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $ServerOutLog `
        -RedirectStandardError $ServerErrLog `
        -WindowStyle Hidden `
        -PassThru
    if ($null -eq $process -or $process.Id -le 0) {
        throw "Detached process launch failed"
    }
    return [int]$process.Id
}

function Stop-Lo2cin4btProcessTree {
    param([int]$RootProcessId)

    $children = Get-CimInstance Win32_Process | Where-Object {
        $_.ParentProcessId -eq $RootProcessId
    }
    foreach ($child in $children) {
        Stop-Lo2cin4btProcessTree -RootProcessId $child.ProcessId
    }
    try {
        Stop-Process -Id $RootProcessId -Force -ErrorAction Stop
    } catch {
        Write-LauncherLog "Could not stop stale lo2cin4bt process ${RootProcessId}: $($_.Exception.Message)"
    }
}

Write-LauncherLog "Starting lo2cin4bt launcher for $Url"

if (Test-Lo2cin4btHealth) {
    Write-LauncherLog "lo2cin4bt is already running; opening browser."
    Open-Lo2cin4btBrowser
    exit 0
}

$stalePythonProcesses = Get-Lo2cin4btServerProcesses
foreach ($process in $stalePythonProcesses) {
    Stop-Lo2cin4btProcessTree -RootProcessId $process.ProcessId
}

if (-not (Test-Path $VenvPython)) {
    Write-LauncherLog "Project venv python not found. Falling back to interpreter health resolution."
}

$PythonPath = Resolve-Lo2cin4btPython
if (-not $PythonPath) {
    Write-LauncherLog "No healthy Python interpreter found for lo2cin4bt app startup."
    Write-LauncherLog "Checked preferred interpreter: $PreferredPython"
    Write-LauncherLog "Checked system interpreter: $SystemPython"
    Write-LauncherLog "Checked venv interpreter: $VenvPython"
    Read-Host "Press Enter to close"
    exit 1
}
Write-LauncherLog "Using Python interpreter: $PythonPath"

for ($attempt = 1; $attempt -le [Math]::Max(1, $RetryCount); $attempt++) {
    Write-LauncherLog "Launching server process (attempt $attempt/$RetryCount)."
    $processId = Start-Lo2cin4btDetachedServer -PythonPath $PythonPath
    Write-LauncherLog "Detached server process created with PID $processId."

    $deadline = (Get-Date).AddSeconds($WaitSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-Lo2cin4btHealth) {
            Write-LauncherLog "lo2cin4bt is ready; opening browser."
            Open-Lo2cin4btBrowser
            exit 0
        }
        $matchingProcesses = @(Get-Lo2cin4btServerProcesses)
        if ($matchingProcesses.Count -eq 0) {
            Start-Sleep -Milliseconds 500
            continue
        }
        Start-Sleep -Milliseconds 500
    }
    $matchingProcesses = @(Get-Lo2cin4btServerProcesses)
    Write-LauncherLog "lo2cin4bt did not become ready within $WaitSeconds seconds on attempt $attempt."
    foreach ($matchingProcess in $matchingProcesses) {
        Stop-Lo2cin4btProcessTree -RootProcessId $matchingProcess.ProcessId
    }
    if ($attempt -lt $RetryCount) {
        Write-LauncherLog "Retrying lo2cin4bt launch after startup failure."
        Start-Sleep -Seconds 2
    }
}

Write-LauncherLog "lo2cin4bt did not become ready within $WaitSeconds seconds."
if (Test-Path $MainLog) {
    Write-Host ""
    Write-Host "Recent main.log:"
    Get-Content -LiteralPath $MainLog -Tail 40
}
if (Test-Path $LauncherLog) {
    Write-Host ""
    Write-Host "Recent launcher.log:"
    Get-Content -LiteralPath $LauncherLog -Tail 40
}
if (Test-Path $ServerErrLog) {
    Write-Host ""
    Write-Host "Recent launcher-server.err.log:"
    Get-Content -LiteralPath $ServerErrLog -Tail 40
}
Read-Host "Press Enter to close"
exit 1
