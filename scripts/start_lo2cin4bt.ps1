param(
    [int]$Port = 2424,
    [switch]$NoBrowser,
    [int]$WaitSeconds = 90,
    [int]$RetryCount = 3
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$RequiredUvVersion = "0.11.32"
$UvRunContract = "uv run --locked --exact"
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

function Resolve-Lo2cin4btUv {
    $uvCommand = Get-Command uv -ErrorAction SilentlyContinue
    if (-not $uvCommand) {
        return $null
    }
    $uvVersionLine = (& $uvCommand.Source --version | Select-Object -First 1)
    if ($uvVersionLine -notmatch "^uv $([regex]::Escape($RequiredUvVersion))(?:\s|$)") {
        Write-LauncherLog "uv $RequiredUvVersion is required; found '$uvVersionLine'."
        return $null
    }
    return $uvCommand.Source
}

function Start-Lo2cin4btDetachedServer {
    param([string]$UvPath)
    $arguments = @(
        "run"
        "--locked"
        "--exact"
        "python"
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
        -FilePath $UvPath `
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

$UvPath = Resolve-Lo2cin4btUv
if (-not $UvPath) {
    Write-LauncherLog "uv $RequiredUvVersion is required for lo2cin4bt app startup."
    Write-LauncherLog "Install the exact version, then run scripts\setup.ps1 before launching."
    Read-Host "Press Enter to close"
    exit 1
}
Write-LauncherLog "Using locked Python route: $UvRunContract"

for ($attempt = 1; $attempt -le [Math]::Max(1, $RetryCount); $attempt++) {
    Write-LauncherLog "Launching server process (attempt $attempt/$RetryCount)."
    $processId = Start-Lo2cin4btDetachedServer -UvPath $UvPath
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
