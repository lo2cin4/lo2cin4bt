param(
    [int]$Port = 2424,
    [switch]$AlsoRepoShortcut
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$LauncherPath = Join-Path $RepoRoot "scripts\start_lo2cin4bt.ps1"
$IconPath = Join-Path $RepoRoot "assets\desktop\lo2cin4bt-logo.ico"
$DesktopPath = [Environment]::GetFolderPath("Desktop")

function New-Lo2cin4btShortcut {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ShortcutPath
    )

    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($ShortcutPath)
    $shortcut.TargetPath = "powershell.exe"
    $shortcut.Arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$LauncherPath`" -Port $Port"
    $shortcut.WorkingDirectory = $RepoRoot
    if (Test-Path $IconPath) {
        $shortcut.IconLocation = "$IconPath,0"
    }
    $shortcut.Description = "Start lo2cin4bt local backtesting app"
    $shortcut.Save()
}

if (-not (Test-Path $LauncherPath)) {
    throw "Missing launcher script: $LauncherPath"
}
if (-not (Test-Path $IconPath)) {
    throw "Missing shortcut icon: $IconPath"
}

$desktopShortcut = Join-Path $DesktopPath "lo2cin4bt.lnk"
New-Lo2cin4btShortcut -ShortcutPath $desktopShortcut
Write-Host "Created shortcut: $desktopShortcut"

if ($AlsoRepoShortcut) {
    $repoShortcut = Join-Path $RepoRoot "trading - Shortcut.lnk"
    New-Lo2cin4btShortcut -ShortcutPath $repoShortcut
    Write-Host "Created shortcut: $repoShortcut"
}
