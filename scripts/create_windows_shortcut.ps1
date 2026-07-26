param(
    [int]$Port = 2424,
    [switch]$AlsoRepoShortcut
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$ProjectRoot = Split-Path -Parent $RepoRoot
$LauncherPath = Join-Path $RepoRoot "scripts\start_lo2cin4bt.ps1"
$SourceLogoPath = Join-Path $ProjectRoot "assets\readme\logos\lo2cin4.jpg"
$IconPath = Join-Path $RepoRoot "assets\desktop\lo2cin4-logo.ico"
$DesktopPath = [Environment]::GetFolderPath("Desktop")

function Update-Lo2cin4btIcon {
    if (-not (Test-Path $SourceLogoPath)) {
        throw "Missing source logo: $SourceLogoPath"
    }

    $needsRefresh = -not (Test-Path $IconPath)
    if (-not $needsRefresh) {
        $sourceItem = Get-Item -LiteralPath $SourceLogoPath
        $iconItem = Get-Item -LiteralPath $IconPath
        $needsRefresh = $sourceItem.LastWriteTimeUtc -gt $iconItem.LastWriteTimeUtc
    }
    if (-not $needsRefresh) {
        return
    }

    $pythonPath = Join-Path $RepoRoot ".venv\Scripts\python.exe"
    if (-not (Test-Path $pythonPath)) {
        $pythonPath = "python"
    }

    $env:LO2CIN4BT_SOURCE_LOGO = $SourceLogoPath
    $env:LO2CIN4BT_ICON_PATH = $IconPath
    $script = @'
import os
from pathlib import Path
from PIL import Image

source = Path(os.environ["LO2CIN4BT_SOURCE_LOGO"])
target = Path(os.environ["LO2CIN4BT_ICON_PATH"])
target.parent.mkdir(parents=True, exist_ok=True)

image = Image.open(source).convert("RGBA")
width, height = image.size
side = min(width, height)
left = (width - side) // 2
top = (height - side) // 2
image = image.crop((left, top, left + side, top + side))
image.save(
    target,
    format="ICO",
    sizes=[(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)],
)
'@
    $script | & $pythonPath -
    if ($LASTEXITCODE -ne 0) {
        throw "Could not generate shortcut icon from $SourceLogoPath"
    }
}

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

Update-Lo2cin4btIcon

$desktopShortcut = Join-Path $DesktopPath "lo2cin4bt.lnk"
New-Lo2cin4btShortcut -ShortcutPath $desktopShortcut
Write-Host "Created shortcut: $desktopShortcut"

if ($AlsoRepoShortcut) {
    $repoShortcut = Join-Path $RepoRoot "trading - Shortcut.lnk"
    New-Lo2cin4btShortcut -ShortcutPath $repoShortcut
    Write-Host "Created shortcut: $repoShortcut"
}
