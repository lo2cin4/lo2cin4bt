param(
    [switch]$Dev,
    [switch]$Brokers,
    [switch]$SkipFrontend
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$RequiredUvVersion = "0.11.32"

Set-Location $RepoRoot

$uvCommand = Get-Command uv -ErrorAction SilentlyContinue
if (-not $uvCommand) {
    throw "uv $RequiredUvVersion is required. Install it from https://docs.astral.sh/uv/ and rerun setup."
}
$uvVersionLine = (& $uvCommand.Source --version | Select-Object -First 1)
if ($uvVersionLine -notmatch "^uv $([regex]::Escape($RequiredUvVersion))(?:\s|$)") {
    throw "uv $RequiredUvVersion is required; found '$uvVersionLine'."
}

$candidateNodeDirs = @(
    $env:LO2CIN4BT_NODE_HOME,
    $env:NODE_HOME,
    (Join-Path $RepoRoot ".tools\nodejs")
) | Where-Object { $_ -and (Test-Path (Join-Path $_ "node.exe")) }

foreach ($nodeDir in $candidateNodeDirs) {
    $npmCmd = Join-Path $nodeDir "npm.cmd"
    if (Test-Path $npmCmd) {
        $env:Path = "$nodeDir;$env:Path"
        Write-Host "Using Node from $nodeDir"
        break
    }
}

$candidateRustRoots = @(
    $env:LO2CIN4BT_RUST_HOME,
    $env:RUST_HOME,
    (Join-Path $RepoRoot ".tools\rust")
) | Where-Object { $_ }

foreach ($rustRoot in $candidateRustRoots) {
    $cargoHome = Join-Path $rustRoot "cargo"
    $rustupHome = Join-Path $rustRoot "rustup"
    $cargoBin = Join-Path $cargoHome "bin"
    $cargoCmd = Join-Path $cargoBin "cargo.exe"
    if (Test-Path $cargoCmd) {
        if (-not $env:CARGO_HOME) {
            $env:CARGO_HOME = $cargoHome
        }
        if (-not $env:RUSTUP_HOME) {
            $env:RUSTUP_HOME = $rustupHome
        }
        $env:Path = "$cargoBin;$env:Path"
        Write-Host "Using Rust from $rustRoot"
        break
    }
}

if ($Brokers) {
    Write-Host "uv sync --locked --group brokers"
    & $uvCommand.Source sync --locked --group brokers
} elseif ($Dev) {
    Write-Host "uv sync --locked --group dev"
    & $uvCommand.Source sync --locked --group dev
} else {
    Write-Host "uv sync --locked"
    & $uvCommand.Source sync --locked
}

if (-not $SkipFrontend) {
    Push-Location plotter/web
    npm ci
    npm run build
    Pop-Location
}

Push-Location rust/lo2cin4bt_core
cargo build --release --bins
Pop-Location

if ($Brokers) {
    & $uvCommand.Source run --locked --exact --group brokers python scripts\doctor.py
} elseif ($Dev) {
    & $uvCommand.Source run --locked --exact --group dev python scripts\doctor.py
} else {
    & $uvCommand.Source run --locked --exact python scripts\doctor.py
}
