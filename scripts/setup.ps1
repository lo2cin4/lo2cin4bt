param(
    [switch]$Dev,
    [switch]$Brokers,
    [switch]$SkipFrontend
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"

Set-Location $RepoRoot

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

if (-not (Test-Path $VenvPython)) {
    python -m venv .venv
}

& $VenvPython -m pip install --upgrade pip wheel setuptools
if ($Brokers) {
    & $VenvPython -m pip install -r requirements-brokers.lock
} elseif ($Dev) {
    & $VenvPython -m pip install -r requirements-dev.lock
} else {
    & $VenvPython -m pip install -r requirements.lock
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

& $VenvPython scripts\doctor.py
