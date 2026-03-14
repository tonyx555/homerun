# Homerun - Windows Setup Script
# Run: .\scripts\infra\setup.ps1

param(
    [switch]$PostgresOnly
)

$ErrorActionPreference = "Stop"

# Navigate to project root (grandparent of scripts\infra\)
Set-Location (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)))

Write-Host "=========================================" -ForegroundColor Green
Write-Host "  Homerun Setup (Windows)" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green
Write-Host ""

function Get-InstallerLogDir {
    $logDir = Join-Path (Get-Location).Path "data\runtime\logs"
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
    return $logDir
}

function Show-LogTail {
    param(
        [string]$Path,
        [string]$Label
    )

    if (-not (Test-Path $Path)) {
        return
    }

    Write-Host "$Label (tail):" -ForegroundColor Yellow
    Get-Content -Path $Path -Tail 30 -ErrorAction SilentlyContinue | ForEach-Object {
        Write-Host "  $_" -ForegroundColor DarkYellow
    }
}

function Find-PostgresBinDir {
    if ($env:POSTGRES_BIN_DIR) {
        $envBin = $env:POSTGRES_BIN_DIR
        if (
            (Test-Path (Join-Path $envBin "initdb.exe")) -and
            (Test-Path (Join-Path $envBin "pg_ctl.exe"))
        ) {
            return $envBin
        }
    }

    $initdb = Get-Command initdb -ErrorAction SilentlyContinue
    $pgctl = Get-Command pg_ctl -ErrorAction SilentlyContinue
    if ($initdb -and $pgctl) {
        return (Split-Path -Parent $initdb.Source)
    }

    $roots = @(
        "C:\Program Files\PostgreSQL",
        "C:\Program Files (x86)\PostgreSQL"
    )
    foreach ($root in $roots) {
        if (-not (Test-Path $root)) {
            continue
        }
        $versions = Get-ChildItem -Path $root -Directory -ErrorAction SilentlyContinue | Sort-Object Name -Descending
        foreach ($versionDir in $versions) {
            $bin = Join-Path $versionDir.FullName "bin"
            if (
                (Test-Path (Join-Path $bin "initdb.exe")) -and
                (Test-Path (Join-Path $bin "pg_ctl.exe"))
            ) {
                return $bin
            }
        }
    }

    return $null
}

function Get-DockerCliPath {
    $cmd = Get-Command docker -ErrorAction SilentlyContinue
    if ($cmd) {
        return $cmd.Source
    }

    $candidates = @(
        (Join-Path $env:ProgramFiles "Docker\Docker\resources\bin\docker.exe"),
        (Join-Path ${env:ProgramFiles(x86)} "Docker\Docker\resources\bin\docker.exe"),
        (Join-Path $env:LocalAppData "Programs\Docker\Docker\resources\bin\docker.exe")
    )
    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path $candidate)) {
            return $candidate
        }
    }

    return $null
}

function Ensure-DockerCommand {
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        return $true
    }

    $dockerPath = Get-DockerCliPath
    if (-not $dockerPath) {
        return $false
    }

    try {
        Set-Alias -Name docker -Value $dockerPath -Scope Script -Force
        return $true
    } catch {
        return $false
    }
}

function Test-DockerRuntimeAvailable {
    if (-not (Ensure-DockerCommand)) {
        return $false
    }

    try {
        docker info *> $null
        return ($LASTEXITCODE -eq 0)
    } catch {
        return $false
    }
}

function Get-DockerDesktopExePath {
    $candidates = @(
        "C:\Program Files\Docker\Docker\Docker Desktop.exe",
        "C:\Program Files\Docker\Docker\Docker Desktop Installer.exe",
        (Join-Path $env:LocalAppData "Programs\Docker\Docker\Docker Desktop.exe")
    )
    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path $candidate)) {
            return $candidate
        }
    }
    return $null
}

function Wait-ForDockerRuntime {
    param([int]$TimeoutSeconds = 240)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $desktopLaunched = $false
    while ((Get-Date) -lt $deadline) {
        if (Test-DockerRuntimeAvailable) {
            return $true
        }
        try {
            Start-Service -Name "com.docker.service" -ErrorAction SilentlyContinue
        } catch {
        }
        if (-not $desktopLaunched) {
            $desktopExe = Get-DockerDesktopExePath
            if ($desktopExe) {
                try {
                    Start-Process -FilePath $desktopExe -WindowStyle Hidden | Out-Null
                    $desktopLaunched = $true
                } catch {
                }
            }
        }
        Start-Sleep -Seconds 3
    }
    return (Test-DockerRuntimeAvailable)
}

function Try-InstallDockerWithWinget {
    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if (-not $winget) {
        return $false
    }

    $logDir = Get-InstallerLogDir
    $logPath = Join-Path $logDir "docker-winget-install.log"
    Write-Host "Installing Docker Desktop via winget..." -ForegroundColor Cyan
    try {
        & winget install --id Docker.DockerDesktop --exact --silent --disable-interactivity --accept-source-agreements --accept-package-agreements 2>&1 | Tee-Object -FilePath $logPath | Out-Null
        $exitCode = $LASTEXITCODE
        if ($exitCode -ne 0 -and $exitCode -ne 3010) {
            Write-Host "Winget Docker install failed (exit code $exitCode)." -ForegroundColor Yellow
            Write-Host "Log: $logPath" -ForegroundColor Yellow
            Show-LogTail -Path $logPath -Label "winget output"
            return $false
        }
        if (Wait-ForDockerRuntime -TimeoutSeconds 300) {
            Write-Host "Docker runtime installed via winget (Docker.DockerDesktop)." -ForegroundColor Green
            return $true
        }
        Write-Host "Docker install completed but Docker runtime is still not ready." -ForegroundColor Yellow
        Write-Host "Log: $logPath" -ForegroundColor Yellow
        Show-LogTail -Path $logPath -Label "winget output"
        return $false
    } catch {
        Write-Host "Winget Docker install threw an exception: $($_.Exception.Message)" -ForegroundColor Yellow
        Write-Host "Log: $logPath" -ForegroundColor Yellow
        Show-LogTail -Path $logPath -Label "winget output"
        return $false
    }
}

function Try-InstallDockerWithChocolatey {
    $choco = Get-Command choco -ErrorAction SilentlyContinue
    if (-not $choco) {
        return $false
    }

    $logDir = Get-InstallerLogDir
    $logPath = Join-Path $logDir "docker-choco-install.log"
    Write-Host "Installing Docker Desktop via Chocolatey..." -ForegroundColor Cyan
    try {
        & choco install docker-desktop -y --no-progress 2>&1 | Tee-Object -FilePath $logPath | Out-Null
        $exitCode = $LASTEXITCODE
        if ($exitCode -ne 0) {
            Write-Host "Chocolatey Docker install failed (exit code $exitCode)." -ForegroundColor Yellow
            Write-Host "Log: $logPath" -ForegroundColor Yellow
            Show-LogTail -Path $logPath -Label "choco output"
            return $false
        }
        if (Wait-ForDockerRuntime -TimeoutSeconds 300) {
            Write-Host "Docker runtime installed via Chocolatey (docker-desktop)." -ForegroundColor Green
            return $true
        }
        Write-Host "Docker install completed but Docker runtime is still not ready." -ForegroundColor Yellow
        Write-Host "Log: $logPath" -ForegroundColor Yellow
        Show-LogTail -Path $logPath -Label "choco output"
        return $false
    } catch {
        Write-Host "Chocolatey Docker install threw an exception: $($_.Exception.Message)" -ForegroundColor Yellow
        Write-Host "Log: $logPath" -ForegroundColor Yellow
        Show-LogTail -Path $logPath -Label "choco output"
        return $false
    }
}

function Ensure-DockerRuntime {
    if (Test-DockerRuntimeAvailable) {
        return $true
    }
    if (Wait-ForDockerRuntime -TimeoutSeconds 45) {
        return $true
    }

    Write-Host "Docker runtime missing. Attempting to install Docker Desktop..." -ForegroundColor Cyan
    if (Try-InstallDockerWithWinget) {
        return $true
    }
    if (Try-InstallDockerWithChocolatey) {
        return $true
    }
    return $false
}

function Test-PostgresRuntimeAvailable {
    if (Test-DockerRuntimeAvailable) { return $true }
    return [bool](Find-PostgresBinDir)
}

function Ensure-PostgresRuntime {
    if (Test-DockerRuntimeAvailable) {
        Write-Host "Postgres runtime prerequisite found (docker)." -ForegroundColor Green
        return $true
    }

    if (Ensure-DockerRuntime) {
        Write-Host "Postgres runtime prerequisite found (docker)." -ForegroundColor Green
        return $true
    }

    Write-Host "Docker runtime unavailable. Attempting PostgreSQL tools fallback..." -ForegroundColor Yellow

    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if ($winget) {
        $wingetIds = @(
            "PostgreSQL.PostgreSQL",
            "PostgreSQL.PostgreSQL.16"
        )
        foreach ($id in $wingetIds) {
            try {
                winget install --id $id --exact --silent --accept-source-agreements --accept-package-agreements *> $null
                if (Test-PostgresRuntimeAvailable) {
                    Write-Host "Postgres runtime installed via winget ($id)." -ForegroundColor Green
                    return $true
                }
            } catch {
            }
        }
    }

    $choco = Get-Command choco -ErrorAction SilentlyContinue
    if ($choco) {
        try {
            choco install postgresql -y *> $null
            if (Test-PostgresRuntimeAvailable) {
                Write-Host "Postgres runtime installed via Chocolatey." -ForegroundColor Green
                return $true
            }
        } catch {
        }
    }

    Write-Host "Failed to auto-install Postgres runtime prerequisites." -ForegroundColor Red
    Write-Host "Install Docker Desktop (recommended) or PostgreSQL tools (initdb + pg_ctl), then rerun setup." -ForegroundColor Yellow
    return $false
}

if ($PostgresOnly) {
    if (-not (Ensure-PostgresRuntime)) {
        exit 1
    }
    exit 0
}

$pythonMinMinor = 10
$pythonMaxMinor = 12

function Test-PythonVersionSupported {
    param(
        [string]$Command,
        [string[]]$PrefixArgs = @()
    )

    if (-not (Get-Command $Command -ErrorAction SilentlyContinue)) {
        return $false
    }

    try {
        & $Command @PrefixArgs -c "import sys; raise SystemExit(0 if sys.version_info.major == 3 and $pythonMinMinor <= sys.version_info.minor <= $pythonMaxMinor else 1)" *> $null
        return ($LASTEXITCODE -eq 0)
    } catch {
        return $false
    }
}

function Get-PythonVersionString {
    param(
        [string]$Command,
        [string[]]$PrefixArgs = @()
    )

    try {
        return (& $Command @PrefixArgs -c "import platform; print(platform.python_version())")
    } catch {
        return $null
    }
}

function Get-SupportedPythonCandidate {
    $candidates = @(
        @{ command = "py"; prefix = @("-3.12") },
        @{ command = "py"; prefix = @("-3.11") },
        @{ command = "py"; prefix = @("-3.10") },
        @{ command = "python3.12"; prefix = @() },
        @{ command = "python3.11"; prefix = @() },
        @{ command = "python3.10"; prefix = @() },
        @{ command = "python"; prefix = @() }
    )

    foreach ($candidate in $candidates) {
        if (Test-PythonVersionSupported -Command $candidate.command -PrefixArgs $candidate.prefix) {
            return $candidate
        }
    }
    return $null
}

function Install-SupportedPython {
    Write-Host "No supported Python 3.10-3.12 interpreter found. Attempting automatic install..." -ForegroundColor Cyan

    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if ($winget) {
        foreach ($id in @("Python.Python.3.12", "Python.Python.3.11", "Python.Python.3.10")) {
            try {
                winget install --id $id --exact --silent --accept-source-agreements --accept-package-agreements *> $null
                if (Get-SupportedPythonCandidate) {
                    return $true
                }
            } catch {
            }
        }
    }

    $choco = Get-Command choco -ErrorAction SilentlyContinue
    if ($choco) {
        foreach ($pkg in @("python312", "python311", "python310", "python")) {
            try {
                choco install $pkg -y *> $null
                if (Get-SupportedPythonCandidate) {
                    return $true
                }
            } catch {
            }
        }
    }

    return $false
}

$pythonCandidate = Get-SupportedPythonCandidate
if (-not $pythonCandidate) {
    Install-SupportedPython | Out-Null
    $pythonCandidate = Get-SupportedPythonCandidate
}
if (-not $pythonCandidate) {
    Write-Host "Error: Python 3.10-3.12 is required for full Homerun setup." -ForegroundColor Red
    if (Get-Command python -ErrorAction SilentlyContinue) {
        $detected = python --version 2>&1
        Write-Host "Detected python: $detected" -ForegroundColor Yellow
    }
    Write-Host "Install Python 3.12 or 3.11 and rerun setup." -ForegroundColor Yellow
    exit 1
}

$pythonCommand = [string]$pythonCandidate.command
$pythonPrefixArgs = [string[]]$pythonCandidate.prefix
$pythonVersion = Get-PythonVersionString -Command $pythonCommand -PrefixArgs $pythonPrefixArgs
$pythonCommandLabel = if ($pythonPrefixArgs.Count -gt 0) {
    "$pythonCommand $($pythonPrefixArgs -join ' ')"
} else {
    $pythonCommand
}
Write-Host "Found Python $pythonVersion via $pythonCommandLabel"

# Check Node.js
try {
    $nodeVersion = node --version 2>&1
    Write-Host "Found Node.js $nodeVersion"
} catch {
    Write-Host "Error: Node.js is required but not found in PATH." -ForegroundColor Red
    Write-Host "Download from https://nodejs.org/" -ForegroundColor Yellow
    exit 1
}

# Setup backend
Write-Host ""
Write-Host "Setting up backend..." -ForegroundColor Cyan

Push-Location backend

if (Test-Path "venv") {
    $venvPython = ".\venv\Scripts\python.exe"
    if (-not (Test-Path $venvPython)) {
        Write-Host "Existing backend\\venv is missing Python. Recreating virtual environment..." -ForegroundColor Yellow
        Remove-Item -Recurse -Force "venv"
    } else {
        try {
            $venvVersionRaw = & $venvPython --version 2>&1
            $venvMatch = [regex]::Match($venvVersionRaw, '(\d+)\.(\d+)')
            $venvMajor = [int]$venvMatch.Groups[1].Value
            $venvMinor = [int]$venvMatch.Groups[2].Value
            if ($venvMajor -ne 3 -or $venvMinor -lt $pythonMinMinor -or $venvMinor -gt $pythonMaxMinor) {
                Write-Host "Existing backend\\venv uses unsupported Python $venvVersionRaw. Recreating virtual environment..." -ForegroundColor Yellow
                Remove-Item -Recurse -Force "venv"
            }
        } catch {
            Write-Host "Existing backend\\venv could not be validated. Recreating virtual environment..." -ForegroundColor Yellow
            if (Test-Path "venv") {
                Remove-Item -Recurse -Force "venv"
            }
        }
    }
}

if (-not (Test-Path "venv")) {
    Write-Host "Creating Python virtual environment..."
    & $pythonCommand @pythonPrefixArgs -m venv venv
}

Write-Host "Activating virtual environment..."
& .\venv\Scripts\Activate.ps1

try {
    $activePythonVersion = python --version 2>&1
    $activeMatch = [regex]::Match($activePythonVersion, '(\d+)\.(\d+)')
    $activeMajor = [int]$activeMatch.Groups[1].Value
    $activeMinor = [int]$activeMatch.Groups[2].Value
    if ($activeMajor -ne 3 -or $activeMinor -lt $pythonMinMinor -or $activeMinor -gt $pythonMaxMinor) {
        Write-Host "Error: backend virtualenv uses unsupported Python $activePythonVersion (requires 3.10-3.12)." -ForegroundColor Red
        Write-Host "Delete backend\\venv and rerun setup." -ForegroundColor Yellow
        exit 1
    }
} catch {
    Write-Host "Error: failed to verify backend virtualenv Python version." -ForegroundColor Red
    exit 1
}

Write-Host "Installing Python dependencies..."
python -m pip install --quiet --upgrade pip
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to upgrade pip in backend virtualenv." -ForegroundColor Red
    exit 1
}

python -m pip install --quiet -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to install backend dependencies from requirements.txt." -ForegroundColor Red
    exit 1
}

python -c "import socksio" *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: SOCKS5 proxy support dependency 'socksio' is missing after install." -ForegroundColor Red
    Write-Host "Run: python -m pip install `"httpx[socks]>=0.27.0,<1.0`"" -ForegroundColor Yellow
    exit 1
}

Write-Host "Installing trading dependencies..."
python -m pip install --quiet -r requirements-trading.txt
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to install trading dependencies from requirements-trading.txt." -ForegroundColor Red
    exit 1
}

python -c "import py_clob_client, eth_account" *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: trading dependencies are missing after install." -ForegroundColor Red
    Write-Host "Expected imports: py_clob_client, eth_account" -ForegroundColor Yellow
    exit 1
}

Pop-Location

# Setup frontend
Write-Host ""
Write-Host "Setting up frontend..." -ForegroundColor Cyan

Push-Location frontend

Write-Host "Installing Node.js dependencies..."
npm install --silent 2>$null
if ($LASTEXITCODE -ne 0) {
    npm install
}
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to install frontend dependencies." -ForegroundColor Red
    exit 1
}

Pop-Location

Write-Host ""
Write-Host "Setting up launcher tooling..." -ForegroundColor Cyan
$toolingInstallOk = $false
try {
    $originalCxxFlags = $env:CXXFLAGS
    $env:CXXFLAGS = "$($env:CXXFLAGS) /std:c++20".Trim()
    $ErrorActionPreference = "Continue"
    npm --prefix scripts/infra/tooling install --silent 2>$null
    if ($LASTEXITCODE -ne 0) {
        npm --prefix scripts/infra/tooling install 2>$null
    }
    $toolingInstallOk = ($LASTEXITCODE -eq 0)
} catch {
    $toolingInstallOk = $false
} finally {
    $env:CXXFLAGS = $originalCxxFlags
    $ErrorActionPreference = "Stop"
}

if ($toolingInstallOk) {
    Write-Host "Verifying PowerShell launcher syntax..." -ForegroundColor Cyan
    try {
        $ErrorActionPreference = "Continue"
        node .\scripts\infra\tooling\check_powershell_syntax.mjs .\scripts\infra\run.ps1 .\scripts\infra\setup.ps1
        $ErrorActionPreference = "Stop"
        if ($LASTEXITCODE -ne 0) {
            Write-Host "PowerShell syntax verification failed (non-fatal)." -ForegroundColor Yellow
        }
    } catch {
        $ErrorActionPreference = "Stop"
        Write-Host "PowerShell syntax verification failed (non-fatal)." -ForegroundColor Yellow
    }
} else {
    Write-Host "Launcher tooling install failed (native module build issue); skipping syntax check." -ForegroundColor Yellow
    Write-Host "This is non-fatal - the application will still run." -ForegroundColor Yellow
}

# Create data directory
if (-not (Test-Path "data")) {
    New-Item -ItemType Directory -Path "data" | Out-Null
}

Write-Host ""
Write-Host "Ensuring Postgres runtime prerequisites..." -ForegroundColor Cyan
if (-not (Ensure-PostgresRuntime)) {
    exit 1
}

# Write setup fingerprint so run.ps1 can detect drift and auto-rerun setup.
function Get-HashOrMissing {
    param([string]$Path)
    if (-not (Test-Path $Path)) { return "missing" }
    return (Get-FileHash -Path $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

$stamp = @{
    python_version = (& .\backend\venv\Scripts\python.exe -c "import platform; print(platform.python_version())")
    requirements_sha256 = Get-HashOrMissing "backend\requirements.txt"
    requirements_trading_sha256 = Get-HashOrMissing "backend\requirements-trading.txt"
    package_json_sha256 = Get-HashOrMissing "frontend\package.json"
    package_lock_sha256 = Get-HashOrMissing "frontend\package-lock.json"
    launcher_tools_package_json_sha256 = if ($toolingInstallOk) { Get-HashOrMissing "scripts\infra\tooling\package.json" } else { "skipped" }
    launcher_tools_package_lock_sha256 = if ($toolingInstallOk) { Get-HashOrMissing "scripts\infra\tooling\package-lock.json" } else { "skipped" }
}

$stamp | ConvertTo-Json | Set-Content -Path ".setup-stamp.json" -Encoding UTF8
Write-Host "Wrote .setup-stamp.json"

Write-Host ""
Write-Host "=========================================" -ForegroundColor Green
Write-Host "  Setup Complete!" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green
Write-Host ""
Write-Host "To start the application, run:"
Write-Host "  .\scripts\infra\run.ps1" -ForegroundColor Cyan
Write-Host ""
Write-Host "Or run runtime validation only:"
Write-Host "  .\scripts\infra\run.ps1 --services-smoke-test" -ForegroundColor Cyan
Write-Host ""
Write-Host "Or start services individually:"
Write-Host "  Backend:  cd backend; .\venv\Scripts\Activate.ps1; uvicorn main:app --reload" -ForegroundColor Gray
Write-Host "  Frontend: cd frontend; npm run dev" -ForegroundColor Gray
Write-Host ""
Write-Host "The app will be available at:"
Write-Host "  Frontend: http://localhost:3000" -ForegroundColor Cyan
Write-Host "  Backend:  http://localhost:8000" -ForegroundColor Cyan
Write-Host "  API Docs: http://localhost:8000/docs" -ForegroundColor Cyan
Write-Host ""

