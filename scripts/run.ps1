# Homerun - Windows Run Script (TUI)
# Run: .\scripts\run.ps1

$ErrorActionPreference = "Stop"

# Navigate to project root (parent of scripts\)
Set-Location (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))

$redisHost = if ($env:REDIS_HOST) { $env:REDIS_HOST } else { "127.0.0.1" }
$redisPort = if ($env:REDIS_PORT) { [int]$env:REDIS_PORT } else { 6379 }
$redisContainerName = if ($env:REDIS_CONTAINER_NAME) { $env:REDIS_CONTAINER_NAME } else { "homerun-redis" }
$redisImage = if ($env:REDIS_IMAGE) { $env:REDIS_IMAGE } else { "redis:7-alpine" }

function Test-RedisRuntimeAvailable {
    if (Get-Command docker -ErrorAction SilentlyContinue) { return $true }
    if (Get-Command redis-server -ErrorAction SilentlyContinue) { return $true }
    try {
        $svc = Get-Service -Name "Memurai" -ErrorAction SilentlyContinue
        if ($svc) { return $true }
    } catch {
    }
    return $false
}

function Ensure-RedisRuntime {
    if (Test-RedisRuntimeAvailable) {
        return $true
    }
    Write-Host "Redis runtime missing; invoking setup redis bootstrap..." -ForegroundColor Cyan
    try {
        & .\scripts\setup.ps1 -RedisOnly
        return (Test-RedisRuntimeAvailable)
    } catch {
        return $false
    }
}

function Test-RedisPing {
    param(
        [string]$Host,
        [int]$Port
    )

    $client = $null
    $stream = $null
    try {
        $client = [System.Net.Sockets.TcpClient]::new()
        $client.ReceiveTimeout = 500
        $client.SendTimeout = 500
        $client.Connect($Host, $Port)
        $stream = $client.GetStream()
        $payload = [System.Text.Encoding]::ASCII.GetBytes("*1`r`n`$4`r`nPING`r`n")
        $stream.Write($payload, 0, $payload.Length)
        $buffer = New-Object byte[] 64
        $bytesRead = $stream.Read($buffer, 0, $buffer.Length)
        if ($bytesRead -le 0) { return $false }
        $response = [System.Text.Encoding]::ASCII.GetString($buffer, 0, $bytesRead)
        return $response.Contains("+PONG")
    } catch {
        return $false
    } finally {
        if ($stream) { $stream.Dispose() }
        if ($client) { $client.Dispose() }
    }
}

function Wait-ForRedis {
    param(
        [string]$Host,
        [int]$Port
    )

    for ($i = 0; $i -lt 20; $i++) {
        if (Test-RedisPing -Host $Host -Port $Port) {
            return $true
        }
        Start-Sleep -Milliseconds 250
    }
    return $false
}

function Start-RedisDocker {
    param(
        [string]$Host,
        [int]$Port,
        [string]$ContainerName,
        [string]$Image
    )

    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        return $false
    }
    try {
        docker info *> $null
        if ($LASTEXITCODE -ne 0) { return $false }
    } catch {
        return $false
    }

    try {
        docker container inspect $ContainerName *> $null
        if ($LASTEXITCODE -eq 0) {
            docker start $ContainerName *> $null
            return ($LASTEXITCODE -eq 0)
        }
    } catch {
    }

    try {
        docker run --name $ContainerName --detach --publish "${Host}:${Port}:6379" $Image redis-server --save "" --appendonly no *> $null
        return ($LASTEXITCODE -eq 0)
    } catch {
        return $false
    }
}

function Start-RedisLocal {
    param(
        [string]$Host,
        [int]$Port
    )

    $redisServer = Get-Command redis-server -ErrorAction SilentlyContinue
    if ($redisServer) {
        try {
            Start-Process -FilePath $redisServer.Source -ArgumentList @("--bind", $Host, "--port", "$Port", "--save", "", "--appendonly", "no") -WindowStyle Hidden | Out-Null
            return $true
        } catch {
        }
    }

    try {
        $memuraiService = Get-Service -Name "Memurai" -ErrorAction SilentlyContinue
        if ($memuraiService) {
            if ($memuraiService.Status -ne "Running") {
                Start-Service -Name "Memurai"
            }
            return $true
        }
    } catch {
    }

    return $false
}

function Ensure-Redis {
    param(
        [string]$Host,
        [int]$Port,
        [string]$ContainerName,
        [string]$Image
    )

    if (Test-RedisPing -Host $Host -Port $Port) {
        Write-Host "Redis already running on ${Host}:${Port}" -ForegroundColor Green
        return
    }

    if (-not (Ensure-RedisRuntime)) {
        Write-Host "Failed to provision Redis runtime automatically." -ForegroundColor Red
        Write-Host "Install Docker Desktop, redis-server, or Memurai, then rerun." -ForegroundColor Yellow
        exit 1
    }

    Write-Host "Starting Redis..." -ForegroundColor Cyan
    $dockerStarted = Start-RedisDocker -Host $Host -Port $Port -ContainerName $ContainerName -Image $Image
    if ($dockerStarted -and (Wait-ForRedis -Host $Host -Port $Port)) {
        Write-Host "Redis started via Docker on ${Host}:${Port}" -ForegroundColor Green
        return
    }

    $localStarted = Start-RedisLocal -Host $Host -Port $Port
    if ($localStarted -and (Wait-ForRedis -Host $Host -Port $Port)) {
        Write-Host "Redis started via redis-server on ${Host}:${Port}" -ForegroundColor Green
        return
    }

    Write-Host "Failed to start Redis automatically." -ForegroundColor Red
    Write-Host "Install Docker or redis-server, then rerun." -ForegroundColor Yellow
    exit 1
}

function Test-NeedsSetup {
    if (-not (Test-Path "backend\venv")) { return $true }
    if (-not (Test-Path "frontend\node_modules")) { return $true }
    if (-not (Test-Path ".setup-stamp.json")) { return $true }

    try {
        $stamp = Get-Content ".setup-stamp.json" -Raw | ConvertFrom-Json
    } catch {
        return $true
    }

    function Get-HashOrMissing {
        param([string]$Path)
        if (-not (Test-Path $Path)) { return "missing" }
        return (Get-FileHash -Path $Path -Algorithm SHA256).Hash.ToLowerInvariant()
    }

    $pythonVersion = (python -c "import platform; print(platform.python_version())")
    if ($stamp.python_version -ne $pythonVersion) { return $true }
    if ($stamp.requirements_sha256 -ne (Get-HashOrMissing "backend\requirements.txt")) { return $true }
    if ($stamp.requirements_trading_sha256 -ne (Get-HashOrMissing "backend\requirements-trading.txt")) { return $true }
    if ($stamp.package_json_sha256 -ne (Get-HashOrMissing "frontend\package.json")) { return $true }
    if ($stamp.package_lock_sha256 -ne (Get-HashOrMissing "frontend\package-lock.json")) { return $true }

    return $false
}

if (Test-NeedsSetup) {
    Write-Host "Setup missing or stale. Running setup..." -ForegroundColor Yellow
    & .\scripts\setup.ps1
}

Ensure-Redis -Host $redisHost -Port $redisPort -ContainerName $redisContainerName -Image $redisImage

# Activate venv
& backend\venv\Scripts\Activate.ps1

# Ensure TUI dependencies are installed
try {
    python -c "import textual" 2>$null
} catch {
    Write-Host "Installing TUI dependencies..." -ForegroundColor Cyan
    pip install -q textual rich
}

# Launch the TUI
python tui.py @args
