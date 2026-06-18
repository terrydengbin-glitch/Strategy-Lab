param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("start", "stop", "restart", "status")]
    [string]$Action,

    [string]$Profile = "",
    [int]$ApiPort = 8000,
    [int]$UiPort = 5173
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = (Resolve-Path (Join-Path $ScriptDir "..")).Path
$WebRoot = Join-Path $Root "web"
$RuntimeDir = Join-Path $Root "DATA\runtime"
New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null

$ApiBase = "http://127.0.0.1:$ApiPort"

function Invoke-LaomaApi {
    param(
        [string]$Method,
        [string]$Path,
        [object]$Body = $null,
        [int]$TimeoutSec = 20
    )
    try {
        $uri = "$ApiBase$Path"
        if ($null -eq $Body) {
            return Invoke-RestMethod -Method $Method -Uri $uri -TimeoutSec $TimeoutSec
        }
        $json = $Body | ConvertTo-Json -Depth 20
        return Invoke-RestMethod -Method $Method -Uri $uri -ContentType "application/json; charset=utf-8" -Body $json -TimeoutSec $TimeoutSec
    }
    catch {
        return @{
            ok = $false
            error = $_.Exception.Message
            path = $Path
        }
    }
}

function Test-ApiHealthy {
    $resp = Invoke-LaomaApi -Method "GET" -Path "/api/health" -TimeoutSec 5
    return [bool]($resp.ok -eq $true)
}

function Wait-ApiHealthy {
    param([int]$TimeoutSec = 45)
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (Test-ApiHealthy) { return $true }
        Start-Sleep -Seconds 1
    }
    return $false
}

function Get-LaomaProcesses {
    $escapedRoot = $Root.Replace("\", "\\")
    Get-CimInstance Win32_Process | Where-Object {
        $cmd = [string]$_.CommandLine
        if ([string]::IsNullOrWhiteSpace($cmd)) { return $false }
        if ($_.ProcessId -eq $PID) { return $false }
        if ($cmd -like "*laoma_signal_engine.api.app*" -or $cmd -like "*uvicorn*laoma_signal_engine.api.app*") {
            return $true
        }
        return (
            ($cmd -like "*$Root*" -or $cmd -like "*$escapedRoot*") -and
            (
                $cmd -like "*vite*" -or
                $cmd -like "*micro-collector-daemon*" -or
                $cmd -like "*paper-daemon-run*" -or
                $cmd -like "*snapshot-daemon*" -or
                $cmd -like "*strategy4-observe*daemon*"
            )
        )
    }
}

function Stop-LaomaProcesses {
    $procs = @(Get-LaomaProcesses)
    foreach ($proc in $procs) {
        try {
            Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
            Write-Host "stopped pid=$($proc.ProcessId) name=$($proc.Name)"
        }
        catch {
            Write-Host "stop_failed pid=$($proc.ProcessId) name=$($proc.Name) error=$($_.Exception.Message)"
        }
    }
}

function Start-FastApi {
    if (Test-ApiHealthy) {
        Write-Host "fastapi already healthy"
        return
    }
    $out = Join-Path $RuntimeDir "fastapi.out.log"
    $err = Join-Path $RuntimeDir "fastapi.err.log"
    Start-Process -FilePath "python" `
        -ArgumentList @("-m", "uvicorn", "laoma_signal_engine.api.app:app", "--host", "127.0.0.1", "--port", "$ApiPort") `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $out `
        -RedirectStandardError $err `
        -WindowStyle Hidden
    if (-not (Wait-ApiHealthy -TimeoutSec 60)) {
        throw "FastAPI did not become healthy. See $err"
    }
    Write-Host "fastapi started on $ApiBase"
}

function Clear-StaleMicroPid {
    $pidFile = Join-Path $Root "DATA\runtime\micro_daemon.pid"
    if (-not (Test-Path $pidFile)) { return }
    try {
        $raw = Get-Content -Path $pidFile -Encoding UTF8 -Raw | ConvertFrom-Json
        $mpid = [int]$raw.pid
    }
    catch {
        return
    }
    if ($mpid -le 0) { return }
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $mpid" -ErrorAction SilentlyContinue
    if ($null -eq $proc) { return }
    $cmd = [string]$proc.CommandLine
    $name = [string]$proc.Name
    if ($cmd -notlike "*micro-collector-daemon*" -and $cmd -notlike "*laoma_signal_engine.cli*micro-daemon*" -and $name -notlike "python*") {
        Remove-Item -LiteralPath $pidFile -Force
        Write-Host "removed stale micro pid registry pid=$mpid actual=$name"
    }
}

function Start-Frontend {
    $existing = @(Get-LaomaProcesses | Where-Object { [string]$_.CommandLine -like "*vite*" })
    if ($existing.Count -gt 0) {
        Write-Host "vite already running pid=$($existing[0].ProcessId)"
        return
    }
    $out = Join-Path $RuntimeDir "vite.out.log"
    $err = Join-Path $RuntimeDir "vite.err.log"
    Start-Process -FilePath "npm.cmd" `
        -ArgumentList @("run", "dev", "--", "--host", "127.0.0.1", "--port", "$UiPort") `
        -WorkingDirectory $WebRoot `
        -RedirectStandardOutput $out `
        -RedirectStandardError $err `
        -WindowStyle Hidden
    Write-Host "vite start requested http://127.0.0.1:$UiPort"
}

function Start-Strategy4Daemon {
    $existing = @(Get-LaomaProcesses | Where-Object { [string]$_.CommandLine -like "*strategy4-observe*daemon*" })
    if ($existing.Count -gt 0) {
        Write-Host "strategy4 daemon already running pid=$($existing[0].ProcessId)"
        return
    }
    $out = Join-Path $RuntimeDir "strategy4_daemon.out.log"
    $err = Join-Path $RuntimeDir "strategy4_daemon.err.log"
    Start-Process -FilePath "python" `
        -ArgumentList @("-m", "laoma_signal_engine.cli", "strategy4-observe", "daemon", "--project-root", $Root) `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $out `
        -RedirectStandardError $err `
        -WindowStyle Hidden
    Write-Host "strategy4 daemon start requested"
}

function Start-SnapshotDaemon {
    $existing = @(Get-LaomaProcesses | Where-Object { [string]$_.CommandLine -like "*snapshot-daemon*run*" })
    if ($existing.Count -gt 0) {
        Write-Host "snapshot daemon already running pid=$($existing[0].ProcessId)"
        return
    }
    $out = Join-Path $RuntimeDir "snapshot_daemon.out.log"
    $err = Join-Path $RuntimeDir "snapshot_daemon.err.log"
    Start-Process -FilePath "python" `
        -ArgumentList @("-m", "laoma_signal_engine.cli", "snapshot-daemon", "run", "--project-root", $Root, "--stdout-json") `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $out `
        -RedirectStandardError $err `
        -WindowStyle Hidden
    Write-Host "snapshot daemon start requested"
}

function Show-Status {
    $api = Invoke-LaomaApi -Method "GET" -Path "/api/health" -TimeoutSec 5
    $runtime = Invoke-LaomaApi -Method "GET" -Path "/api/runtime/status" -TimeoutSec 15
    $strategy4 = Invoke-LaomaApi -Method "GET" -Path "/api/strategy4/runtime" -TimeoutSec 15
    $profiles = Invoke-LaomaApi -Method "GET" -Path "/api/config/profiles" -TimeoutSec 15
    $procs = @(Get-LaomaProcesses | Select-Object ProcessId, Name, CommandLine)
    [pscustomobject]@{
        api = $api
        active_profile = $profiles.data.active_profile
        runtime_status = $runtime.data.status
        runtime_errors = $runtime.data.errors
        strategy4_ok = $strategy4.ok
        strategy4_state = $strategy4.data.status.state
        processes = $procs
    } | ConvertTo-Json -Depth 8
}

if ($Action -in @("stop", "restart")) {
    if (Test-ApiHealthy) {
        Invoke-LaomaApi -Method "POST" -Path "/api/pipeline/stop" -TimeoutSec 20 | Out-Null
        Invoke-LaomaApi -Method "POST" -Path "/api/runtime/stop" -TimeoutSec 60 | Out-Null
    }
    Stop-LaomaProcesses
    if ($Action -eq "stop") {
        Show-Status
        exit 0
    }
}

if ($Action -eq "start" -or $Action -eq "restart") {
    Start-FastApi
    if (-not [string]::IsNullOrWhiteSpace($Profile)) {
        $applied = Invoke-LaomaApi -Method "POST" -Path "/api/config/profiles/$Profile/apply" -TimeoutSec 30
        Write-Host "profile_apply=$($applied.ok) profile=$Profile"
    }
    Clear-StaleMicroPid
    Invoke-LaomaApi -Method "POST" -Path "/api/runtime/start" -TimeoutSec 90 | Out-Null
    Start-SnapshotDaemon
    Start-Strategy4Daemon
    Start-Frontend
    Start-Sleep -Seconds 5
    Show-Status
    exit 0
}

Show-Status
