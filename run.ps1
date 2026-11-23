[CmdletBinding()]
param(
    [ValidateSet('fixture','live')][string]$Mode = 'fixture',
    [ValidateSet('Check','Start','Status','Stop')][string]$Action = 'Check'
)

$ErrorActionPreference = 'Stop'
$Mode = $Mode.ToLowerInvariant()
$project = "fyp-pc5-six-day-$Mode"
$settings = @{
    FYP_RUN_MODE = $Mode
    FYP_ENABLE_LIVE = $(if ($Mode -eq 'live') { '1' } else { '0' })
    FYP_RUN_NETWORK = $(if ($Mode -eq 'live') { 'bridge' } else { 'none' })
    FYP_RESTART_POLICY = $(if ($Mode -eq 'live') { 'unless-stopped' } else { 'on-failure:2' })
}
$previous = @{}
Push-Location -LiteralPath $PSScriptRoot
try {
    foreach ($name in $settings.Keys) {
        $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        [Environment]::SetEnvironmentVariable($name, $settings[$name], 'Process')
    }
    $compose = @('compose','-p',$project,'-f','compose.yaml')
    if ($Action -in @('Check','Start')) {
        $manifest = Get-Content -LiteralPath 'routes/manifest.json' -Raw | ConvertFrom-Json
        foreach ($lane in @('west','east')) {
            $sha = (Get-FileHash -LiteralPath "routes/$lane/route.json" -Algorithm SHA256).Hash
            if ($sha -ne $manifest.routes.$lane.sha256 -or $manifest.routes.$lane.windows -ne 5000) {
                throw "Route verification failed for $lane."
            }
        }
    }
    switch ($Action) {
        Check { & docker @compose config --quiet }
        Start {
            $info = & docker info --format '{{.OSType}}|{{.MemTotal}}|{{.NCPU}}'
            if ($LASTEXITCODE -ne 0) { throw 'Start Docker Desktop and retry.' }
            $engine = ($info -join '').Split('|')
            $minRam = if ($Mode -eq 'live') { 10GB } else { 8GB }
            if ($engine.Length -ne 3 -or $engine[0] -ne 'linux' -or [long]$engine[1] -lt $minRam -or [int]$engine[2] -lt 4) {
                throw 'Use the Linux Docker engine with at least 4 CPU cores and 8 GiB RAM for the test (10 GiB for live).'
            }
            & docker @compose up --build -d
        }
        Status { & docker @compose ps --all }
        Stop { & docker @compose stop collector-west collector-east }
    }
    if ($LASTEXITCODE -ne 0) { throw "Docker $Action failed for $project." }
} finally {
    foreach ($name in $previous.Keys) {
        [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process')
    }
    Pop-Location
}
