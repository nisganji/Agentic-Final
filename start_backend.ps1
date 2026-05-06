$logPath = "$PSScriptRoot\backend.start.log"
Add-Content -Path $logPath -Value "$(Get-Date -Format o) starting backend script"

Set-Location -Path $PSScriptRoot

$envPath = Join-Path $PSScriptRoot ".env"
$envLines = Get-Content -Path $envPath
foreach ($line in $envLines) {
    if ($line -match '^\s*#' -or $line -notmatch '=') { continue }
    $name, $value = $line -split '=', 2
    $name = $name.Trim()
    $value = $value.Trim().Trim('"').Trim("'")
    if ($name) { Set-Item -Path "Env:$name" -Value $value }
}

Add-Content -Path $logPath -Value "$(Get-Date -Format o) running uvicorn"
python -m uvicorn server:app --host 127.0.0.1 --port 8000
Add-Content -Path $logPath -Value "$(Get-Date -Format o) uvicorn exited with code $LASTEXITCODE"
