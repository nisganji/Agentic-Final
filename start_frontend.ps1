$logPath = "$PSScriptRoot\frontend.start.log"
Add-Content -Path $logPath -Value "$(Get-Date -Format o) starting frontend script"

Set-Location -Path "$PSScriptRoot\frontend"
Add-Content -Path $logPath -Value "$(Get-Date -Format o) running vite"
npm run dev -- --host 127.0.0.1 --port 5173
Add-Content -Path $logPath -Value "$(Get-Date -Format o) vite exited with code $LASTEXITCODE"
