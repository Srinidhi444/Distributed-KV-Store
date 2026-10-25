Get-Process -Name "distributed-kv" -ErrorAction SilentlyContinue |
    Stop-Process -Force

Write-Host "All Distributed KV nodes stopped."
