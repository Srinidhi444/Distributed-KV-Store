
$ErrorActionPreference = "Stop"

$exe = Join-Path $PSScriptRoot "distributed-kv.exe"

# Build the latest code first.
Push-Location $PSScriptRoot
try {
    go build -o $exe .
    if ($LASTEXITCODE -ne 0) {
        throw "Go build failed"
    }
}
finally {
    Pop-Location
}

$nodes = @(
    @{
        ID = "node1"
        Port = 8101
        Peers = "node2=127.0.0.1:8102,node3=127.0.0.1:8103"
    },
    @{
        ID = "node2"
        Port = 8102
        Peers = "node1=127.0.0.1:8101,node3=127.0.0.1:8103"
    },
    @{
        ID = "node3"
        Port = 8103
        Peers = "node1=127.0.0.1:8101,node2=127.0.0.1:8102"
    }
)

foreach ($node in $nodes) {
    $dataDir = Join-Path $PSScriptRoot "manual-data\$($node.ID)"

    $arguments = @(
        "-id", $node.ID,
        "-addr", "127.0.0.1:$($node.Port)",
        "-data-dir", $dataDir,
        "-peers", $node.Peers
    )

    Start-Process `
        -FilePath $exe `
        -ArgumentList $arguments `
        -WorkingDirectory $PSScriptRoot

    Write-Host "Started $($node.ID) on port $($node.Port)"
}

Write-Host "`nThree-node cluster launched."
