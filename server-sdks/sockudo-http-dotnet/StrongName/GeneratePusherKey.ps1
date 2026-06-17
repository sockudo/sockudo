$ErrorActionPreference = "stop";
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition;
Push-Location $scriptDir;
try {
    $keyName = "PusherServer";
    .\GenerateStrongNameKey.cmd "$keyName"
    $keyBytes = [System.IO.File]::ReadAllBytes("$scriptDir\\$keyName.snk");
    $keyBase64 = [System.Convert]::ToBase64String($keyBytes);
    Write-Output "";
    Write-Output "Base 64 encoded signing key:";
    Write-Output $keyBase64;
}
finally {
    Pop-Location
}