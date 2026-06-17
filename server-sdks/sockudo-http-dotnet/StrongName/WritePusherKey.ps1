$ErrorActionPreference = "stop";
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition;
Push-Location $scriptDir
try {
    $keyText = $Env:CI_CODE_SIGN_KEY;
    if ($keyText) {
        $key = [System.Convert]::FromBase64String($keyText);
        $fileInfo = [System.IO.FileInfo]::new("$scriptDir\..\PusherServer.snk");
        [System.IO.File]::WriteAllBytes($fileInfo.FullName, $key) | Out-Null;
    }
    else {
        throw "The environment variable CI_CODE_SIGN_KEY is undefined. It needs to be a base 64 encoded key.";
    }
}
finally {
    Pop-Location;
}