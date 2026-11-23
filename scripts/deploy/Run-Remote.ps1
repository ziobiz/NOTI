#Requires -Version 5.1
param(
    [Parameter(Mandatory = $true)][string]$RemoteCmd
)
$ErrorActionPreference = "Stop"
$CredPath = Join-Path $env:USERPROFILE ".noti-deploy\credentials.env"
$AskPassCmd = Join-Path $env:TEMP "noti-deploy-askpass.cmd"
$map = @{}
Get-Content -LiteralPath $CredPath -Encoding UTF8 | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }
    $i = $line.IndexOf("=")
    if ($i -lt 1) { return }
    $k = $line.Substring(0, $i).Trim()
    $v = $line.Substring($i + 1).Trim()
    if (($v.StartsWith('"') -and $v.EndsWith('"')) -or ($v.StartsWith("'") -and $v.EndsWith("'"))) {
        $v = $v.Substring(1, $v.Length - 2)
    }
    $map[$k] = $v
}
$hostName = [string]$map["SSH_HOST"]
$user = [string]$map["SSH_USER"]
$pass = [string]$map["SSH_PASSWORD"]
$safe = $pass.Replace("%", "%%").Replace("^", "^^").Replace("&", "^&").Replace("|", "^|").Replace("<", "^<").Replace(">", "^>")
@"
@echo off
echo $safe
"@ | Set-Content -LiteralPath $AskPassCmd -Encoding ASCII -Force
$env:SSH_ASKPASS = $AskPassCmd
$env:SSH_ASKPASS_REQUIRE = "force"
$env:DISPLAY = "localhost:0"
try {
    $target = "${user}@${hostName}"
    & ssh -o StrictHostKeyChecking=accept-new -o PreferredAuthentications=password -o PubkeyAuthentication=no -o NumberOfPasswordPrompts=1 $target $RemoteCmd
    if ($LASTEXITCODE -ne 0) { throw "SSH exit $LASTEXITCODE" }
}
finally {
    Remove-Item -LiteralPath $AskPassCmd -Force -ErrorAction SilentlyContinue
}
