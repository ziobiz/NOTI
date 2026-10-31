#Requires -Version 5.1
<#
.SYNOPSIS
  NOTI(server.js 등) → SCP → PM2 restart

.EXAMPLE
  .\scripts\deploy\Deploy-Noti.ps1
  .\scripts\deploy\Deploy-Noti.ps1 -Files @('server.js','lib/elementpayNoti.js')

.NOTES
  자격증명: %USERPROFILE%\.noti-deploy\credentials.env (Git 금지)
#>
param(
    [string[]]$Files = @('server.js'),
    [switch]$SkipRestart,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"
$ScriptDir = $PSScriptRoot
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir "..\..")).Path
$CredPath = Join-Path $env:USERPROFILE ".noti-deploy\credentials.env"
$AskPassCmd = Join-Path $env:TEMP "noti-deploy-askpass.cmd"

function Read-Credentials([string]$path) {
    if (-not (Test-Path $path)) {
        throw "자격증명 없음: $path`n→ NOTI SSH 정보를 해당 파일에 저장하세요 (Git 커밋 금지)."
    }
    $map = @{}
    Get-Content -LiteralPath $path -Encoding UTF8 | ForEach-Object {
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
    return $map
}

function Require-Key($map, [string]$key) {
    if (-not $map.ContainsKey($key) -or [string]::IsNullOrWhiteSpace([string]$map[$key])) {
        throw "credentials.env 에 $key 가 필요합니다: $CredPath"
    }
    return [string]$map[$key]
}

function Write-AskPass([string]$password) {
    $safe = $password.Replace("%", "%%").Replace("^", "^^").Replace("&", "^&").Replace("|", "^|").Replace("<", "^<").Replace(">", "^>")
    @"
@echo off
echo $safe
"@ | Set-Content -LiteralPath $AskPassCmd -Encoding ASCII -Force
}

function Clear-AskPass {
    Remove-Item -LiteralPath $AskPassCmd -Force -ErrorAction SilentlyContinue
    Remove-Item Env:SSH_ASKPASS -ErrorAction SilentlyContinue
    Remove-Item Env:SSH_ASKPASS_REQUIRE -ErrorAction SilentlyContinue
    Remove-Item Env:DISPLAY -ErrorAction SilentlyContinue
}

function Build-SshBaseArgs($c) {
    $port = if ($c.ContainsKey("SSH_PORT") -and $c["SSH_PORT"]) { [int]$c["SSH_PORT"] } else { 22 }
    $args = @(
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=30",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=10"
    )
    $key = if ($c.ContainsKey("SSH_KEY_PATH")) { [string]$c["SSH_KEY_PATH"] } else { "" }
    if ($key -and (Test-Path $key)) {
        $args += @("-i", $key, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes")
    } else {
        $pass = if ($c.ContainsKey("SSH_PASSWORD")) { [string]$c["SSH_PASSWORD"] } else { "" }
        if ([string]::IsNullOrWhiteSpace($pass)) {
            throw "SSH_KEY_PATH 또는 SSH_PASSWORD 가 필요합니다."
        }
        Write-AskPass $pass
        $env:SSH_ASKPASS = $AskPassCmd
        $env:SSH_ASKPASS_REQUIRE = "force"
        $env:DISPLAY = "localhost:0"
        $args += @("-o", "PreferredAuthentications=password", "-o", "PubkeyAuthentication=no", "-o", "NumberOfPasswordPrompts=1")
    }
    return @{ Port = $port; Args = $args }
}

function Invoke-Remote($c, [string]$remoteCmd) {
    $hostName = Require-Key $c "SSH_HOST"
    $user = Require-Key $c "SSH_USER"
    $base = Build-SshBaseArgs $c
    $target = "${user}@${hostName}"
    $remoteCmd = ($remoteCmd -replace "`r`n", "`n") -replace "`r", "`n"
    $all = @("-p", "$($base.Port)") + $base.Args + @($target, $remoteCmd)
    Write-Host "SSH> $remoteCmd"
    if ($WhatIf) { return 0 }
    & ssh @all 2>&1 | ForEach-Object { Write-Host $_ }
    $ec = 0
    if ($null -ne $LASTEXITCODE) { $ec = [int]$LASTEXITCODE }
    return $ec
}

function Invoke-RemoteOrThrow($c, [string]$remoteCmd) {
    $ec = Invoke-Remote $c $remoteCmd
    if ($ec -ne 0) { throw "SSH 실패 (exit $ec): $remoteCmd" }
}

function Copy-ToRemote($c, [string]$localPath, [string]$remotePath) {
    $hostName = Require-Key $c "SSH_HOST"
    $user = Require-Key $c "SSH_USER"
    $base = Build-SshBaseArgs $c
    $dest = "${user}@${hostName}:${remotePath}"
    $all = @("-P", "$($base.Port)") + $base.Args + @($localPath, $dest)
    Write-Host "SCP> $localPath → $dest"
    if ($WhatIf) { return }
    & scp @all
    if ($LASTEXITCODE -ne 0) { throw "SCP 실패: $localPath" }
}

try {
    Write-Host "=== NOTI 운영 배포 ==="
    Write-Host "Repo: $RepoRoot"
    $c = Read-Credentials $CredPath
    $remoteDir = Require-Key $c "REMOTE_NOTI_DIR"
    $pm2Name = if ($c.ContainsKey("PM2_APP_NAME") -and $c["PM2_APP_NAME"]) { [string]$c["PM2_APP_NAME"] } else { "pg-noti-relay" }

    Invoke-RemoteOrThrow $c "mkdir -p '$remoteDir'"

    foreach ($rel in $Files) {
        $local = Join-Path $RepoRoot ($rel -replace '/', '\')
        if (-not (Test-Path -LiteralPath $local)) {
            throw "로컬 파일 없음: $local"
        }
        # 종합거래 PG 필터 회귀 방지: server.js 는 ADMIN_PG_LIST_FILTER_V1 필수
        if (($rel -replace '\\', '/') -eq 'server.js') {
            $serverText = Get-Content -LiteralPath $local -Raw -Encoding UTF8
            if ($serverText -notmatch 'ADMIN_PG_LIST_FILTER_V1') {
                throw "배포 거부: server.js 에 ADMIN_PG_LIST_FILTER_V1 마커 없음 (getTransactionListLogs 공통 필터 누락). PG별 if (src===) 체인으로 되돌리지 마세요."
            }
            $idxFn = $serverText.IndexOf('function getTransactionListLogs')
            if ($idxFn -lt 0) { throw "배포 거부: getTransactionListLogs 함수를 찾지 못함" }
            $fnSlice = $serverText.Substring($idxFn, [Math]::Min(2500, $serverText.Length - $idxFn))
            if ($fnSlice -notmatch 'normalizeAdminPgSource\(src\)' -or $fnSlice -notmatch "known !== 'chillpay'") {
                throw "배포 거부: getTransactionListLogs 가 ADMIN_PG_LIST_FILTER_V1 공통 필터 형태가 아님 (normalizeAdminPgSource 기반이어야 함)."
            }
        }
        $remoteRel = ($rel -replace '\\', '/')
        $remotePath = ($remoteDir.TrimEnd('/') + '/' + $remoteRel)
        $idx = $remotePath.LastIndexOf('/')
        if ($idx -gt 0) {
            $remoteParent = $remotePath.Substring(0, $idx)
            if ($remoteParent -and $remoteParent -ne $remoteDir -and $remoteParent -ne '/') {
                Invoke-RemoteOrThrow $c "mkdir -p '$remoteParent'"
            }
        }
        Copy-ToRemote $c $local $remotePath
        Invoke-RemoteOrThrow $c "test -s '$remotePath' && ls -la '$remotePath'"
    }

    # 원격 server.js 도 동일 마커 검증 (다른 경로로 덮인 직후 재배포 시 감지)
    if ($Files | Where-Object { ($_ -replace '\\', '/') -eq 'server.js' }) {
        $verifyJs = @'
const fs = require('fs');
const s = fs.readFileSync(process.argv[1], 'utf8');
if (!s.includes('ADMIN_PG_LIST_FILTER_V1')) {
  console.error('REMOTE_MISSING_ADMIN_PG_LIST_FILTER_V1');
  process.exit(2);
}
const idx = s.indexOf('function getTransactionListLogs');
if (idx < 0) {
  console.error('REMOTE_MISSING_getTransactionListLogs');
  process.exit(3);
}
const fn = s.slice(idx, idx + 2500);
if (!fn.includes('normalizeAdminPgSource(src)') || !fn.includes("known !== 'chillpay'")) {
  console.error('REMOTE_getTransactionListLogs_NOT_GENERIC');
  process.exit(4);
}
console.log('ADMIN_PG_LIST_FILTER_V1 ok');
'@
        $verifyLocal = Join-Path $env:TEMP 'noti-verify-pg-filter.js'
        Set-Content -LiteralPath $verifyLocal -Value $verifyJs -Encoding UTF8
        $remoteVerify = ($remoteDir.TrimEnd('/') + '/scripts/noti-verify-pg-filter.js')
        Invoke-RemoteOrThrow $c "mkdir -p '$remoteDir/scripts'"
        Copy-ToRemote $c $verifyLocal $remoteVerify
        Invoke-RemoteOrThrow $c "node '$remoteVerify' '$remoteDir/server.js'"
        Remove-Item -LiteralPath $verifyLocal -Force -ErrorAction SilentlyContinue
    }

    if (-not $SkipRestart) {
        Write-Host "[PM2] restart $pm2Name ..."
        # Prefer ecosystem/cwd restart; fall back to start if missing
        $restartCmd = @"
cd '$remoteDir' && (pm2 describe '$pm2Name' >/dev/null 2>&1 && pm2 restart '$pm2Name' --update-env || pm2 start server.js --name '$pm2Name') && pm2 save && pm2 list | head -30
"@
        Invoke-RemoteOrThrow $c $restartCmd
    }

    Write-Host "=== NOTI 배포 완료 ==="
    Write-Host "확인: https://noti.icopay.net/"
}
finally {
    Clear-AskPass
}
