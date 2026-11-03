[CmdletBinding()]
param(
 [Parameter(Mandatory=$true)][string]$Source,
 [string]$ProjectName='chatgpt2api-go',
 [string]$ComposeFile=(Join-Path $PSScriptRoot '../compose.yml'),
 [string]$EnvFile=(Join-Path $PSScriptRoot '../.env'),
 [switch]$ReplaceExisting
)
$ErrorActionPreference='Stop'
$backupSource=(Resolve-Path -LiteralPath $Source).Path
$manifest=Get-Content -LiteralPath (Join-Path $backupSource 'manifest.json') -Raw | ConvertFrom-Json
if($manifest.version -ne 1){throw 'Unsupported backup manifest.'}
$expectedNames=@('database.dump','images.tar.gz','environment.env','compose.yml')
if(@($manifest.files).Count -ne $expectedNames.Count){throw 'Incomplete backup manifest.'}
foreach($name in $expectedNames){
 $entry=@($manifest.files | Where-Object name -eq $name)
 if($entry.Count -ne 1){throw "Missing or duplicate manifest entry: $name"}
 $path=Join-Path $backupSource $name
 if(!(Test-Path -LiteralPath $path) -or (Get-Item -LiteralPath $path).Length -ne $entry[0].bytes -or (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash -ne $entry[0].sha256){throw "Backup verification failed: $name"}
}
$composePath=(Resolve-Path -LiteralPath $ComposeFile).Path
$envPath=(Resolve-Path -LiteralPath $EnvFile).Path
$composeArgs=@('compose','--env-file',$envPath,'-f',$composePath,'-p',$ProjectName)
function Invoke-Docker([string[]]$Arguments){ & docker @Arguments; if($LASTEXITCODE -ne 0){throw 'Docker operation failed.'} }
Invoke-Docker ($composeArgs+@('up','-d','postgres'))
$pgId=([string](& docker @composeArgs ps -q postgres)).Trim()
if(!$pgId){throw 'Unable to locate the PostgreSQL container.'}
# 短时有界等待数据库启动；期间不输出连接密码。
$ready=$false
for($i=0;$i -lt 60;$i++){
 & docker exec $pgId sh -ec 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"' *> $null
 if($LASTEXITCODE -eq 0){$ready=$true;break}
 Start-Sleep -Milliseconds 500
}
if(!$ready){throw 'PostgreSQL did not become ready.'}
if(!$ReplaceExisting){
 $tableCount=(& docker exec $pgId sh -ec 'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT count(*) FROM pg_tables WHERE schemaname=''public''"').Trim()
 if($LASTEXITCODE -ne 0 -or $tableCount -ne '0'){throw 'Target database is not empty. Use a fresh project, or explicitly pass -ReplaceExisting.'}
}
Invoke-Docker ($composeArgs+@('create','core'))
$coreId=([string](& docker @composeArgs ps -a -q core)).Trim()
if(!$coreId){throw 'Unable to locate the target core container.'}
if(!$ReplaceExisting){
 $imageCount=(& docker run --rm --volumes-from "${coreId}:ro" alpine:3.23 sh -ec 'find /data/images -mindepth 1 -maxdepth 1 | wc -l').Trim()
 if($LASTEXITCODE -ne 0 -or $imageCount -ne '0'){throw 'Target image volume is not empty.'}
}
Invoke-Docker ($composeArgs+@('stop','core'))
$restored=$false
try {
 Invoke-Docker @('cp',(Join-Path $backupSource 'database.dump'),"${pgId}:/tmp/imagecore-restore.dump")
 Invoke-Docker @('exec',$pgId,'sh','-ec','PGPASSWORD="$POSTGRES_PASSWORD" pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists --no-owner --no-privileges --exit-on-error /tmp/imagecore-restore.dump')
 if($ReplaceExisting){
  # 删除范围固定为目标 core 的图片卷，不能展开成宿主路径。
  Invoke-Docker @('run','--rm','--volumes-from',$coreId,'alpine:3.23','sh','-ec','find /data/images -mindepth 1 -maxdepth 1 -exec rm -rf {} +')
 }
 Invoke-Docker @('run','--rm','--volumes-from',$coreId,'--mount',"type=bind,source=$backupSource,target=/backup,readonly",'alpine:3.23','tar','-xzf','/backup/images.tar.gz','-C','/data/images')
 $restored=$true
} finally {
 & docker exec $pgId rm -f /tmp/imagecore-restore.dump | Out-Null
 if(!$restored){Write-Warning 'Restoration failed; core stays stopped. Fix the error before starting it.'}
}
Invoke-Docker ($composeArgs+@('up','-d','--no-deps','core'))
Write-Host 'Restoration complete. Check /readyz and admin account/image/log counts.'
