[CmdletBinding()]
param(
 [Parameter(Mandatory=$true)][string]$Destination,
 [string]$ProjectName='chatgpt2api-go',
 [string]$ComposeFile=(Join-Path $PSScriptRoot '../compose.yml'),
 [string]$EnvFile=(Join-Path $PSScriptRoot '../.env')
)
$ErrorActionPreference='Stop'
$backupTarget=[IO.Path]::GetFullPath($Destination)
if(Test-Path -LiteralPath $backupTarget){throw 'Destination must be a new directory.'}
$composePath=(Resolve-Path -LiteralPath $ComposeFile).Path
$envPath=(Resolve-Path -LiteralPath $EnvFile).Path
$composeArgs=@('compose','--env-file',$envPath,'-f',$composePath,'-p',$ProjectName)
function Invoke-Docker([string[]]$Arguments){ & docker @Arguments; if($LASTEXITCODE -ne 0){throw 'Docker operation failed.'} }
$coreId=([string](& docker @composeArgs ps -a -q core)).Trim()
$pgId=([string](& docker @composeArgs ps -q postgres)).Trim()
if(!$coreId -or !$pgId){throw 'Create the core container and start PostgreSQL before backup.'}
$wasRunning=(& docker inspect --format '{{.State.Running}}' $coreId).Trim() -eq 'true'
New-Item -ItemType Directory -Path $backupTarget | Out-Null
$backupComplete=$false
try {
 if($wasRunning){Invoke-Docker ($composeArgs+@('stop','core'))}
 # 固定临时路径只在本项目 PostgreSQL 容器中使用；不把密码放到宿主命令参数。
 Invoke-Docker @('exec',$pgId,'sh','-ec','PGPASSWORD="$POSTGRES_PASSWORD" pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc -f /tmp/imagecore-backup.dump')
 Invoke-Docker @('cp',"${pgId}:/tmp/imagecore-backup.dump",(Join-Path $backupTarget 'database.dump'))
 Invoke-Docker @('run','--rm','--volumes-from',"${coreId}:ro",'--mount',"type=bind,source=$backupTarget,target=/backup",'alpine:3.23','tar','-czf','/backup/images.tar.gz','-C','/data/images','.')
 Copy-Item -LiteralPath $envPath -Destination (Join-Path $backupTarget 'environment.env')
 Copy-Item -LiteralPath $composePath -Destination (Join-Path $backupTarget 'compose.yml')
 $files=@('database.dump','images.tar.gz','environment.env','compose.yml') | ForEach-Object {
  $file=Join-Path $backupTarget $_
  @{name=$_;bytes=(Get-Item -LiteralPath $file).Length;sha256=(Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash}
 }
 @{version=1;created_at=[DateTimeOffset]::UtcNow.ToString('o');project=$ProjectName;files=@($files)} | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $backupTarget 'manifest.json') -Encoding utf8
 $backupComplete=$true
 Write-Host "Backup complete: $backupTarget"
} finally {
 & docker exec $pgId rm -f /tmp/imagecore-backup.dump | Out-Null
 if($wasRunning){Invoke-Docker ($composeArgs+@('up','-d','--no-deps','core'))}
 if(!$backupComplete){Write-Warning 'Backup did not complete; do not use the partial directory for restoration.'}
}
