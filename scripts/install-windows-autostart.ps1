$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$UvCommand = Get-Command "uv" -ErrorAction Stop
$UvPath = $UvCommand.Source
$TaskName = "PurffleShorts-Studio"
$CurrentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

$Action = New-ScheduledTaskAction `
    -Execute $UvPath `
    -Argument "run purffle-shorts studio --no-browser" `
    -WorkingDirectory $RepoRoot

$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $CurrentUser
$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Starts the local PurffleShorts Studio for scheduled creator automation." `
    -Force | Out-Null

Write-Host "Installed $TaskName for $CurrentUser."
Write-Host "It will start when this Windows account signs in."
Write-Host "Publishing settings are unchanged; UPLOAD remains off unless you enable it in Studio Setup."
Write-Host "Open http://127.0.0.1:8765 after sign-in."
