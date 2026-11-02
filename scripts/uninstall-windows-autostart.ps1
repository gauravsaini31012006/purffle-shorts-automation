$ErrorActionPreference = "Stop"
$TaskName = "PurffleShorts-Studio"

$Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -eq $Task) {
    Write-Host "$TaskName is not installed."
    exit 0
}

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
Write-Host "Removed $TaskName. Your PurffleShorts files and videos were not changed."
