while ($true) {
    Write-Output "`n[$(Get-Date)] Running git push sequence..."

    git add .

    git commit -m "upload 1"

    git push

    Write-Output "Done. Sleeping for 100 seconds..."

    Start-Sleep -Seconds 100  # 10 minutes
}
