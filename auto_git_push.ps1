while ($true) {
    Write-Output "`n[$(Get-Date)] Running git push sequence..."

    git add .

    git commit -m "upload 1"

    git push

    Write-Output "Done. Sleeping for 10 minutes..."

    Start-Sleep -Seconds 600  # 10 minutes
}
