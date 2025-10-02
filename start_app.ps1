Write-Host "Starting PWHL Analytics App..." -ForegroundColor Green
Set-Location "c:\Users\larss\Apps\PWHL"
Write-Host "App will be available at: http://localhost:8501" -ForegroundColor Yellow
& "C:/Users/larss/Apps/PWHL/.venv/Scripts/python.exe" flask_app.py