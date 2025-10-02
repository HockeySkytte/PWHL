@echo off
echo Starting PWHL Analytics App...
cd /d "c:\Users\larss\Apps\PWHL"
echo App will be available at: http://localhost:8501
C:/Users/larss/Apps/PWHL/.venv/Scripts/python.exe flask_app.py
pause