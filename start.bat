@echo off
cd /d "%~dp0"

:: ローカルの .venv（ドキュメントフォルダ）を優先、なければNAS上の .venv を使用
set VENV_LOCAL=C:\Users\IoT-067\Documents\.venv
set VENV_NAS=%~dp0.venv

if exist "%VENV_LOCAL%\Scripts\python.exe" (
    set VENV=%VENV_LOCAL%
) else if exist "%VENV_NAS%\Scripts\python.exe" (
    set VENV=%VENV_NAS%
) else (
    echo ERROR: Virtual environment not found. Run setup.bat first.
    pause
    exit /b 1
)

"%VENV%\Scripts\python.exe" app.py
