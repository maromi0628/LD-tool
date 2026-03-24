@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"
set LOG=setup_log.txt

echo === LD Tool Setup === > "%LOG%"
echo %DATE% %TIME% >> "%LOG%"
echo. >> "%LOG%"

echo === LD Tool Setup ===
echo.

:: ── 0. 管理者権限チェック ────────────────────────────────────────
net session >nul 2>&1
if errorlevel 1 (
    echo [WARNING] 管理者権限がありません。
    echo           SQL Serverサービスの起動に失敗する可能性があります。
    echo           右クリック→「管理者として実行」で再試行することを推奨します。
    echo.
    echo [WARNING] 管理者権限なし >> "%LOG%"
) else (
    echo [OK] 管理者権限で実行中
    echo [OK] 管理者権限で実行中 >> "%LOG%"
)

:: ── 1. Python チェック ───────────────────────────────────────────
set PYTHON=
where python >nul 2>&1 && set PYTHON=python
if "!PYTHON!"=="" (
    where py >nul 2>&1 && set PYTHON=py
)
if "!PYTHON!"=="" (
    echo [ERROR] Python が見つかりません。
    echo         https://www.python.org/ からインストールし、
    echo         "Add Python to PATH" にチェックを入れてください。
    echo [ERROR] Python が見つかりません >> "%LOG%"
    pause & exit /b 1
)
for /f "delims=" %%V in ('!PYTHON! --version 2^>^&1') do set PYVER=%%V
echo [OK] !PYVER!
echo [OK] !PYVER! >> "%LOG%"

:: ── 2. .venv 作成 ─────────────────────────────────────────────────
if not exist ".venv\Scripts\python.exe" (
    echo [  ] 仮想環境を作成中...
    echo [  ] 仮想環境を作成中 >> "%LOG%"
    !PYTHON! -m venv .venv >> "%LOG%" 2>&1
    if errorlevel 1 (
        echo [ERROR] 仮想環境の作成に失敗しました。詳細は setup_log.txt を確認してください。
        echo [ERROR] 仮想環境の作成に失敗 >> "%LOG%"
        pause & exit /b 1
    )
    echo [OK] 仮想環境を作成しました。
    echo [OK] 仮想環境を作成しました >> "%LOG%"
) else (
    echo [OK] 仮想環境は既に存在します。
    echo [OK] 仮想環境は既に存在します >> "%LOG%"
)

:: ── 3. パッケージインストール ─────────────────────────────────────
echo [  ] パッケージをインストール中...
echo [  ] パッケージをインストール中 >> "%LOG%"
.venv\Scripts\pip install -q -r requirements.txt >> "%LOG%" 2>&1
if errorlevel 1 (
    echo [ERROR] パッケージのインストールに失敗しました。詳細は setup_log.txt を確認してください。
    echo [ERROR] パッケージのインストールに失敗 >> "%LOG%"
    pause & exit /b 1
)
echo [OK] パッケージをインストールしました。
echo [OK] パッケージをインストールしました >> "%LOG%"

:: ── 4. ODBC ドライバ確認 ──────────────────────────────────────────
echo [  ] ODBC Driver for SQL Server を確認中...
reg query "HKLM\SOFTWARE\ODBC\ODBCINST.INI\ODBC Driver 17 for SQL Server" >nul 2>&1 && goto ODBC_OK
reg query "HKLM\SOFTWARE\ODBC\ODBCINST.INI\ODBC Driver 18 for SQL Server" >nul 2>&1 && goto ODBC_OK

echo.
echo [WARNING] ODBC Driver for SQL Server が見つかりません。
echo           以下からインストールしてください:
echo           https://learn.microsoft.com/ja-jp/sql/connect/odbc/download-odbc-driver-for-sql-server
echo           (msodbcsql17.msi または msodbcsql18.msi をダウンロード)
echo.
echo [WARNING] ODBC Driver for SQL Server が見つかりません >> "%LOG%"
goto ODBC_DONE
:ODBC_OK
echo [OK] ODBC Driver for SQL Server が見つかりました。
echo [OK] ODBC Driver for SQL Server が見つかりました >> "%LOG%"
:ODBC_DONE

:: ── 5. Lutron SQL Server サービス設定 ────────────────────────────
echo [  ] Lutron SQL Server サービスを確認中...
echo [  ] Lutron SQL Server サービスを確認中 >> "%LOG%"
set FOUND_SVC=0
set LUTRON_VER=
for %%S in (MSSQL$LUTRON2025 MSSQL$LUTRON2024 MSSQL$LUTRON2023 MSSQL$LUTRON2022 MSSQL$LUTRON2021 MSSQL$LUTRON2020 MSSQL$LUTRON2019) do (
    sc query %%S >nul 2>&1
    if not errorlevel 1 (
        echo [OK] サービス検出: %%S
        echo [OK] サービス検出: %%S >> "%LOG%"
        echo [  ] 自動起動に設定中: %%S
        sc config %%S start=auto >nul 2>&1
        net start %%S >nul 2>&1
        if "!FOUND_SVC!"=="0" set LUTRON_VER=%%S
        set FOUND_SVC=1
    )
)
if "!FOUND_SVC!"=="0" (
    echo [WARNING] Lutron SQL Server サービスが見つかりません。
    echo           Lutron Designer がインストールされているか確認してください。
    echo [WARNING] Lutron SQL Server サービスが見つかりません >> "%LOG%"
)

:: ── 6. app.py の SQL_INSTANCE を自動更新 ────────────────────────
if not "!LUTRON_VER!"=="" (
    set LUTRON_YEAR=!LUTRON_VER:MSSQL$LUTRON=!
    echo [  ] app.py の SQL接続先を .\LUTRON!LUTRON_YEAR! に更新中...
    echo [  ] SQL_INSTANCE を .\LUTRON!LUTRON_YEAR! に更新中 >> "%LOG%"
    > _update_sql.py echo import re
    >> _update_sql.py echo year = '!LUTRON_YEAR!'
    >> _update_sql.py echo f = open^('app.py', 'r', encoding='utf-8'^)
    >> _update_sql.py echo content = f.read^(^)
    >> _update_sql.py echo f.close^(^)
    >> _update_sql.py echo pattern = r'SQL_INSTANCE = r"\.\\LUTRON\d+"'
    >> _update_sql.py echo replacement = 'SQL_INSTANCE = r".' + chr^(92^) + 'LUTRON' + year + '"'
    >> _update_sql.py echo content = re.sub^(pattern, replacement, content^)
    >> _update_sql.py echo f = open^('app.py', 'w', encoding='utf-8'^)
    >> _update_sql.py echo f.write^(content^)
    >> _update_sql.py echo f.close^(^)
    .venv\Scripts\python.exe _update_sql.py >> "%LOG%" 2>&1
    if errorlevel 1 (
        echo [WARNING] app.py の更新に失敗しました。手動で SQL_INSTANCE を変更してください。
        echo [WARNING] app.py の更新に失敗 >> "%LOG%"
    ) else (
        echo [OK] app.py の SQL接続先を更新しました。
        echo [OK] app.py の SQL接続先を更新しました >> "%LOG%"
    )
    del _update_sql.py >nul 2>&1
)

echo.
echo === セットアップ完了 ===
echo start.bat を実行してアプリを起動してください。
echo.
echo ログは setup_log.txt に保存されています。
echo.
echo === セットアップ完了 === >> "%LOG%"
echo %DATE% %TIME% >> "%LOG%"
pause
