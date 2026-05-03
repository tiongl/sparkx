@echo off
REM ─────────────────────────────────────────────────────────────────────────────
REM run-demo.bat — Launch the sparkx showcase in Spark local mode (Windows)
REM
REM Spark and winutils are downloaded automatically if not present.
REM Requires: Java 8+ on PATH, PowerShell 5+, curl/tar (Windows 10 1803+)
REM
REM Usage:
REM   run-demo.bat                          run all 5 scenarios
REM   run-demo.bat skew                     run only the data-skew scenario
REM   run-demo.bat skew --pause             run scenario then hold the Spark UI open
REM   run-demo.bat history-server           start History Server to replay past runs
REM
REM Supported scenario names: skew | straggler | gc | spill | broadcast | suggestions | all
REM ─────────────────────────────────────────────────────────────────────────────

setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
REM The sample assembly already bundles all sparkx classes — no separate --jars needed.
set SAMPLE_JAR=%SCRIPT_DIR%sample\target\scala-2.12\sparkx-sample-assembly-0.1.0.jar

set SPARK_VERSION=3.5.0
set SPARK_PKG=spark-%SPARK_VERSION%-bin-hadoop3
set SPARK_CACHE_DIR=%SCRIPT_DIR%.spark-dist
set SPARK_DOWNLOAD_URL=https://archive.apache.org/dist/spark/spark-%SPARK_VERSION%/%SPARK_PKG%.tgz

REM winutils is required by Spark on Windows (provides Hadoop filesystem primitives)
set WINUTILS_DIR=%SPARK_CACHE_DIR%\winutils
set WINUTILS_BASE_URL=https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin

REM ── 1. Check Java ────────────────────────────────────────────────────────────
where java >nul 2>&1
if errorlevel 1 (
    echo ERROR: Java not found on PATH. Spark requires Java 8 or later.
    echo   Install from: https://adoptium.net
    exit /b 1
)
echo ✓ Java found: & java -version 2>&1 | findstr /i "version"

REM ── 2. Locate or download Spark ───────────────────────────────────────────────
if defined SPARK_HOME (
    if exist "%SPARK_HOME%\bin\spark-submit.cmd" (
        echo ✓ Using existing Spark: %SPARK_HOME%
        goto spark_ready
    )
)

set SPARK_HOME=%SPARK_CACHE_DIR%\%SPARK_PKG%
if exist "%SPARK_HOME%\bin\spark-submit.cmd" (
    echo ✓ Using cached Spark: %SPARK_HOME%
    goto spark_ready
)

echo ──────────────────────────────────────────────────────────────────────
echo   Spark %SPARK_VERSION% not found. Downloading now (~350 MB) ...
echo   Source: %SPARK_DOWNLOAD_URL%
echo ──────────────────────────────────────────────────────────────────────

if not exist "%SPARK_CACHE_DIR%" mkdir "%SPARK_CACHE_DIR%"
set SPARK_TGZ=%SPARK_CACHE_DIR%\%SPARK_PKG%.tgz

REM Try curl.exe (available on Windows 10 1803+)
where curl >nul 2>&1
if not errorlevel 1 (
    curl -L --progress-bar -o "%SPARK_TGZ%" "%SPARK_DOWNLOAD_URL%"
    goto extract
)

REM Fall back to PowerShell Invoke-WebRequest
echo   (using PowerShell to download — this may take a few minutes)
powershell -NoProfile -Command ^
  "[Net.ServicePointManager]::SecurityProtocol = 'Tls12'; " ^
  "$ProgressPreference = 'SilentlyContinue'; " ^
  "Invoke-WebRequest -Uri '%SPARK_DOWNLOAD_URL%' -OutFile '%SPARK_TGZ%'"
if errorlevel 1 (
    echo ERROR: Download failed. Check your internet connection.
    exit /b 1
)

:extract
echo   Extracting to %SPARK_CACHE_DIR% ...
tar -xzf "%SPARK_TGZ%" -C "%SPARK_CACHE_DIR%"
if errorlevel 1 (
    echo ERROR: Extraction failed. 'tar' requires Windows 10 version 1803 or later.
    exit /b 1
)
del /q "%SPARK_TGZ%"
echo ✓ Spark %SPARK_VERSION% ready at: %SPARK_HOME%

:spark_ready

REM ── 3. Set up winutils (required by Spark on Windows) ────────────────────────
if defined HADOOP_HOME (
    if exist "%HADOOP_HOME%\bin\winutils.exe" (
        echo ✓ Using existing winutils: %HADOOP_HOME%
        goto winutils_ready
    )
)

set HADOOP_HOME=%WINUTILS_DIR%
if exist "%WINUTILS_DIR%\bin\winutils.exe" (
    echo ✓ Using cached winutils: %WINUTILS_DIR%
    goto winutils_ready
)

echo ──────────────────────────────────────────────────────────────────────
echo   Downloading winutils.exe for Hadoop 3 (required by Spark on Windows)
echo   Source: %WINUTILS_BASE_URL%
echo ──────────────────────────────────────────────────────────────────────

if not exist "%WINUTILS_DIR%\bin" mkdir "%WINUTILS_DIR%\bin"

powershell -NoProfile -Command ^
  "[Net.ServicePointManager]::SecurityProtocol = 'Tls12';" ^
  "$ProgressPreference = 'SilentlyContinue';" ^
  "Invoke-WebRequest -Uri '%WINUTILS_BASE_URL%/winutils.exe' -OutFile '%WINUTILS_DIR%\bin\winutils.exe';" ^
  "Invoke-WebRequest -Uri '%WINUTILS_BASE_URL%/hadoop.dll'   -OutFile '%WINUTILS_DIR%\bin\hadoop.dll'"

if not exist "%WINUTILS_DIR%\bin\winutils.exe" (
    echo ERROR: winutils download failed. Check your internet connection.
    echo   Alternatively, download manually from https://github.com/cdarlint/winutils
    echo   and set HADOOP_HOME to a directory containing bin\winutils.exe
    exit /b 1
)
echo ✓ winutils ready at: %WINUTILS_DIR%

:winutils_ready
set HADOOP_HOME=%WINUTILS_DIR%
REM Add hadoop.dll to PATH so NativeIO works on Windows
set PATH=%HADOOP_HOME%\bin;%PATH%

REM ── 4. Branch on command ─────────────────────────────────────────────────────
if /i "%1"=="history-server" goto history_server

REM ─── 4a. Run demo scenarios ──────────────────────────────────────────────────
if not exist "%SAMPLE_JAR%" (
    echo ERROR: sparkx sample JAR not found.
    echo   Run: sbt assembly ^&^& sbt "sample/assembly"
    exit /b 1
)
echo ✓ sparkx sample JAR found

set SCENARIO=%1
if "%SCENARIO%"=="" set SCENARIO=all

REM Collect any extra flags (e.g. --pause) — supports up to 4 extra args
set EXTRA=%2 %3 %4 %5
REM Trim trailing spaces
set EXTRA=%EXTRA: =%

set EVENTS_DIR=%TEMP%\spark-events
if not exist "%EVENTS_DIR%" mkdir "%EVENTS_DIR%"

echo.
echo   Launching sparkx demo (scenario: %SCENARIO%)
echo   Spark UI will be at  : http://localhost:4040
echo   sparkx tab will be at : http://localhost:4040/sparkx
if not "%EXTRA%"=="" (
    echo   Flags: %EXTRA%
)
echo.

"%SPARK_HOME%\bin\spark-submit.cmd" ^
  --master "local[*]" ^
  --conf "spark.extraListeners=com.sparkx.SparkXListener" ^
  --conf "spark.ui.enabled=true" ^
  --conf "spark.eventLog.enabled=true" ^
  --conf "spark.eventLog.dir=%EVENTS_DIR%" ^
  --conf "spark.executor.memory=1g" ^
  --class "com.sparkx.sample.SparkXDemo" ^
  "%SAMPLE_JAR%" ^
  %SCENARIO% %2 %3 %4 %5

goto end

REM ─── 4b. History Server ───────────────────────────────────────────────────────
:history_server
set XPARK_JAR=%SCRIPT_DIR%target\scala-2.12\sparkx-assembly-0.1.0.jar
if not exist "%XPARK_JAR%" (
    echo ERROR: sparkx plugin JAR not found.
    echo   Run: sbt assembly
    exit /b 1
)
echo ✓ sparkx plugin JAR found

set EVENTS_DIR=%TEMP%\spark-events
if not exist "%EVENTS_DIR%" mkdir "%EVENTS_DIR%"

echo.
echo   Starting Spark History Server
echo   Log directory : %EVENTS_DIR%
echo   UI will be at : http://localhost:18080
echo   sparkx tab     : http://localhost:18080  (visible after selecting an app)
echo   Press Ctrl+C to stop.
echo.

set SPARK_CLASSPATH=%XPARK_JAR%
set SPARK_HISTORY_OPTS=-Dspark.history.fs.logDirectory=%EVENTS_DIR% -Dspark.history.ui.port=18080

"%SPARK_HOME%\bin\spark-class.cmd" org.apache.spark.deploy.history.HistoryServer

:end
endlocal
