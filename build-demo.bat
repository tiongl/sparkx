@echo off
REM ─────────────────────────────────────────────────────────────────────────────
REM build-demo.bat — Build the sparkx plugin and sample demo JARs (Windows)
REM
REM Requires: Java 8+, sbt (https://www.scala-sbt.org/download.html)
REM
REM Produces:
REM   target\scala-2.12\sparkx-assembly-0.1.0.jar              (plugin JAR)
REM   sample\target\scala-2.12\sparkx-sample-assembly-0.1.0.jar (demo JAR)
REM
REM Usage:
REM   build-demo.bat            build both JARs
REM   build-demo.bat --clean    clean before building
REM ─────────────────────────────────────────────────────────────────────────────

setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

set PLUGIN_JAR=%SCRIPT_DIR%target\scala-2.12\sparkx-assembly-0.1.0.jar
set SAMPLE_JAR=%SCRIPT_DIR%sample\target\scala-2.12\sparkx-sample-assembly-0.1.0.jar

REM ── 1. Check prerequisites ─────────────────────────────────────────────────
where java >nul 2>&1
if errorlevel 1 (
    echo ERROR: Java not found on PATH. Install Java 8+ from https://adoptium.net
    exit /b 1
)
echo ✓ Java found: & java -version 2>&1 | findstr /i "version"

where sbt >nul 2>&1
if errorlevel 1 (
    echo ERROR: sbt not found on PATH. Install from https://www.scala-sbt.org/download.html
    exit /b 1
)
echo ✓ sbt found

REM ── 2. Optional clean ──────────────────────────────────────────────────────
if /i "%~1"=="--clean" (
    echo   Cleaning previous build artifacts ...
    call sbt clean
)

REM ── 3. Build plugin assembly ───────────────────────────────────────────────
echo.
echo ══════════════════════════════════════════════════════════════════════
echo   Building sparkx plugin JAR ...
echo ══════════════════════════════════════════════════════════════════════
call sbt assembly
if errorlevel 1 (
    echo ERROR: Plugin build failed.
    exit /b 1
)

if not exist "%PLUGIN_JAR%" (
    echo ERROR: Plugin JAR was not produced at %PLUGIN_JAR%
    exit /b 1
)
echo ✓ Plugin JAR: %PLUGIN_JAR%

REM ── 4. Build sample assembly ───────────────────────────────────────────────
echo.
echo ══════════════════════════════════════════════════════════════════════
echo   Building sparkx sample (demo) JAR ...
echo ══════════════════════════════════════════════════════════════════════
call sbt "sample/assembly"
if errorlevel 1 (
    echo ERROR: Sample build failed.
    exit /b 1
)

if not exist "%SAMPLE_JAR%" (
    echo ERROR: Sample JAR was not produced at %SAMPLE_JAR%
    exit /b 1
)
echo ✓ Sample JAR: %SAMPLE_JAR%

REM ── 5. Done ────────────────────────────────────────────────────────────────
echo.
echo ══════════════════════════════════════════════════════════════════════
echo   Build complete!
echo.
echo   Plugin JAR : %PLUGIN_JAR%
echo   Sample JAR : %SAMPLE_JAR%
echo.
echo   Next steps:
echo     run-demo.bat                   run all demo scenarios
echo     run-demo.bat skew --pause      run one scenario and keep UI open
echo     run-demo.bat history-server    replay past runs in History Server
echo ══════════════════════════════════════════════════════════════════════

endlocal
