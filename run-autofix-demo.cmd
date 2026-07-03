@echo off
REM ===========================================================================
REM  Run the sparkx Auto-Fix demo (SparkXAutoFixDemo) on Windows / JDK 17.
REM
REM  Usage:
REM     run-autofix-demo.cmd [broadcast^|partitioning^|all] [--pause]
REM
REM  Examples:
REM     run-autofix-demo.cmd                 (runs "all")
REM     run-autofix-demo.cmd broadcast
REM     run-autofix-demo.cmd all --pause     (pause between runs to inspect UI)
REM
REM  Notes:
REM   * Spark is a "provided" dependency, so we use sbt's Test classpath which
REM     includes Spark. No spark-submit required.
REM   * JDK 17 needs the --add-opens flags below or Spark aborts on startup.
REM   * The classpath (~15 KB) is passed through a Java argument file, because
REM     it is far too long for both cmd.exe (8 KB command line) and "for /f"
REM     (8 KB per line). The JVM reads @argfile directly with no size limit.
REM ===========================================================================
setlocal

REM Run from the repo root (the directory this script lives in).
cd /d "%~dp0"

set "CPFILE=%TEMP%\sparkx-cp-%RANDOM%.txt"
set "ARGFILE=%TEMP%\sparkx-args-%RANDOM%.txt"

REM --- Resolve the sample Test classpath from sbt (includes provided Spark) ---
echo Resolving classpath via sbt (first run may take a minute)...
call sbt --error "export sample/Test/fullClasspath" 2>nul 1>"%CPFILE%"

REM Fail early if sbt produced no classpath.
for %%A in ("%CPFILE%") do if %%~zA LSS 10 (
  echo.
  echo ERROR: could not resolve the classpath. Is sbt installed and on PATH?
  echo        Try running:  sbt "sample/compile"
  del "%CPFILE%" 2>nul
  exit /b 1
)

REM Build a Java argument file: "-cp" on its own line, classpath on the next.
echo -cp> "%ARGFILE%"
type "%CPFILE%" >> "%ARGFILE%"

REM --- JDK 17 module opens required by Spark 3.5 ---
set "OPENS=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"

echo.
echo Launching SparkXAutoFixDemo %*
echo (Open the Auto-Fix tab at http://localhost:4040/sparkx/autofix while it runs)
echo.

java @"%ARGFILE%" %OPENS% com.sparkx.sample.SparkXAutoFixDemo %*
set "RC=%ERRORLEVEL%"

del "%CPFILE%" "%ARGFILE%" 2>nul
endlocal & exit /b %RC%
