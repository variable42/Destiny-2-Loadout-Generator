@ECHO OFF

IF EXIST Hunter (
    RMDIR /S /Q Hunter
)

set JAVA_HOME="C:\Progra~1\Java\jdk1.8.0_231"

set PATH=%_JAVA_BIN%;%PATH%
set _JAVA_OPTIONS="-Xmx24g"

set /p script="Enter script: "
time /t

REM 23
spark-shell -i generate_loadouts-%script%.scala --driver-memory 24G --conf spark.driver.args="Hunter 22"