@ECHO OFF

IF EXIST Warlock (
    RMDIR /S /Q Warlock
)

set JAVA_HOME="C:\Progra~1\Java\jdk1.8.0_231"

set PATH=%_JAVA_BIN%;%PATH%
set _JAVA_OPTIONS="-Xmx24g"

set /p script="Enter script: "
time /t

REM Mine = 21, Else = 15
spark-shell -i generate_loadouts-%script%.scala --driver-memory 24G --conf spark.driver.args="Warlock 21"