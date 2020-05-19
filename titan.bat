@ECHO OFF

IF EXIST Titan (
    RMDIR /S /Q Titan
)

set JAVA_HOME="C:\Progra~1\Java\jdk1.8.0_231"

set PATH=%_JAVA_BIN%;%PATH%
set _JAVA_OPTIONS="-Xmx24g"

set /p script="Enter script: "
time /t

spark-shell -i generate_loadouts-%script%.scala --driver-memory 24G --conf spark.driver.args="Titan 19"