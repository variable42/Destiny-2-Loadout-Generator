@ECHO OFF

IF EXIST Warlock (
    RMDIR /S /Q Warlock
)

spark-shell -i generate_loadouts.scala --conf spark.driver.args="Warlock 21"