@ECHO OFF

IF EXIST Titan (
    RMDIR /S /Q Titan
)

spark-shell -i generate_loadouts.scala --conf spark.driver.args="Titan 19"