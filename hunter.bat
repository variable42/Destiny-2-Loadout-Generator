@ECHO OFF

IF EXIST Hunter (
    RMDIR /S /Q Hunter
)

spark-shell -i generate_loadouts.scala --conf spark.driver.args="Hunter 22"