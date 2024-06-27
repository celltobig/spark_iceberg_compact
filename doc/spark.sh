#!/bin/bash
# spark-sql-compact
# dl_ods.ods_iceberg_cowell_oms_order_base_stream_v2
appName=spark-iceberg-rewrite_3.4.3
mysqlUrl="jdbc:mysql://xxxxxxx:3306/flink_web1?characterEncoding=utf8&useSSL=false"
mysqlUser=root
mysqlPassword="xxxxxx"
mysqlDbTable=f_iceberg_table
mysqlSql="select * from f_iceberg_table where id=40"
expireFiles=yes
removeFiles=no
rewritePosDelFiles=yes
rewriteFiles=yes
rewriteManifests=yes
useCache=true
sleepTime=300000

export SPARK_HOME=/data/appdata/spark-3.4.3-bin-hadoop3
spark-submit \
 --conf spark.sql.debug.maxToStringFields=1000 \
 --conf spark.rpc.message.maxSize=1024 \
 --conf spark.sql.legacy.timeParserPolicy=LEGACY \
 --conf spark.sql.storeAssignmentPolicy=ANSI \
 --conf spark.driver.maxResultSize=4G \
 --master yarn \
 --deploy-mode cluster \
 --class task.SparkSqlSumit \
 --driver-memory 4G \
 --executor-memory 8G \
 --executor-cores 1 \
 --num-executors 1 \
 --name ${appName} \
 --queue root.KYLIN \
 hdfs:///BDP/spark/spark-iceberg-rewrite_3.4.3.jar appName="${appName}"  mysqlUrl="${mysqlUrl}" mysqlUser="${mysqlUser}" mysqlPassword="${mysqlPassword}" mysqlDbTable="${mysqlDbTable}" mysqlSql="${mysqlSql}" expireFiles="${expireFiles}" removeFiles="${removeFiles}" rewritePosDelFiles="${rewritePosDelFiles}" rewriteFiles="${rewriteFiles}" rewriteManifests="${rewriteManifests}" useCache="${useCache}" sleepTime="${sleepTime}"

echo "Good bye!"
exit 0
