# Spark submit command in yarn client mode

**spark-submit --master yarn --deploy-mode client --class com.hortonworks.SparkHive --name SparkHiveTest --num-executors 2 --executor-memory 512m SparkHiveTest-1.0-SNAPSHOT.jar**


# Spark submit command in yarn cluster mode

**spark-submit --master yarn --deploy-mode cluster --class com.hortonworks.SparkHive --name SparkHiveTest --num-executors 2 --executor-memory 512m  --files /etc/spark2/conf/hive-site.xml SparkHiveTest-1.0-SNAPSHOT.jar**
