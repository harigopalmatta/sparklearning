package com.hortonworks
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object SparkHive {

  def main(args: Array[String]): Unit =
  {
    val spark =  SparkSession.builder().appName("SparkHiveTest")
    .enableHiveSupport().getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://HA2")

    val logger = LogManager.getRootLogger

    logger.info("project remote table")

    spark.sql("select * from test.remote_tbl").show

    logger.info("Inserting data to remote table")

    spark.sql("insert into table test.remote_tbl values(100,'row1')")

    logger.info("Projecting data from remote table ")

    spark.sql("select * from test.remote_tbl").show()

    spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://HA1")

    spark.sql(" insert into table test.local_tbl values(100,'row1')").show()


  }

}
