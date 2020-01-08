package com.cloudera.examples

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object SparkCldr {

  def main(args: Array[String]): Unit =
  {
    val spark =  SparkSession.builder().appName("SparkHiveIntegration")
      .enableHiveSupport().getOrCreate()

    val logger = LogManager.getRootLogger

    logger.info("creating table test")

    spark.sql("create table test(id int)")

    logger.info("Inserting data to table test")

    spark.sql("insert into table test values(100)")
    spark.sql( sqlText = "insert into table test values(200)")

    logger.info("Projecting data from table test")

    spark.sql("select * from test").show()
  }

}
