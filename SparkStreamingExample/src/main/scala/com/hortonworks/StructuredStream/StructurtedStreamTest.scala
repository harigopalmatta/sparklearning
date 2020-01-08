package com.hortonworks.StructuredStream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}

object StructurtedStreamTest {

    def main(args: Array[String]): Unit = {

      val sparkSession = SparkSession.builder
        .appName("example")
        .getOrCreate()

      val schema = StructType(
        Array(StructField("transactionId", StringType),
          StructField("customerId", StringType),
          StructField("itemId", StringType),
          StructField("amountPaid", DoubleType)))

      //create stream from folder
      val fileStreamDf = sparkSession.readStream
        .option("header", "true")
        .schema(schema)
        .csv("/tmp/input")

      val countDs = fileStreamDf.groupBy("customerId").sum("amountPaid")

      val query =
        countDs.writeStream
          .format("csv")
          .option("checkpointLocation", "hdfs:///tmp/checkpoint")
          .option("path", "hdfs:///user/spark/structuredtest")
          .outputMode(OutputMode.Complete())

      query.start().awaitTermination()
    }
  }
