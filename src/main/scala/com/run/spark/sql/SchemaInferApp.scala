package com.run.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * schema infer
  * */
object SchemaInferApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("schemaInfer").master("local[2]").getOrCreate()

    val df = spark.read.format("json").load("file:///E:/study_data/schmea.json")

    df.printSchema()

    df.show()

    spark.stop()

  }


}
