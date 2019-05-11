package com.run.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * sparksession 的使用
  * */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {

    val path = args(0)
//    val warehouseLocation = args(1)

    val spark = SparkSession.builder().appName("SparkSessionApp")
      .master("local")
//      .config("spark.sql.warehouse.dir", "/opt/shell/spark/spark-warehouse")
//      .enableHiveSupport()
      .getOrCreate()

    val people = spark.read.json(path)
    people.show()

    spark.stop()

  }

}
