package com.run.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * parquet 文件操作
  * */
object ParquetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()

    // spark 默认处理的类型为 parquet
    val userPQ = spark.read.format("parquet").load("file:///E:/study_data/users.parquet")

    spark.read.format("parquet").option("path","file:///E:/study_data/users.parquet").load().show()

    userPQ.printSchema()
    userPQ.show()

    //生成文件
//    userPQ.select("name","favorite_color").write.format("json").save("file:///E:/study_data/usersout.json")

    spark.stop()

  }


}
