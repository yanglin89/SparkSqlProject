package com.run.log

import org.apache.spark.sql.SparkSession

/**
  * 使用 spark 完成数据清洗操作
  * */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///E:/study_data/output/*")

//    accessRDD.take(10).foreach(println)
//    println(accessRDD.count())

    // RDD => dataframe  rdd 转换为 dataframe
    val accessDF = spark.createDataFrame(accessRDD.map(line => AccessConvertUtil.parseLog(line)),AccessConvertUtil.structType)

    accessDF.printSchema()
    accessDF.show(30,true)

    spark.stop()

  }


}
