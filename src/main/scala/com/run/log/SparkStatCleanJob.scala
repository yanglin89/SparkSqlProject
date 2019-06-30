package com.run.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用 spark 完成数据清洗操作
  * */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]")
      //.config("spark.sql.parquet.compression.codec","gzip")
      .getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///E:/study_data/output/*")

//    accessRDD.take(10).foreach(println)
//    println(accessRDD.count())

    // RDD => dataframe  rdd 转换为 dataframe
    val accessDF = spark.createDataFrame(
      accessRDD.map(
        line => AccessConvertUtil.parseLog(line)),AccessConvertUtil.structType)

    accessDF.printSchema()
    accessDF.show(30,true)

    // 将结果集按照 hour 分区 ，并且每个分区设定一个输出文件，已覆盖的方式生成 parquet 文件
    // coalesce 方法返回一个DataSet[T]
    accessDF.coalesce(1).write.format("parquet")
      .mode(SaveMode.Overwrite).partitionBy("hour").save("E:/study_data/clean/")

    spark.stop()

  }


}
