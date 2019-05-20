package com.run.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用 spark 完成数据清洗操作 ,提到作业到 yarn 上运行
  * */
object SparkStatCleanJobYarn {

  def main(args: Array[String]): Unit = {

    if(args.length < 2){
      println("Usage : SparkStatCleanJob <InputPath> <OutputPath>")
      System.exit(1)
    }

    val Array(inputPath,outputPath) = args

    val spark = SparkSession.builder().getOrCreate()

    val accessRDD = spark.sparkContext.textFile(inputPath)

//    accessRDD.take(10).foreach(println)
//    println(accessRDD.count())

    // RDD => dataframe  rdd 转换为 dataframe
    val accessDF = spark.createDataFrame(accessRDD.map(line => AccessConvertUtil.parseLog(line)),AccessConvertUtil.structType)

    accessDF.printSchema()
    accessDF.show(30,true)

    // 将结果集按照 hour 分区 ，并且每个分区设定一个输出文件，已覆盖的方式生成 parquet 文件
    accessDF.coalesce(1).write.format("parquet")
      .mode(SaveMode.Overwrite).partitionBy("hour").save(outputPath)

    spark.stop()

  }


}
