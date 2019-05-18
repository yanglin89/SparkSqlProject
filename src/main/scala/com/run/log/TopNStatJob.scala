package com.run.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * topn 统计 作业
  * */
object TopNStatJob {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TopNStatJob").master("local[2]")
      .config("spark.sql.sources.partitionColumnTypeInference.enable","false").getOrCreate()

    // 注意 load 的path 中，要是最后的路径后面加 * 如 XXX/clean/*，则加载到的数据没有分区信息，我们把* 去掉，则打印schema 存在分区信息
    val accessDF = spark.read.format("parquet").load("E:/study_data/clean/")

//    accessDF.printSchema()
//    accessDF.show(50,false)

    topNStatCountByDF(spark,accessDF)
//    topNStatCountBySql(spark,accessDF)

    spark.stop()
  }

  /**
    * topn 的topicid 统计
    * 通过 dataframe 的方式
    * */
  def topNStatCountByDF(spark: SparkSession, accessDF: DataFrame) = {

    import spark.implicits._


    val topnDF = accessDF.filter($"hour".startsWith("20130721-")  && $"cmsType" === "topicId")
      .groupBy("hour","cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    topnDF.show(false)

  }

  /**
    * topn 的topicid 统计
    * 通过 sql 的方式
    * */
  def topNStatCountBySql(spark: SparkSession, accessDF: DataFrame) = {

    accessDF.createOrReplaceTempView("access_clean")
    val topnDF = spark.sql("select hour,cmsId,count(1) as times from access_clean " +
          "where hour like '20130721-%' and cmsType = 'topicId' " +
          "group by hour,cmsId order by times desc")

    topnDF.show(false)

  }

}














