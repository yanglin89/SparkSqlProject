package com.run.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * topn 统计 作业
  * */
object TopNStatJobReusable {

  def main(args: Array[String]): Unit = {

    // spark.sql.sources.partitionColumnTypeInference.enable 分区字段类型的智能识别是否开区，默认开启 true
    val spark = SparkSession.builder().appName("TopNStatJob").master("local[2]")
      .config("spark.sql.sources.partitionColumnTypeInference.enable","false").getOrCreate()

    // 注意 load 的path 中，要是最后的路径后面加 * 如 XXX/clean/*，则加载到的数据没有分区信息，我们把* 去掉，则打印schema 存在分区信息
    val accessDF = spark.read.format("parquet").load("E:/study_data/clean/")

//    accessDF.printSchema()
//    accessDF.show(50,false)

    // 首先删除数据库已有数据 , 按照分区
    val partitions = ArrayBuffer[String]()
    val day = "20130721-"
    for(i <- 0 to 23){
      var complete = ""
      if (i < 10){
        complete = day + "0" + i
      }else{
        complete = day + i
      }

      partitions.append(complete)
    }

    for (partiton <- partitions){
      StatDao.deletePartition(partiton)
    }

  // 复用 commonDF ，将 commonDF 放置在 缓存 cache 中
    import spark.implicits._
    val commonDF = accessDF.filter($"hour".startsWith(day)  && $"cmsType" === "topicId")
    commonDF.cache()

    // 按照 topicId 统计 topn
    topNStatCountByDF(spark,commonDF)
//    topNStatCountBySql(spark,accessDF)

    // 按照地市信息统计 topn
    cityTopnStat(spark,commonDF)

    // 按照流量进行统计 topn
    trafficTopnStat(spark,commonDF)

    // 将缓存中的 commonDF 释放
    commonDF.unpersist(true)
    spark.stop()
  }


  /**
    * topn 的 流量 traffic 统计，并 过滤掉 流量为 0 的
    * 通过 dataframe 的方式
    * */
  def trafficTopnStat(spark: SparkSession, commonDF: DataFrame) = {
    import spark.implicits._

    val trafficTopn = commonDF
      .groupBy("hour","cmsId")
      .agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)
      .filter("traffics > 0")
//      .show()


    // 窗口 Windows 函数在 sparksql 中的使用
    // 将统计结果写入到 mysql数据库
    try{
      // 循环所有的分区
      trafficTopn.foreachPartition(partitonOfAccess => {
        val list = new ListBuffer[TrafficAccessTopnStat]

        // 循环每一个分区的数据
        partitonOfAccess.foreach(info => {
          val hour = info.getAs[String]("hour")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")

          list.append(TrafficAccessTopnStat(hour,cmsId,traffics))
        })

        StatDao.insertTrafficAccessStatTopn(list)

      })
    }catch {
      case e:Exception => e.printStackTrace()
    }


  }


  /**
    * topn 的 city 统计
    * 通过 dataframe 的方式
    * */
  def cityTopnStat(spark: SparkSession, commonDF: DataFrame) = {
    import spark.implicits._

    val cityTopn = commonDF
      .groupBy("hour","cmsId","city")
      .agg(count("cmsId").as("times"))
      .orderBy($"times".desc)

//    println(cityTopn.count())

    // 窗口 Windows 函数在 sparksql 中的使用
    // 每个地市做一次 window 函数，按照 times 降序 即为： 分别统计每一个地市的topicId 的top3
    val topnDF = cityTopn.select(cityTopn.col("hour"),cityTopn.col("cmsId"),
      cityTopn.col("city"),cityTopn.col("times"),
      row_number().over(
        Window.partitionBy(cityTopn.col("city"))
        .orderBy(cityTopn.col("times").desc))
        .as("times_rank"))
      .filter("times_rank <= 3")
//      .show()

    // 将统计结果写入到 mysql数据库
    try{
      // 循环所有的分区
      topnDF.foreachPartition(partitonOfAccess => {
        val list = new ListBuffer[CityAccessTopnStat]

        // 循环每一个分区的数据
        partitonOfAccess.foreach(info => {
          val hour = info.getAs[String]("hour")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val rank = info.getAs[Int]("times_rank")

          list.append(CityAccessTopnStat(hour,cmsId,city,times,rank))
        })

        StatDao.insertCityAccessStatTopn(list)

      })
    }catch {
      case e:Exception => e.printStackTrace()
    }


  }


  /**
    * topn 的topicid 统计
    * 通过 dataframe 的方式
    * */
  def topNStatCountByDF(spark: SparkSession, commonDF: DataFrame) = {

    import spark.implicits._


    val topnDF = commonDF
      .groupBy("hour","cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    topnDF.show(false)

    // 将统计结果写入到 mysql数据库
    try{
      // 循环所有的分区
        topnDF.foreachPartition(partitonOfAccess => {
          val list = new ListBuffer[HourAccessTopnStat]

          // 循环每一个分区的数据
          partitonOfAccess.foreach(info => {
            val hour = info.getAs[String]("hour")
            val cmsId = info.getAs[Long]("cmsId")
            val times = info.getAs[Long]("times")

            list.append(HourAccessTopnStat(hour,cmsId,times))
          })

          StatDao.insertAccessStatTopn(list)

        })
    }catch {
      case e:Exception => e.printStackTrace()
    }

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

    // 将结果添加到mysql 数据库
    try{

      topnDF.foreachPartition(partitions =>{
        val list = new ListBuffer[HourAccessTopnStat]

        partitions.foreach(info => {
          val hour = info.getAs[String]("hour")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(HourAccessTopnStat(hour,cmsId,times))
        })

        StatDao.insertAccessStatTopn(list)

      })

    }catch {
      case e: Exception => e.printStackTrace()
    }


  }

}














