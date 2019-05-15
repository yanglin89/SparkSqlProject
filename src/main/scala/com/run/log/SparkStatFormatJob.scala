package com.run.log

import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗 ： 抽取出我们所需要的列
  * */
object SparkStatFormatJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("file:///E:/study_data/trackinfo_20130721.data")
//    access.take(10).foreach(println)

    // {} 是一个匿名函数  最后一行默认为返回值
    access.map(line => {
      val splits = line.split("\001")
      val ip = splits(13)
      val time = splits(17)
      val url = splits(1)
      val traffic = splits(11) //消耗流量

      /**
        * 将 括号内的内容作为一条记录原子返回
        * */
      (time + "\t" + url + "\t" + traffic + "\t" + ip)
    }).saveAsTextFile("file:///E:/study_data/output/")



    spark.stop()

  }


}
