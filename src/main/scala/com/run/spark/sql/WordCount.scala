package com.run.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * 读取hdfs数据，统计词频
  * 小写输出
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]")
      .appName("WordCount").getOrCreate()
    val sc = spark.sparkContext

    val fileRDD = sc.textFile("hdfs://master:9000/user/root/input/README.txt")

    val result = fileRDD.flatMap(_.split("\\|")).map(x => (x.toLowerCase,1)).reduceByKey(_+_)

    result.foreach(println)

  }
}
