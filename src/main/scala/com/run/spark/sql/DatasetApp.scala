package com.run.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * dataset 基本使用
  * */
object DatasetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()

    // spark 如何解析 csv
    // csv 文件头部有属性标识
    val path = "file:///E:/study_data/sales.csv"
    val salas = spark.read.option("header","true").option("inferSchema","true").csv(path)

    salas.show()

    // 将dataframe 转为 dataset 通过 as[]
//    val dataset = salas.as[Salas]
//    dataset.map(line => line.b).show()


    spark.stop()

  }

  case class Salas(a:Int,b:Int,c:Int,d:Double)

}
