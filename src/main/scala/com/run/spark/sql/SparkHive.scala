package com.run.spark.sql

import org.apache.spark.sql.SparkSession

object SparkHive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]")
      .enableHiveSupport().getOrCreate()

    // 切换数据库 默认为default
    spark.sql("use default")

    //查询数据库中的表
    spark.sql("show tables").show

    //查询数据库中表的数据
    spark.table("hive_table").show

    // 将查询结果处理后生成表
    spark.sql("select id,count(1) as count from hello_hive group by id")
      .filter("id > 1").write.saveAsTable("hive_table")

    spark.stop()

  }

}
