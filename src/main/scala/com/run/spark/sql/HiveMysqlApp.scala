package com.run.spark.sql

import java.io.File

import org.apache.spark.sql.SparkSession

/**
  * 使用外部数据源综合查询 hive 和 mysql
  *
  * */
object HiveMysqlApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    // 加载 hive 表
    spark.sql("use sparksqltest")
    spark.sql("show tables").show()
    val hiveDF = spark.table("hello_hive")

    //加载 mysql 数据库表
    val mysqlDF = spark.read.format("jdbc").option("url","jdbc:mysql://master:3306/sparksql").option("dbtable","dept").option("user","hadoop").option("password","mdhc5891").option("driver","com.mysql.jdbc.Driver").load()

    // join 操作
    val resultDF = hiveDF.join(mysqlDF,hiveDF.col("id") === mysqlDF.col("deptno"))
    resultDF.show()

    resultDF.select(mysqlDF.col("deptno"),hiveDF.col("id")).show()

    spark.stop()

  }

}
