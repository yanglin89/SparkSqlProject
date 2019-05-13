package com.run.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * dataframe api 基本操作
  * */
object DataFrameApp {

  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder().appName("DataFrameApp")
      .master("local[2]")
      .getOrCreate()

    // 将一个json文件加载成dataframe文件
    val people = spark.read.format("json").load("file:///E:/study_data/people.json")

    //输出schema
    people.printSchema()

    //输出结果集，默认20条，show方法
    people.show(2)

    // 查询某一列特定的数据
    people.select("name").show()

    //查询某几列数据，并对结果做操作
    people.select(people.col("name"),(people.col("age")+10).as("age2")).show()

    // 根据某一列的值进行过滤
    people.filter(people.col("age") > 19).show()

    // 按照某一列进行分组，聚合
    people.groupBy("age").count().show()


    spark.stop()

  }


}