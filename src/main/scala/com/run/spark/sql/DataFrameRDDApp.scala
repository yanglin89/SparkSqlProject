package com.run.spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * dataframe 和 rdd 相互操作
  *
  * 使用反射来推断包含了特定数据类型的rdd的元数据
  * 其实就是使用case class
  *
  * */
object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    // 方式一： 反射
//    reflaction(spark)

    //方式二： 编程
    programe(spark)

    spark.stop()

  }


  private def programe(spark: SparkSession) = {
    val rdd = spark.sparkContext.textFile("file:///E:/study_data/infos.txt")

    // 第一步： 生成一个row
    val row_rdd = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))

    // 第二步：定义一个 structType
    val structType = StructType(Array(StructField("id",IntegerType,true),StructField("name",StringType,true),
                      StructField("age",IntegerType,true)))
    // 将上面两步的结果关联，生成df
    val infoDF = spark.createDataFrame(row_rdd,structType)

    // 后续可以使用 dataframe，通过df的方式或者sql 的方式进行操作，进行业务处理

    infoDF.printSchema()
    infoDF.show()

  }

  private def reflaction(spark: SparkSession) = {
    val rdd = spark.sparkContext.textFile("file:///E:/study_data/infos.txt")

    // 将 rdd 转换成 dataframe  （将 行 转换为 列）
    // 注意此处需要引入一个隐式转化  spark.implicits._
    import spark.implicits._
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()

    infoDF.show()

    // 通过 dataframe 的方式进行操作
    infoDF.filter(infoDF.col("age") > 30).show()

    // 通过sqarksql 的方式进行操作
    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 30").show()
  }

  // 类似于java 的实体 bean
  case class Info (val id:Int, val name:String, val age:Int)



}
