package com.run.spark.sql

import com.run.spark.sql.DataFrameRDDApp.programe
import org.apache.spark.sql.SparkSession

/**
  * dataframe 案例
  * */
object DataFrameCase {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    // 方式一： 反射
    val rdd = spark.sparkContext.textFile("file:///E:/study_data/student.data")

    import spark.implicits._
    val stuDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt,line(1),line(2),line(3))).toDF()

    //共显示30条，超过一定长度显示不截取
    stuDF.show(30,false)

    stuDF.take(5).foreach(println)
    println(stuDF.first())
    stuDF.head(3).foreach(println)

    stuDF.filter("name = ''").show()
    stuDF.filter("name = '' OR name = 'NULL'").show(false)

    // 支持内置函数 ，拿到名字以M开头的
    stuDF.filter("SUBSTR(NAME,0,1) = 'M'").show()

    // 排序
    stuDF.sort(stuDF.col("name").desc).show()
    stuDF.sort("id","name").show()
    stuDF.sort(stuDF.col("name").asc,stuDF.col("id").desc).show()

    stuDF.select(stuDF.col("name").as("student_name")).show(2)


    // join 操作
    // 等于判断用 三个 ===
    // 默认采用 inner join
    val stuDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt,line(1),line(2),line(3))).toDF()

    stuDF.join(stuDF2,stuDF.col("id") === stuDF2.col("id")).show()
    stuDF.join(stuDF2,stuDF.col("id") === stuDF2.col("id"),"left").show()

    spark.stop()
  }

  case class Student(id:Int,name:String,phone:String,email:String)

}
