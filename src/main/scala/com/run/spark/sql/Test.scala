package com.run.spark.sql

import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test").master("local[2]").getOrCreate()

    val sc = spark.sparkContext
    val b = spark.sparkContext.parallelize(Array(("A","a2"),("B","b1"),("C","c1"),("D","d1"),("E","e1"),("F","f1")))
    val a = sc.parallelize(Array(("A","a1"),("B","b1"),("C","c1"),("D","d1"),("E","e1"),("F","f1")))

    b.join(a).collect()

    spark.stop()
  }

}
