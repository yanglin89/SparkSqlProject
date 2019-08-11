package com.run.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 求出每个user 出现的次数
  * 以及每个user 对应的ip数
  */
object UserIpCountTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]")
      .appName("UserIpCountTest")
      .getOrCreate()

    val sc = spark.sparkContext

    val data: RDD[String] = sc.textFile("hdfs://master:9000/spark/test/")
    val lines = data.map(_.split("  "))

    /**
      * 将数据转换成 user为key，value为ip和1，
      * 其中ip是用作set集合求出有多少个ip，1的作用是求出每个user出现的次数求和。
      */
    val userIpOne = lines.map(x => {
      val ip = x(0)
      val user = x(1)
      (user, (ip, 1))
    })

    userIpOne.foreach(println(_))

    /**
      * combineByKey
      * 设定初始值，是对key vaule 中的value起作用到的。
      * 并且后续的两个方法都是针对value 来操作的，key的作用就是by key，然后输出
      */
    val userListIpCount: RDD[(String, (Set[String], Int))] = userIpOne.combineByKey(
      x => (Set(x._1), x._2),
      (a: (Set[String], Int), b: (String, Int)) => {
        (a._1 + b._1, a._2 + b._2)
      },
      (m: (Set[String], Int), n: (Set[String], Int)) => {
        (m._1 ++ n._1, m._2 + n._2)
      })

    userListIpCount.foreach(println(_))

    val result: RDD[String] = userListIpCount.map(x => {
      x._1 + ":userCount:" + x._2._2 + ",ipCount:" + x._2._1.size
    })

    println(result.collect().toBuffer)

  }

}
