package com.run.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * spqrk sqlContext 的使用
  * */
object SqlContextApp {

  def main(args: Array[String]): Unit = {

    val path = args(0)

    // 步骤共有三步
    //1、创建相应的context
    val sparkConf = new SparkConf()
    // 在生产和测试环境中，appname和master是通过脚本的形式指定的
//    sparkConf.setAppName("AppcationName").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //2、业务逻辑处理 :处理json文件
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    //3、释放资源
    sc.stop()

  }


}
