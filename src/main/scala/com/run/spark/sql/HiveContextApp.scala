package com.run.spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * hivecontext 使用
  * */
object HiveContextApp {

  def main(args: Array[String]): Unit = {


    // 步骤共有三步
    //1、创建相应的context
    val sparkConf = new SparkConf()
    // 在生产和测试环境中，appname和master是通过脚本的形式指定的
    //    sparkConf.setAppName("AppcationName").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //2、业务逻辑处理 :处理json文件
    hiveContext.sql("use sparksqltest")
    hiveContext.table("test_database_table").show()

    //3、释放资源
    sc.stop()

  }

}
