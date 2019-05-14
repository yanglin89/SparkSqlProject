package com.run.spark.sql

import org.apache.spark.sql.SparkSession

object SparkMysql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()

    // 连接数据库 方式一
    val jdbcDF = spark.read.format("jdbc").option("url","jdbc:mysql://master:3306/sparksql").option("dbtable","sparksql.TBLS").option("user","hadoop").option("password","mdhc5891").option("driver","com.mysql.jdbc.Driver").load()


    jdbcDF.printSchema()
    jdbcDF.show()

    // 向 mysql 写入数据
    jdbcDF.select("TBL_ID").write.format("jdbc").option("url","jdbc:mysql://master:3306/sparksql").option("dbtable","sparksql.TBLS_TEST").option("user","hadoop").option("password","mdhc5891").option("driver","com.mysql.jdbc.Driver").save()

    // 连接数据库方式二
    import java.util.Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user","hadoop")
    connectionProperties.put("password","mdhc5891")
    connectionProperties.put("driver","com.mysql.jdbc.Driver")

    val jdbcDF2 = spark.read.jdbc("jdbc:mysql://master:3306/sparksql","TBLS",connectionProperties)
    jdbcDF2.printSchema()
    jdbcDF2.show()

    // 向 mysql 写入数据
    jdbcDF2.select("TBL_ID").write.jdbc("jdbc:mysql://master:3306/sparksql","TBLS_test2",connectionProperties)

    spark.stop()

  }


}
