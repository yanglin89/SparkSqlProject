package com.run.spark.sql

import java.sql.DriverManager

/**
  * thrift server beeline 使用
  * */
object SparlSqlThriftServerApp {

  def main(args: Array[String]): Unit = {

    // 首先加载驱动
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    // 创建连接,进行数据库操作
    // 此处的url是beeline连接时的url，user和password同理
    val conn = DriverManager.getConnection("jdbc:hive2://master:10000","hadoop","")
    // 切换操作的数据库，使用 execute 方法 ，查询 sql 语句使用 executeQuery 方法
    conn.prepareStatement("use sparksqltest").execute()
    val pstmt = conn.prepareStatement("select id,name from test_database_table")
    val rs = pstmt.executeQuery()
    while (rs.next()){
      println("id: " + rs.getInt("id") + " name: " + rs.getString("name"))
    }

    rs.close()
    pstmt.close()
    conn.close()

  }

}
