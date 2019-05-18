package com.run.log

import java.sql.{Connection, DriverManager, PreparedStatement}


/**
  * mysql 工具类
  * */
object MysqlUtils {

  /**
    * 获取 mysql 数据库连接
    * */
  def getConnection(): Connection ={
    DriverManager.getConnection("jdbc:mysql://master:3306/sparksql_project?user=hadoop&password=mdhc5891")
  }

  /**
    * 释放数据库连接等资源
    * */
  def release(conn:Connection,pstmt:PreparedStatement): Unit ={
    try {
      if (pstmt != null){
        pstmt.close()
      }
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if (conn != null){
        conn.close()
      }
    }
  }


  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

}
