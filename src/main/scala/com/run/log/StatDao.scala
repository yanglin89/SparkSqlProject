package com.run.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * 统计 dao
  */
object StatDao {


  /**
    * 批量删除 数据库表数据
    */
  def deletePartition(partition:String): Unit ={

    val tables = Array("city_hour_access_topn_stat","hour_access_topn_stat","traffic_hour_access_topn_stat")

    var connection:Connection = null
    var pstat:PreparedStatement = null

    try{

      connection = MysqlUtils.getConnection()

      for(table <- tables){

        // sql 前面一个 s ，然后在变量前面 $
        val sql = s"delete from $table where hour = ?"

        pstat = connection.prepareStatement(sql)
        pstat.setString(1,partition)

        pstat.executeUpdate()

      }

      println("data delete success")

    }catch {
      case e : Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection,pstat)
    }

  }


  /**
    * 批量保存 HourAccessTopnStat 到 mysql 数据库
    * @param list
    */
  def insertAccessStatTopn(list: ListBuffer[HourAccessTopnStat]): Unit ={

    var connection:Connection = null
    var pstat:PreparedStatement = null

    try{

      connection = MysqlUtils.getConnection()

      // 取消自动提交
      connection.setAutoCommit(false)

      val sql = "insert into hour_access_topn_stat(hour,cms_id,times) values(?,?,?)"

      pstat = connection.prepareStatement(sql)

      for(ele <- list){
        pstat.setString(1,ele.hour)
        pstat.setLong(2,ele.cmsId)
        pstat.setLong(3,ele.times)

        // 将pstat 添加到批处理中
        pstat.addBatch()
      }

      // 批处理执行
      pstat.executeBatch()
      // 提交数据库
      connection.commit()
      println("data insert success")

    }catch {
      case e : Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection,pstat)
    }

  }


  /**
    * 批量保存 HourAccessTopnStat 到 mysql 数据库
    * @param list
    */
  def insertCityAccessStatTopn(list: ListBuffer[CityAccessTopnStat]): Unit ={

    var connection:Connection = null
    var pstat:PreparedStatement = null

    try{

      connection = MysqlUtils.getConnection()

      // 取消自动提交
      connection.setAutoCommit(false)

      val sql = "insert into city_hour_access_topn_stat(hour,cms_id,city,times,time_rank) values(?,?,?,?,?)"

      pstat = connection.prepareStatement(sql)

      for(ele <- list){
        pstat.setString(1,ele.hour)
        pstat.setLong(2,ele.cmsId)
        pstat.setString(3,ele.city)
        pstat.setLong(4,ele.times)
        pstat.setInt(5,ele.time_rank)

        // 将pstat 添加到批处理中
        pstat.addBatch()
      }

      // 批处理执行
      pstat.executeBatch()
      // 提交数据库
      connection.commit()
      println("data insert success")

    }catch {
      case e : Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection,pstat)
    }

  }


  /**
    * 批量保存 TrafficAccessTopnStat 到 mysql 数据库
    * @param list
    */
  def insertTrafficAccessStatTopn(list: ListBuffer[TrafficAccessTopnStat]): Unit ={

    var connection:Connection = null
    var pstat:PreparedStatement = null

    try{

      connection = MysqlUtils.getConnection()

      // 取消自动提交
      connection.setAutoCommit(false)

      val sql = "insert into traffic_hour_access_topn_stat(hour,cms_id,traffics) values(?,?,?)"

      pstat = connection.prepareStatement(sql)

      for(ele <- list){
        pstat.setString(1,ele.hour)
        pstat.setLong(2,ele.cmsId)
        pstat.setLong(3,ele.traffics)

        // 将pstat 添加到批处理中
        pstat.addBatch()
      }

      // 批处理执行
      pstat.executeBatch()
      // 提交数据库
      connection.commit()
      println("data insert success")

    }catch {
      case e : Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection,pstat)
    }

  }


}



















