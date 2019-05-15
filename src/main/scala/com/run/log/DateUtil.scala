package com.run.log

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期转换类
  *
  * */
object DateUtil {

  /**
    * 注意 SimpleDateFormat 是线程不安全的
    * 公用一个可能在大批量执行时导致解析日期错误
    * */

  // 原始时间 ：   10/Nov/2016:00:01:02 +0800
  val ORIG_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
//  val ORIG_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)

  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
//  val TARGET_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def parse(time:String): String ={
    TARGET_FORMAT.format(new Date(getTimeLong(time)))
  }

  def getTimeLong(time:String): Long ={
    try{
      ORIG_FORMAT.parse(time).getTime
    }catch {
      // catch 捕获到异常时直接返回 0L
      case e : Exception => {
        0L
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(parse("10/Nov/2016:00:01:02 +0800"))
  }

}
