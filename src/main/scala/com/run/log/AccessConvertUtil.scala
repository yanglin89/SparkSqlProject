package com.run.log

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换 （输入到输出） 工具类
  * */
object AccessConvertUtil {

  // 定义输出的类型
  val structType = StructType(
    Array(
      StructField("url",StringType),
      StructField("cmsType",StringType),
      StructField("cmsId",LongType),
      StructField("traffic",LongType),
      StructField("ip",StringType),
      StructField("city",StringType),
      StructField("time",StringType),
      StructField("hour",StringType)
    )
  )

  /**
    *  根据输入的每一行信息转换为定义的输出的样式
    * */
  def parseLog(log:String) ={

  try {

    val splits = log.split("\t")

    var url = ""
    if(StringUtils.isNotBlank(splits(1)))
      url = splits(1)
    var traffic = 0L
    if(StringUtils.isNotBlank(splits(2)))
      traffic = splits(2).toLong
    var ip = ""
    var city = ""
    if(StringUtils.isNotBlank(splits(3))){
      ip = splits(3)
      city = IpUtils.getCity(ip)
    }

    var cms = ""
    var cmsType = ""
    var cmsId = 0L

    val domain = "topicId"
    if (url.indexOf("?") >= 0) {
      cms = url.substring(url.indexOf("?") + 1)
      if (cms.indexOf(domain) >= 0) {
        cmsType = domain
        val last = cms.substring(cms.indexOf(domain) + domain.length + 1)
        if (last.indexOf("&") >= 0) {
          cmsId = last.substring(0, last.indexOf("&")).toLong
        } else {
          cmsId = last.toLong
        }
      } else {
        cmsType = "--" // 将没有topicId 的分类为 --
      }
    } else {
      cmsType = "--" // 将没有topicId 的分类为 --
    }



    var time = ""
    var hour = ""
    if(StringUtils.isNotBlank(splits(0))){
      time = splits(0)
      hour = time.substring(0, 13).replaceAll("-", "")
    }

    // 得到 Row  row中的字段要和结构体中的字段对应上
    Row(url, cmsType, cmsId.toLong, traffic.toLong, ip, city, time, hour)
  } catch {
    // 异常处理为当出现异常时，不过处理，返回Row(0)
    case e : Exception => Row(0)
  }

  }


}
