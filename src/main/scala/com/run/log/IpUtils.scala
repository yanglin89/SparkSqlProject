package com.run.log

import com.ggstar.util.ip.IpHelper

/**
  * ip 解析工具类
  * */
object IpUtils {

  def getCity(ip:String):String={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    val ip = "15.118.4.192"
    println(getCity(ip))
  }

}
