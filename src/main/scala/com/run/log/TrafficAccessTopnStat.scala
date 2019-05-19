package com.run.log

/**
  * topicid 次数统计 实体类
  * @param hour
  * @param cmsId
  * @param traffics
  */
case class TrafficAccessTopnStat(hour:String,cmsId:Long,traffics:Long)
