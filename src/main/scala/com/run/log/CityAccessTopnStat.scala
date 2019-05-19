package com.run.log

/**
  * 每个地市按照 topicid 统计 topn
  */
case class CityAccessTopnStat(hour:String,cmsId:Long,city:String,times:Long,time_rank:Int)
