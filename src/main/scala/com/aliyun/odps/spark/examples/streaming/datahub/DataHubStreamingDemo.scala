package com.aliyun.odps.spark.examples.streaming.datahub

import java.sql.Timestamp

import com.aliyun.datahub.model.RecordEntry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.datahub.DatahubUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.tools.ant.util.DateUtils

object DataHubStreamingDemo {

  def transferFunc(record: RecordEntry): String = {
    // 这个转化函数目前只支持把DataHub Record转成String
    // 如果是需要多个字段的话, 那么需要处理一下拼接的逻辑
    record.getString(1)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("DataHubStreamingDemo")
//      .config("spark.hadoop.fs.oss.credentials.provider", "org.apache.hadoop.fs.aliyun.oss.AliyunStsTokenCredentialsProvider")
//      .config("spark.hadoop.fs.oss.ststoken.roleArn", "acs:ram::****:role/aliyunodpsdefaultrole")
//      .config("spark.hadoop.fs.oss.endpoint", "oss-cn-hangzhou-zmf.aliyuncs.com")
      .getOrCreate()


    // 设置Batch间隔时间
    val ssc = new StreamingContext(spark.sparkContext, Seconds(30))

    // checkpoint dir to oss
    //ssc.checkpoint("oss://bucket/inputdata/")
    ssc.checkpoint("./checkpoint")

    val dataStream = DatahubUtils.createStream(
      ssc,
      "zj_binlog",
      "zj_binlog",
      "****",//在datahub配置页新增订阅
      "****",
      "****",
      "https://dh-cn-shenzhen.aliyuncs.com", //在datahub项目首页常用信息
      transferFunc(_),
      StorageLevel.MEMORY_AND_DISK
    )

    /**
      * datahub streaming任务目前可以在自建spark上跑，暂不支持maxcompute平台
      */

    //dataStream.count().print()
    dataStream.count().foreachRDD((rdd,time)=>{
      println(DateUtils.format(new Timestamp(time.milliseconds),DateUtils.ISO8601_DATETIME_PATTERN))
      rdd.checkpoint() //该操作属于懒加载,相当于transform，触发action后才会checkpoint
      println(rdd.take(1)(0))
      println(rdd.isCheckpointed)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

//spark-submit --master local[6] --class com.aliyun.odps.spark.examples.streaming.datahub.DataHubStreamingDemo spark-examples_2.11-1.0.0-SNAPSHOT-shaded.jar