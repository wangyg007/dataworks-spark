package com.aliyun.odps.spark.examples.streaming.datahub

import com.aliyun.datahub.model.RecordEntry
import com.aliyun.odps.spark.examples.streaming.common.SparkSessionSingleton
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.datahub.DatahubUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DataHub2OdpsDemo {

  def transferFunc(record: RecordEntry): String = {
    // 这个转化函数目前只支持把DataHub Record转成String
    // 如果是需要多个字段的话, 那么需要处理一下拼接的逻辑
    record.getString(1)
  }

  def main(args: Array[String]): Unit = {

    //Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("DataHubStreamingDemo")
//      .config("spark.hadoop.fs.oss.credentials.provider", "org.apache.hadoop.fs.aliyun.oss.AliyunStsTokenCredentialsProvider")
//      .config("spark.hadoop.fs.oss.ststoken.roleArn", "acs:ram::****:role/aliyunodpsdefaultrole")
//      .config("spark.hadoop.fs.oss.endpoint", "oss-cn-hangzhou-zmf.aliyuncs.com")
      .getOrCreate()

    // 设置Batch间隔时间
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    // checkpoint dir to oss
    //ssc.checkpoint("oss://bucket/inputdata/")
    ssc.checkpoint("./checkpoint")

    val dataStream = DatahubUtils.createStream(
      ssc,
      "zj_binlog",
      "zj_binlog",
      "159281146566829S86",
      "****",
      "****",
      "https://dh-cn-shenzhen.aliyuncs.com",
      transferFunc(_),
      StorageLevel.MEMORY_AND_DISK
    )


    /**
      * datahub streaming任务目前可以在自建spark上跑，不支持maxcompute平台
      *
      * 目前MaxCompute Spark支持以下使用场景：
      * Java/Scala所有离线场景，GraphX、Mllib、RDD、Spark-SQL、PySpark等。
      * 读写MaxCompute Table。
      * OSS非结构化存储支持。
      * 读写VPC环境下的服务。例如，RDS、Redis、ECS上部署的服务等。
      *
      * 暂不支持以下场景：
      * Streaming场景。
      * 交互式类需求，Spark-Shell、Spark-SQL-Shell、PySpark-Shell等。
      */

    val table_name="test_table"
    dataStream.map(x => new String(x)).foreachRDD(rdd => {
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      //import spark._
      import spark.implicits._

      //rdd.toDF("id").write.mode("append").saveAsTable(table_name)


      rdd.foreach(record=>println(record))

    })


    ssc.start()
    ssc.awaitTermination()
  }
}
