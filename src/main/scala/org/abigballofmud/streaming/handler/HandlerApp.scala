package org.abigballofmud.streaming.handler

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.typesafe.scalalogging.Logger
import org.abigballofmud.streaming.model.{SourceData, VinSourceData}
import org.abigballofmud.streaming.redis.InternalRedisClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2019/10/17 11:34
 * @since 1.0
 */
//noinspection DuplicatedCode
object HandlerApp {

  private val log = Logger(LoggerFactory.getLogger(HandlerApp.getClass))
  private val gson = new Gson()

  def main(args: Array[String]): Unit = {

    // 如果hadoop没有启Kerberos或者从Kerberos获取的用户为null，那么获取HADOOP_USER_NAME环境变量，
    // 并将它的值作为Hadoop执行用户。如果我们没有设置HADOOP_USER_NAME环境变量，那么程序将调用whoami来获取当前用户，并用groups来获取用户所在组
    System.setProperty("HADOOP_USER_NAME", "hive")
    // Spark默认将用户程序运行所在系统的当前登录用户作为用户程序的关联用户
    System.setProperty("user.name", "hive")

    // kafka
    //    val brokers: String = "hdsp001:6667,hdsp002:6667,hdsp003:6667"
    val brokers: String = "poc1:9093,poc2:9093,poc3:9093"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "poc_uaes_streaming",
      "auto.offset.reset" -> "none",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //    val topicPartitions = Map[String, Int]("vin-source" -> 3)
    //    val topicPartitions = Map[String, Int]("uaes-super-knock-poc" -> 1)
    val topicPartitions = Map[String, Int]("GW-GWM-uaes-topic" -> 1)

    // redis
    val redisHost: String = "hdsp004"
    val redisPort: Int = 6379
    val redisPassword: String = "hdsp_dev"

    // 创建redis
    InternalRedisClient.makePool(redisHost, redisPort, redisPassword)
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("poc_streaming_test")
      // 加这个配置访问集群中的hive
      // https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
      .set("spark.sql.warehouse.dir", "/warehouse/tablespace/managed/hive")
      .set("metastore.catalog.default", "hive")
      .set("hive.metastore.uris", "thrift://hdsp001:9083")
      // spark调优 http://www.imooc.com/article/262032
      // 以下设置是为了减少hdfs小文件的产生
      //    https://www.cnblogs.com/dtmobile-ksw/p/11254294.html
      //    https://www.cnblogs.com/dtmobile-ksw/p/11293891.html
      .set("spark.sql.adaptive.enabled", "true")
      // 默认值64M
      .set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "67108864")
      .set("spark.sql.adaptive.join.enabled", "true")
      // 20M
      .set("spark.sql.autoBroadcastJoinThreshold", "20971520")

    // 创建StreamingSession
    val sparkSession = getOrCreateSparkSession(conf)
    val sc = sparkSession.sparkContext
    // 创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(30))

    // 创建kafka流
    val kafkaStream = createKafkaStream(ssc, topicPartitions, kafkaParams)
    // 开始消费，业务逻辑
    handler(kafkaStream, sparkSession)
    // 启动并等待
    startAndWait(ssc)
  }

  /**
   * 消费处理业务逻辑
   *
   * @param stream InputDStream
   */
  def handler(stream: InputDStream[ConsumerRecord[String, String]], sparkSession: SparkSession): Unit = {
    stream.foreachRDD { rdd =>
      // 获取offset信息
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 计算相关指标，这里就统计下条数了
      val total = rdd.count()

      // 写入Hive表，指定压缩格式。发现可以自动创建表
      import sparkSession.implicits._
      //      val df = rdd.map(_.value()).map(o => gson.fromJson(o, classOf[VinSourceData])).toDF()
      val df = rdd.mapPartitions(x => {
        x.map(_.value())
          .filter(str => str.contains("event.suprKnkSrv") || str.contains("gps.") || str.contains("rolling."))
          .map(o => {
            try {
              val typeToken = new TypeToken[java.util.Map[String, String]]() {}.getType
              val m: java.util.Map[String, String] = gson.fromJson(o, typeToken)
              // gson.fromJson(o, classOf[VinSourceData])
              //  val m = JSONObject.fromObject(o)
              val data = new SourceData()
              data.vin = m.get("vin")
              data.ts = m.get("ts")
              data.ct = m.get("ct")
              data.oem = m.get("oem")
              data.st = m.get("st")
              data.value = m.get("val")
              data.rc = m.getOrDefault("rc", null)
              data.ts_time = LocalDateTime.ofInstant(Instant.ofEpochMilli(m.get("ts").substring(0, 13).toLong), ZoneId.of("Asia/Shanghai"))
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
              val eventType = data.st.toString.split("\\.")
              if (eventType.isEmpty) {
                data.sts = data.st
              } else {
                data.sts = eventType(eventType.length - 1)
              }
              data
            } catch {
              case e: Exception =>
                log.error("gson serialization error,data: {}", o, e)
                null
            }
          })
      })
        .filter(sourceData => sourceData != null)
        .map(sourceData => VinSourceData(sourceData.data_id,
          sourceData.vin,
          sourceData.st,
          sourceData.value,
          sourceData.ts,
          sourceData.ct,
          sourceData.oem,
          sourceData.rc,
          sourceData.sts,
          sourceData.ts_time,
          sourceData.creation_date)).toDF()

      /*df.createTempView("vin_source")
      sparkSession.sql("select * from vin_source where sts = 'c_SuprKnk'")*/

      //      sparkSession.sql("show databases").show()
      df.show()
      if (df.count() > 0) {
        df.write.mode(SaveMode.Append).format("hive").saveAsTable("poc_uaes.stg_vin_source_data4")
      }

      val jedis = InternalRedisClient.getResource
      val pipeline = jedis.pipelined()
      // 会阻塞redis
      pipeline.multi()

      // 更新相关指标
      pipeline.incrBy("poc_" + offsetRanges.apply(0).topic + ":total", total)

      // 更新offset
      offsetRanges.foreach { offsetRange =>
        log.info("save offsets, topic: {}, partition: {}, offset: {}",
          offsetRange.topic,
          offsetRange.partition,
          offsetRange.untilOffset)
        val topicPartitionKey = "poc_" + offsetRange.topic + ":" + offsetRange.partition
        pipeline.set(topicPartitionKey, offsetRange.untilOffset + "")
      }

      // 执行，释放
      pipeline.exec()
      pipeline.sync()
      pipeline.close()
      InternalRedisClient.recycleResource(jedis)
    }
  }

  /**
   * 获取或创建SparkSession
   *
   * @return SparkSession
   */
  def getOrCreateSparkSession(conf: SparkConf): SparkSession = {
    val spark = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    spark
  }

  /**
   * 创建kafka流
   *
   * @param ssc StreamingContext
   * @return InputDStream
   */
  def createKafkaStream(ssc: StreamingContext,
                        topicPartitions: Map[String, Int],
                        kafkaParams: Map[String, Object]): InputDStream[ConsumerRecord[String, String]] = {
    val offsets = getOffsets(topicPartitions)

    // 创建kafka stream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String, String](offsets.keys.toList, kafkaParams, offsets)
    )
    stream
  }

  /**
   * 从redis中获取offset信息
   *
   * @return Map[TopicPartition, Long]
   */
  def getOffsets(topicPartitions: Map[String, Int]): Map[TopicPartition, Long] = {
    val jedis = InternalRedisClient.getResource

    // 设置每个分区起始的offset
    val offsets = mutable.Map[TopicPartition, Long]()

    topicPartitions.foreach { it =>
      val topic = it._1
      val partitions = it._2
      // 遍历分区，设置每个topic下对应partition的offset
      for (partition <- 0 until partitions) {
        val topicPartitionKey = topic + ":" + partition
        var lastOffset = 0L
        val lastSavedOffset = jedis.get(topicPartitionKey)

        if (null != lastSavedOffset) {
          try {
            lastOffset = lastSavedOffset.toLong
          } catch {
            case e: Exception =>
              log.error("get last saved offset error", e)
              System.exit(1)
          }
        }
        log.info("from redis topic: {}, partition: {}, last offset: {}", topic, partition, lastOffset)

        // 添加
        offsets += (new TopicPartition(topic, partition) -> lastOffset)
      }
    }

    InternalRedisClient.recycleResource(jedis)

    offsets.toMap
  }

  /**
   * 启动并等待
   *
   * @param ssc StreamingContext
   */
  def startAndWait(ssc: StreamingContext): Unit = {
    // 启动
    ssc.start()
    // 等待
    ssc.awaitTermination()
  }

}
