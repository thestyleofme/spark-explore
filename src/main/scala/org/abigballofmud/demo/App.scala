package org.abigballofmud.demo

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.abigballofmud.streaming.handler.HandlerApp
import org.abigballofmud.streaming.model.VinSourceData
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import scalikejdbc._
import scalikejdbc.config.DBs

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2019/10/20 0:45
 * @since 1.0
 */
object App {
  private val log = Logger(LoggerFactory.getLogger(HandlerApp.getClass))
  private val gson = new Gson()


  def main(args: Array[String]): Unit = {

    val aList: List[String] = List("a")
    println("b" +: aList)


    /* val load: Config = ConfigFactory.load()

     // kafka
     val kafkaParams: Map[String, Object] = Map[String, Object](
       "bootstrap.servers" -> load.getString("kafka.brokers"),
       "key.deserializer" -> classOf[StringDeserializer],
       "value.deserializer" -> classOf[StringDeserializer],
       "group.id" -> load.getString("kafka.group.id"),
       "auto.offset.reset" -> "earliest",
       "enable.auto.commit" -> (false: java.lang.Boolean)
     )
     val topics: Set[String] = load.getString("kafka.topics").split(",").toSet


     val conf: SparkConf = new SparkConf().
       setMaster("local[2]").
       setAppName("streaming_test").
       // 加这个配置访问集群中的hive
       // https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
       set("spark.sql.warehouse.dir", "/warehouse/tablespace/managed/hive").
       set("metastore.catalog.default", "hive").
       set("hive.metastore.uris", "thrift://hdsp001:9083")

     // 创建StreamingSession
     val sparkSession: SparkSession = getOrCreateSparkSession(conf)
     val sc: SparkContext = sparkSession.sparkContext
     // 创建StreamingContext
     val ssc = new StreamingContext(sc, Seconds(5))

     // 从mysql获取topic的offset
     // todo 数据库连接池
     // 加载配置信息
     DBs.setup()
     val offsets: Map[TopicPartition, Long] = DB.readOnly { implicit session =>
       sql"""select topic, group_id, partitions, offset from spark_streaming_kafka_offset where group_id = ?"""
         .bind(load.getString("kafka.group.id"))
         .map(rs => {
           (new TopicPartition(rs.string("topic"), rs.int("partition")), rs.long("offset"))
         }).list().apply()
     }.toMap

     val stream: InputDStream[ConsumerRecord[String, String]] = if (offsets.isEmpty) {
       // 第一次启动
       KafkaUtils.createDirectStream(
         ssc,
         LocationStrategies.PreferConsistent,
         ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
       )
     } else {
       // 非第一次启动

       // todo 检验offset是否过期, 存的和查询出来进行对比

       KafkaUtils.createDirectStream(
         ssc,
         LocationStrategies.PreferConsistent,
         ConsumerStrategies.Assign[String, String](offsets.keys.toList, kafkaParams, offsets))
     }

     // 处理kafka数据
     stream.foreachRDD(rdd => {
       val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

       rdd.map(_.value())
         .map(o => gson.fromJson(o, classOf[VinSourceData]))
         .filter(_.st.equalsIgnoreCase("event.suprKnkSrv.knkRatio")
         )

       // 记录offset
       offsetRanges.foreach { offsetRange =>
         log.info("save offsets, topic: {}, partition: {}, offset: {}",
           offsetRange.topic,
           offsetRange.partition,
           offsetRange.untilOffset)
         DB.autoCommit { implicit session =>
           sql"""replace into spark_streaming_kafka_offset(topic, group_id, partitions, offset) values(?,?,?,?)"""
             .bind(
               offsetRange.topic,
               load.getString("kafka.group.id"),
               offsetRange.partition,
               offsetRange.untilOffset)
             .update()
             .apply()
         }

       }
     }
     )

     // 启动并等待
     startAndWait(ssc)*/
  }


  /**
   * 获取或创建SparkSession
   *
   * @return SparkSession
   */
  def getOrCreateSparkSession(conf: SparkConf): SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    spark
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
