package org.abigballofmud.common

import org.abigballofmud.redis.InternalRedisClient
import org.abigballofmud.structured.app.model.TopicInfo
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.mutable

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/02/10 1:13
 * @since 1.0
 */
object CommonUtil {

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
   * 获取topic上次消费的最新offset
   *
   * @return offset
   */
  def getLastTopicOffset(topic: String, appName: String): String = {
    val jedis: Jedis = InternalRedisClient.getResource
    val partitionOffset: String = jedis.get(topic + ":" + appName)
    jedis.close()
    partitionOffset
  }

  /**
   * 记录kafka消费信息
   *
   * @param topicInfo Dataset[TopicInfo]
   * @param appName   spark appName
   */
  def recordTopicOffset(topicInfo: Dataset[TopicInfo], appName: String): Unit = {
    val jedis: Jedis = InternalRedisClient.getResource
    val pipeline: Pipeline = jedis.pipelined()
    // 会阻塞redis
    pipeline.multi()
    val map: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
    for (elem <- topicInfo.collect()) {
      // kafka offset缓存
      map.empty
      map += ("topic" -> elem.topic)
      map += ("partition" -> elem.partition)
      map += ("offset" -> elem.offset)
      map += ("appName" -> appName)
      pipeline.set(map("topic") + ":" + map("appName"), "{\"%s\":%s}".format(map("partition"), map("offset").toInt + 1))
    }
    // 执行，释放
    pipeline.exec()
    pipeline.sync()
    pipeline.close()
    InternalRedisClient.recycleResource(jedis)
  }

}
