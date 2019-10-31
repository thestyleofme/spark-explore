package org.abigballofmud.streaming.kafka

import java.util.{Collections, Properties}

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.iterableAsScalaIterable

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2019/10/20 18:31
 * @since 1.0
 */
object KafkaConsumerApp {
  private val log = Logger(LoggerFactory.getLogger(KafkaConsumerApp.getClass))

  def main(args: Array[String]): Unit = {
//    val brokers = "hdsp001:6667,hdsp002:6667,hdsp003:6667"
//    val topic = "vin-source"
//    val brokers = "poc1:9093,poc2:9093,poc3:9093"
//    val topic = "uaes-super-knock-poc"
//    val topic = "GW-GWM-uaes-topic"
    val brokers = "docker02:9092"
    val topic = "mytest.just_test.book"
    // 配置
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    // group
    props.put("group.id", "test1111")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", false: java.lang.Boolean)
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    // 创建消费者
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singleton(topic))
    // 消费数据
    try {
      while (true) {
        // poll数据
        val records = consumer.poll(500)
        // 处理
        for (record <- records) {
          log.info("partition: {}, offset: {}, key: {}, value: {}",
            record.partition,
            record.offset,
            record.key,
            record.value)
        }
        // 异步提交offset
        consumer.commitAsync()
      }
    } finally {
      consumer.close()
    }
  }
}
