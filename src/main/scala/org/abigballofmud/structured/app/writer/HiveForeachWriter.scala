package org.abigballofmud.structured.app.writer

import com.typesafe.scalalogging.Logger
import org.abigballofmud.common.CommonUtil
import org.abigballofmud.redis.InternalRedisClient
import org.abigballofmud.structured.app.model.SyncConfig
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.mutable

/**
 * <p>
 * NOTE: this class can not work at spark 2.4.x, the reason i want to know also
 * maybe??: the df ,spark2.3.x planWithBarrier ,however spark2.4.x no planWithBarrier
 * how to fix it?
 * </p>
 *
 * @author isacc 2020/02/10 11:59
 * @since 1.0
 */
//noinspection DuplicatedCode
object HiveForeachWriter {

  private val log = Logger(LoggerFactory.getLogger(HiveForeachWriter.getClass))

  def handle(syncConfig: SyncConfig, conf: SparkConf): ForeachWriter[Row] = {
    val hiveTableName: String = syncConfig.syncSpark.hiveDatabaseName + "." + syncConfig.syncSpark.hiveTableName
    val columns: List[String] = syncConfig.syncSpark.columns.trim.split(",").toList
    var colList: List[String] = List()
    colList = columns :+ "op" :+ "ts"
    val writer: ForeachWriter[Row] = new ForeachWriter[Row] {
      var pipeline: Pipeline = _
      var jedis: Jedis = _

      override def open(partitionId: Long, version: Long): Boolean = {
        jedis = InternalRedisClient.getResource
        pipeline = jedis.pipelined()
        // 会阻塞redis
        pipeline.multi()
        true
      }

      override def process(row: Row): Unit = {
        log.info("row: {}", row)
        val map: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
        map += ("appName" -> syncConfig.syncSpark.sparkAppName)
        for (i <- row.schema.fields.indices) {
          map += (row.schema.fields.apply(i).name -> row.getString(i))
        }
        val spark: SparkSession = CommonUtil.getOrCreateSparkSession(conf)
        val rowRDD: RDD[Row] = spark.sparkContext.makeRDD(Seq(row))
        val data: DataFrame = spark.createDataFrame(rowRDD, row.schema).selectExpr(colList: _*)
        // there will throw NPE if work at spark2.4.x
        data.show(false)
        if (data.count() > 0) {
          data.write.mode(SaveMode.Append).format("hive").saveAsTable(hiveTableName)
        }
        // 记录offset
        pipeline.set(map("topic") + ":" + map("appName"), "{\"%s\":%s}".format(map("partition"), map("offset").toInt + 1))
      }

      override def close(errorOrNull: Throwable): Unit = {
        // 执行，释放
        pipeline.exec()
        pipeline.sync()
        pipeline.close()
        InternalRedisClient.recycleResource(jedis)
      }
    }
    writer
  }

}
