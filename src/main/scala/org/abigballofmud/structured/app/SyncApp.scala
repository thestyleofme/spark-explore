package org.abigballofmud.structured.app

import java.util.concurrent.TimeUnit

import com.google.gson.Gson
import com.typesafe.scalalogging.Logger
import org.abigballofmud.common.CommonUtil
import org.abigballofmud.redis.InternalRedisClient
import org.abigballofmud.structured.app.constants.WriterTypeConstant
import org.abigballofmud.structured.app.model.SyncConfig
import org.abigballofmud.structured.app.writer.{HiveForeachBatchWriter, HiveForeachWriter}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

/**
 * <p>
 * 运行示例:
 * nohup /usr/hdp/3.1.0.0-78/spark2/bin/spark-submit \
 * --class org.abigballofmud.structured.SyncApp \
 * --master yarn
 * --deploy-mode client \
 * /data/spark-apps/spark-app-1.1.3-SNAPSHOT.jar 'file' '/data/spark_config_file/userinfo_spark_sync.json' \
 * > /data/spark-apps/spark-sync.log 2>&1 &
 * </p>
 *
 * @author abigballofmud 2019/11/04 15:33
 * @since 1.0
 */
//noinspection DuplicatedCode
object SyncApp {

  private val log = Logger(LoggerFactory.getLogger(SyncApp.getClass))
  private val gson = new Gson()

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hive")
    System.setProperty("user.name", "hive")

    log.info("load spark conf file: [{}]", args.apply(0) + ":" + args.apply(1))

    // 解析参数
    var syncConfigStr: String = null
    var syncConfig: SyncConfig = null
    val tempSc = new SparkContext(new SparkConf()
      //      本地调试时 不注释 spark服务器运行时需注释
      .setMaster("local[2]")
      .setAppName("test"))
    if (args.apply(0).equals("file")) {
      syncConfigStr = tempSc.textFile("file:///" + args.apply(1)).collect().mkString
      syncConfig = gson.fromJson(syncConfigStr, classOf[SyncConfig])
    } else {
      syncConfigStr = tempSc.textFile("hdfs://" + args.apply(1)).collect().mkString
      syncConfig = gson.fromJson(syncConfigStr, classOf[SyncConfig])
    }
    tempSc.stop()

    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(syncConfig.syncSpark.sparkAppName)
      // 加这个配置访问集群中的hive
      // https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
      //      .set("spark.sql.warehouse.dir", "/warehouse/tablespace/managed/hive")
      //      .set("metastore.catalog.default", "hive")
      .set("hive.metastore.uris", syncConfig.syncHive.metastoreUris)
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

    // redis
    val redisHost: String = syncConfig.syncRedis.redisHost
    val redisPort: Int = syncConfig.syncRedis.redisPort
    val redisPassword: String = syncConfig.syncRedis.redisPassword

    // kafka
    val topic: String = syncConfig.syncKafka.kafkaTopic

    val brokers: String = syncConfig.syncKafka.kafkaBootstrapServers

    // 创建redis
    InternalRedisClient.makePool(redisHost, redisPort, redisPassword)

    var partitionOffset: String = getLastTopicOffset(topic)
    if (partitionOffset == null) {
      partitionOffset = syncConfig.syncKafka.initDefaultOffset
    } else {
      partitionOffset = "{\"%s\":%s}".format(topic, partitionOffset).trim
    }
    // 创建StreamingSession
    val spark: SparkSession = CommonUtil.getOrCreateSparkSession(conf)
    import spark.implicits._
    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .option("failOnDataLoss", value = false)
      .option("startingOffsets", partitionOffset)
      .load()

    val columns: List[String] = syncConfig.syncSpark.columns.trim.split(",").toList
    var dataStructType = new StructType()
    var afterList: List[String] = List()
    var beforeList: List[String] = List()
    for (elem <- columns) {
      dataStructType = dataStructType.add(elem.trim, StringType, nullable = false)
      afterList = afterList :+ ("event.payload.after." + elem.trim)
      beforeList = beforeList :+ ("event.payload.before." + elem.trim)
    }
    afterList = afterList :+ "ts" :+ "topic" :+ "partition" :+ "offset"
    beforeList = beforeList :+ "ts" :+ "topic" :+ "partition" :+ "offset"

    val payloadSchema_o: StructType = new StructType()
      .add("payload",
        new StructType().add("op", StringType)
          .add("before", dataStructType, nullable = false)
          .add("after", dataStructType, nullable = false)
      )

    StructType(payloadSchema_o)
    val df_c_u: DataFrame = df.select(
      functions.from_json(functions.col("value").cast("string"), payloadSchema_o).alias("event"),
      functions.col("timestamp").cast("string").alias("ts"),
      functions.col("topic").cast("string").alias("topic"),
      functions.col("partition").cast("string").alias("partition"),
      functions.col("offset").cast("string").alias("offset"))
      .filter($"event.payload.op".===("c") || $"event.payload.op".===("u"))
      .select("event.payload.op",
        afterList: _*)
    val df_d: DataFrame = df.select(
      functions.from_json(functions.col("value").cast("string"), payloadSchema_o).alias("event"),
      functions.col("timestamp").cast("string").alias("ts"),
      functions.col("topic").cast("string").alias("topic"),
      functions.col("partition").cast("string").alias("partition"),
      functions.col("offset").cast("string").alias("offset"))
      .filter($"event.payload.op".===("d"))
      .select("event.payload.op",
        beforeList: _*)
    val ds: Dataset[Row] = df_c_u.union(df_d)

    val query: StreamingQuery = ds.repartition(1).writeStream
      .trigger(Trigger.ProcessingTime(syncConfig.syncSpark.interval, TimeUnit.SECONDS))
//      .foreach(writer = getForeachSink(syncConfig, conf))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        handler(syncConfig, batchDF, batchId, spark)
      }
      .start()

    query.awaitTermination()
  }

  val handler: (SyncConfig, DataFrame, Long, SparkSession) =>
    Unit = (syncConfig: SyncConfig,
            batchDF: DataFrame,
            batchId: Long,
            spark: SparkSession) => {
    val writeType: String = syncConfig.syncSpark.writeType
    if (WriterTypeConstant.HIVE.equalsIgnoreCase(writeType)) {
      // 直接写到表
      HiveForeachBatchWriter.handle(syncConfig, batchDF, batchId, spark)
    } else {
      throw new IllegalArgumentException("invalid writeType")
    }
  }

  def getForeachSink(syncConfig: SyncConfig, conf: SparkConf): ForeachWriter[Row] = {
    val writeType: String = syncConfig.syncSpark.writeType
    var writer: ForeachWriter[Row] = null
    if (WriterTypeConstant.HIVE.equalsIgnoreCase(writeType)) {
      writer = HiveForeachWriter.handle(syncConfig, conf)
    } else {
      throw new IllegalArgumentException("invalid writeType")
    }
    writer
  }

  /**
   * 获取topic上次消费的最新offset
   *
   * @return offset
   */
  def getLastTopicOffset(topic: String): String = {
    val jedis: Jedis = InternalRedisClient.getResource
    val partitionOffset: String = jedis.get(topic)
    jedis.close()
    partitionOffset
  }

}
