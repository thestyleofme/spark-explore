package org.abigballofmud.structured.sink

import java.util.concurrent.TimeUnit

import com.google.gson.Gson
import com.typesafe.scalalogging.Logger
import org.abigballofmud.common.CommonUtil
import org.abigballofmud.redis.InternalRedisClient
import org.abigballofmud.structured.app.constants.WriterTypeConstant
import org.abigballofmud.structured.app.model.SyncConfig
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/02/18 17:48
 * @since 1.0
 */
//noinspection DuplicatedCode
object JdbcSinkDemo {

  private val log = Logger(LoggerFactory.getLogger(JdbcSinkDemo.getClass))
  private val gson = new Gson()

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalArgumentException("Need two args! one: the config json type[file/hdfs], two: the config json path")
    }

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

    if (WriterTypeConstant.HIVE.equalsIgnoreCase(syncConfig.syncSpark.writeType)) {
      conf
        // 加这个配置访问集群中的hive
        // https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
        //      .set("spark.sql.warehouse.dir", "/warehouse/tablespace/managed/hive")
        //      .set("metastore.catalog.default", "hive")
        .set("hive.metastore.uris", syncConfig.syncFile.metastoreUris)
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
    }

    // redis
    val redisHost: String = syncConfig.syncRedis.redisHost
    val redisPort: Int = syncConfig.syncRedis.redisPort
    val redisPassword: String = syncConfig.syncRedis.redisPassword

    // kafka
    val topic: String = syncConfig.syncKafka.kafkaTopic

    val brokers: String = syncConfig.syncKafka.kafkaBootstrapServers

    // 创建redis
    InternalRedisClient.makePool(redisHost, redisPort, redisPassword)

    // 获取topic的offset
    var partitionOffset: String = CommonUtil.getLastTopicOffset(topic, syncConfig.syncSpark.sparkAppName)
    if (partitionOffset == null) {
      partitionOffset = syncConfig.syncKafka.initDefaultOffset
    } else {
      partitionOffset = "{\"%s\":%s}".format(topic, partitionOffset).trim
    }

    // 创建StreamingSession
    val spark: SparkSession = CommonUtil.getOrCreateSparkSession(conf)
    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .option("failOnDataLoss", value = false)
      .option("startingOffsets", partitionOffset)
      .load()

    var columns: List[String] = List()
    for (i <- 0 until syncConfig.syncColumns.size()) {
      columns = columns :+ syncConfig.syncColumns.get(i).colName
    }
    val ds: Dataset[Row] = buildSchema(spark, df, columns)
    ds.writeStream
      .trigger(Trigger.ProcessingTime(syncConfig.syncSpark.interval, TimeUnit.SECONDS))
      .format("myJdbc")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", "check/path05")
      .option("syncConfig", gson.toJson(syncConfig))
      .start()
      .awaitTermination()
  }

  /**
   *
   * 构建schema
   *
   * @param spark   SparkSession
   * @param df      DataFrame
   * @param columns 写入字段列表
   * @return Dataset[Row]
   */
  def buildSchema(spark: SparkSession, df: DataFrame, columns: List[String]): Dataset[Row] = {
    import spark.implicits._
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
    ds
  }
}
