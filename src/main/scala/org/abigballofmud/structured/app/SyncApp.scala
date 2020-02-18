package org.abigballofmud.structured.app

import java.util.concurrent.TimeUnit

import com.google.gson.Gson
import com.typesafe.scalalogging.Logger
import org.abigballofmud.common.CommonUtil
import org.abigballofmud.redis.InternalRedisClient
import org.abigballofmud.structured.app.constants.WriterTypeConstant
import org.abigballofmud.structured.app.model.SyncConfig
import org.abigballofmud.structured.app.utils.JdbcUtil
import org.abigballofmud.structured.app.writer.{HiveForeachBatchWriter, JdbcForeachBatchWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.mutable

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

    var colList: List[String] = List()
    if (syncConfig.syncSpark.writeType.equalsIgnoreCase(WriterTypeConstant.FILE)) {
      doFileWrite(ds, syncConfig)
    } else {
      // 其他写操作暂时使用foreach
      ds.repartition(1).writeStream
        .trigger(Trigger.ProcessingTime(syncConfig.syncSpark.interval, TimeUnit.SECONDS))
        .foreach(writer = new ForeachWriter[Row] {
          var pipeline: Pipeline = _
          var jedis: Jedis = _

          override def open(partitionId: Long, version: Long): Boolean = {
            jedis = InternalRedisClient.getResource
            pipeline = jedis.pipelined()
            // 会阻塞redis
            pipeline.multi()
            // hive加上 op ts
            if (syncConfig.syncSpark.writeType.equalsIgnoreCase(WriterTypeConstant.FILE)) {
              colList = columns :+ "op" :+ "ts"
            } else if (syncConfig.syncSpark.writeType.equalsIgnoreCase(WriterTypeConstant.JDBC)) {
              colList = columns :+ "op"
            }
            true
          }

          override def process(row: Row): Unit = {
            val map: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
            map += ("appName" -> syncConfig.syncSpark.sparkAppName)
            for (i <- row.schema.fields.indices) {
              map += (row.schema.fields.apply(i).name -> row.getString(i))
            }
            val spark: SparkSession = CommonUtil.getOrCreateSparkSession(conf)
            val rowRDD: RDD[Row] = spark.sparkContext.makeRDD(Seq(row))
            val data: DataFrame = spark.createDataFrame(rowRDD, row.schema).selectExpr(colList: _*)
            // here will throw NPE if work at spark2.4.x
            if (data.count() > 0) {
              // 写到不同端
              doWrite(data, syncConfig, columns)
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
        })
        // spark 2.4.x 后支持
        //      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        //        handler(syncConfig, batchDF, batchId, spark, columns)
        //      }
        .start().awaitTermination()
    }

  }

  /**
   * 写文件，也可写到hive表
   *
   * @param ds         Dataset[Row]
   * @param syncConfig syncConfig
   */
  def doFileWrite(ds: Dataset[Row], syncConfig: SyncConfig): Unit = {
    ds.repartition(1).writeStream
      .trigger(Trigger.ProcessingTime(syncConfig.syncSpark.interval, TimeUnit.SECONDS))
      .format(syncConfig.syncFile.format)
      .outputMode(syncConfig.syncFile.writeMode)
      //      后续支持
      //      .partitionBy(syncConfig.syncFile.partitionBy)
      .option("checkpointLocation", syncConfig.syncFile.checkpointLocation)
      .option("path", syncConfig.syncFile.writePath)
      .start().awaitTermination()
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

  /**
   * Spark2.3执行写操作
   *
   * @param df         DataFrame
   * @param syncConfig SyncConfig
   */
  def doWrite(df: DataFrame, syncConfig: SyncConfig, columns: List[String]): Unit = {
    val writeType: String = syncConfig.syncSpark.writeType
    if (WriterTypeConstant.HIVE.equalsIgnoreCase(writeType)) {
      val hiveTableName: String = syncConfig.syncFile.hiveDatabaseName + "." + syncConfig.syncFile.hiveTableName
      df.write.mode(SaveMode.Append).format("hive").saveAsTable(hiveTableName)
    } else if (WriterTypeConstant.JDBC.equalsIgnoreCase(writeType)) {
      JdbcUtil.saveDFtoDBCreateTableIfNotExist(syncConfig, df, columns)
    } else {
      throw new IllegalArgumentException("invalid writeType")
    }
  }

  /**
   * spark 2.4执行写操作
   */
  val handler: (SyncConfig, DataFrame, Long, SparkSession, List[String]) =>
    Unit = (syncConfig: SyncConfig,
            batchDF: DataFrame,
            batchId: Long,
            spark: SparkSession,
            columns: List[String]) => {
    val writeType: String = syncConfig.syncSpark.writeType
    if (WriterTypeConstant.HIVE.equalsIgnoreCase(writeType)) {
      HiveForeachBatchWriter.handle(syncConfig, batchDF, batchId, spark, columns)
    } else if (WriterTypeConstant.JDBC.equalsIgnoreCase(writeType)) {
      JdbcForeachBatchWriter.handle(syncConfig, batchDF, batchId, spark, columns)
    } else {
      throw new IllegalArgumentException("invalid writeType")
    }
  }

}
