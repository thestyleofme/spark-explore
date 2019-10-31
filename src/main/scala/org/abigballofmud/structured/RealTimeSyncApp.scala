package org.abigballofmud.structured

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession, functions}

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2019/10/22 15:33
 * @since 1.0
 */
//noinspection DuplicatedCode
object RealTimeSyncApp {

  def main(args: Array[String]): Unit = {

    // 如果hadoop没有启Kerberos或者从Kerberos获取的用户为null，那么获取HADOOP_USER_NAME环境变量，
    // 并将它的值作为Hadoop执行用户。如果我们没有设置HADOOP_USER_NAME环境变量，那么程序将调用whoami来获取当前用户，并用groups来获取用户所在组
    System.setProperty("HADOOP_USER_NAME", "hive")
    // Spark默认将用户程序运行所在系统的当前登录用户作为用户程序的关联用户
    System.setProperty("user.name", "hive")

    val conf = new SparkConf().
      setMaster("local[2]").
      setAppName("test_structured_streaming")
    // 加这个配置访问集群中的hive
    // https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
    //      .set("spark.sql.warehouse.dir", "/warehouse/tablespace/managed/hive")
    //      .set("metastore.catalog.default", "hive")
    //      .set("hive.metastore.uris", "thrift://hdsp001:9083")

    // 创建StreamingSession
    val spark = getOrCreateSparkSession(conf)
    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "docker02:9092")
      //      .option("subscribePattern", "mytest.just_test.*")
      .option("subscribe", "mytest.just_test.book")
      .option("failOnDataLoss", value = false)
      .load()

    //    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    //      .as[(String, String)]
    //
    //    // Write key-value data from a DataFrame to Kafka using a topic specified in the data
    //    val ds = df
    //      .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
    //      .writeStream
    //      .outputMode("append")
    //      .format("console")
    //      .start()
    //
    //    ds.awaitTermination()

    val payloadSchema_o = new StructType()
      .add("payload",
        new StructType().add("op", StringType)
          .add("before", StringType, nullable = false)
          .add("after",
            new StructType()
              .add("id", StringType, nullable = false)
              .add("author", StringType, nullable = false)
              .add("name", StringType, nullable = false)
              .add("page_count", StringType, nullable = false)
              .add("release_date", StringType, nullable = false)
            , nullable = false)
      )

    StructType(payloadSchema_o)
    val nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
    val ds = df.select(
      functions.from_json(functions.col("value").cast("string"), payloadSchema_o).alias("event"),
      functions.col("timestamp").cast("string").alias("ts"))
      .filter($"event.payload.op".===("c"))

      .select("event.payload.op",
        "event.payload.after.id",
        "event.payload.after.author",
        "event.payload.after.name",
        "event.payload.after.page_count",
        "event.payload.after.release_date",
        "ts")

      //      写到本地json测试
      //      .writeStream
      //      .trigger(Trigger.ProcessingTime("10 seconds"))
      //      .option("checkpointLocation", "check/path01")
      //      .outputMode("append")
      //      .format("json")
      //      .option("path", "output")
      //      .start()

      //    写到hdfs
      .repartition(1)
      .writeStream
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .option("checkpointLocation", "check/path02")
      //      .format("json")
      .format("parquet")
      //      .outputMode("append")
      //      .option("path", "hdfs://hdsp001:8020/warehouse/tablespace/managed/hive/test.db/book")
      //      .option("path", "hdfs://hdsp001:8020/warehouse/tablespace/managed/hive/test.db/book_parquet")
      .option("path", "hdfs://hdsp001:8020/warehouse/tablespace/spark/book_parquet")
      .start()

    // 直接写到表
    //      .repartition(1)
    //      .writeStream
    //      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
    //      .foreach(new ForeachWriter[Row]{
    //
    //        override def open(partitionId: Long, version: Long): Boolean = {
    //          true
    //        }
    //
    //        override def process(value: StructType): Unit = {
    //
    //        }
    //
    //        override def close(errorOrNull: Throwable): Unit = {
    //
    //        }
    //      })
    //      .start()

    ds.awaitTermination()

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


}
