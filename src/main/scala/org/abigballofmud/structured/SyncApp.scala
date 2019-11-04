package org.abigballofmud.structured

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{SparkSession, functions}

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2019/11/04 15:33
 * @since 1.0
 */
//noinspection DuplicatedCode
object SyncApp {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hive")
    System.setProperty("user.name", "hive")

    val conf = new SparkConf().
      setMaster("local[2]").
      setAppName("test_structured_streaming")

    // 创建StreamingSession
    val spark = getOrCreateSparkSession(conf)
    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hdsp001:6667,hdsp002:6667,hdsp003:6667")
      .option("subscribe", "hdsp_spark_sync.hdsp_core.test_userinfo")
      .option("failOnDataLoss", value = false)
      .load()

    val payloadSchema_o = new StructType()
      .add("payload",
        new StructType().add("op", StringType)
          .add("before", new StructType()
            .add("id", StringType, nullable = false)
            .add("username", StringType, nullable = false)
            .add("password", StringType, nullable = false)
            .add("age", StringType, nullable = false)
            .add("sex", StringType, nullable = false)
            .add("address", StringType, nullable = false)
            , nullable = false)
          .add("after",
            new StructType()
              .add("id", StringType, nullable = false)
              .add("username", StringType, nullable = false)
              .add("password", StringType, nullable = false)
              .add("age", StringType, nullable = false)
              .add("sex", StringType, nullable = false)
              .add("address", StringType, nullable = false)
            , nullable = false)
      )

    StructType(payloadSchema_o)
    val nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
    val df_c_u = df.select(
      functions.from_json(functions.col("value").cast("string"), payloadSchema_o).alias("event"),
      functions.col("timestamp").cast("string").alias("ts"))
      .filter($"event.payload.op".===("c") || $"event.payload.op".===("u"))
      .select("event.payload.op",
        "event.payload.after.id",
        "event.payload.after.username",
        "event.payload.after.password",
        "event.payload.after.age",
        "event.payload.after.sex",
        "event.payload.after.address",
        "ts")

    val df_d = df.select(
      functions.from_json(functions.col("value").cast("string"), payloadSchema_o).alias("event"),
      functions.col("timestamp").cast("string").alias("ts"))
      .filter($"event.payload.op".===("d"))
      .select("event.payload.op",
        "event.payload.before.id",
        "event.payload.before.username",
        "event.payload.before.password",
        "event.payload.before.age",
        "event.payload.before.sex",
        "event.payload.before.address",
        "ts")
    val ds = df_c_u.union(df_d)

    val query = ds.repartition(1)
      .writeStream
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .option("checkpointLocation", "check/path03")
      .format("parquet")
      .outputMode("append")
      .option("path", "hdfs://hdsp001:8020/warehouse/tablespace/spark/userinfo_parquet")
      .start()

    query.awaitTermination()
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
