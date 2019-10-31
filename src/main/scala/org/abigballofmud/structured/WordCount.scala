package org.abigballofmud.structured

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2019/10/22 15:33
 * @since 1.0
 */
//noinspection DuplicatedCode
object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local[2]").
      setAppName("structured_streaming_test").
      // 加这个配置访问集群中的hive
      // https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
      set("spark.sql.warehouse.dir", "/warehouse/tablespace/managed/hive").
      set("metastore.catalog.default", "hive").
      set("hive.metastore.uris", "thrift://hdsp001:9083")

    // 创建StreamingSession
    val sparkSession = getOrCreateSparkSession(conf)

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._
    val lines = spark
      .readStream
      .format("socket")
      .option("host", "hdsp001")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts
      .writeStream
      .outputMode("complete")
      .format("console")
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
