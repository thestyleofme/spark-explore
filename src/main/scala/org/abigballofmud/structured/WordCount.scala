package org.abigballofmud.structured

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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

    val conf: SparkConf = new SparkConf().
      setMaster("local[2]").
      setAppName("structured_streaming_test").
      // 加这个配置访问集群中的hive
      // https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
      set("spark.sql.warehouse.dir", "/warehouse/tablespace/managed/hive").
      set("metastore.catalog.default", "hive").
      set("hive.metastore.uris", "thrift://hdsp001:9083")

    // 创建StreamingSession
    val spark: SparkSession = getOrCreateSparkSession(conf)

    //    val spark: SparkSession = SparkSession
    //      .builder
    //      .appName("StructuredNetworkWordCount")
    //      .getOrCreate()

    import spark.implicits._
    // 输入表
    val lines: DataFrame = spark
      .readStream
      .format("socket")
      .option("host", "hdsp001")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    //    val wordCounts: DataFrame = words.groupBy("value").count()
    words.createOrReplaceTempView("word")
    // 输出表
    val wordCounts: DataFrame = spark.sql(
      """
        |select
        |*,
        |count(*) count
        |from word
        |group by value
        |""".stripMargin)

    // Start running the query that prints the running counts to the console
    val query: StreamingQuery = wordCounts
      .writeStream
      /*
      三种模式： 思想--> 输入表 输出表 主要对比输出表的数据
      complete: 全部输出，必须有聚合
      append: 追加模式，只输出那些将来永远不可能再更新的数据，
              没有聚合的时候，append和update一致
              有聚合的时候，必须有watermark
      update: 只输出变化的部分
       */
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
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    spark
  }


}
