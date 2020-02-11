package org.abigballofmud.structured.window

import java.sql.Timestamp

import org.abigballofmud.common.CommonUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/02/10 1:05
 * @since 1.0
 */
object WindowsDemo {

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
    val spark: SparkSession = CommonUtil.getOrCreateSparkSession(conf)
    import spark.implicits._
    // 导入spark提供的全局函数
    import org.apache.spark.sql.functions._
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "hdsp001")
      .option("port", 9999)
      // 给产生的数据自动添加时间戳
      .option("includeTimestamp", value = true)
      .load()
      .as[(String, Timestamp)]
      .flatMap {
        case (words, ts) => words.split("\\W+").map((_, ts))
      }.toDF("word", "ts")
      .groupBy(
        window($"ts", "4 minutes", "2 minutes"),
        $"word"
      ).count()


    lines.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .option("truncate", value = false)
      .start()
      .awaitTermination()

  }

}



