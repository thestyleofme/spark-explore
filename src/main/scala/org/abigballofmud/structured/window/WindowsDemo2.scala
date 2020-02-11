package org.abigballofmud.structured.window

import java.util.concurrent.TimeUnit

import org.abigballofmud.common.CommonUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * <p>
 * 窗口的划分见源码
 * org.apache.spark.sql.catalyst.analysis.Analyzer
 *
 * The windows are calculated as below:
 * maxNumOverlapping <- ceil(windowDuration / slideDuration)
 * for (i <- 0 until maxNumOverlapping)
 * windowId <- ceil((timestamp - startTime) / slideDuration)
 * windowStart <- windowId * slideDuration + (i - maxNumOverlapping) * slideDuration + startTime
 * windowEnd <- windowStart + windowDuration
 * return windowStart, windowEnd
 * </p>
 *
 * @author isacc 2020/02/10 1:05
 * @since 1.0
 */
//noinspection DuplicatedCode
object WindowsDemo2 {

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
      .load()
      .as[String]
      .map(line => {
        // 2020-02-12 09:50:25,hello
        val split: Array[String] = line.split(",")
        (split(0), split(1))
      })
      .toDF("ts", "word")
      .groupBy(
        window($"ts", "10 minutes", "5 minutes"),
        $"word"
      )
      .count()
      .sort("window")


    lines.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime(2, TimeUnit.SECONDS))
      .option("truncate", value = false)
      .start()
      .awaitTermination()

  }

}



