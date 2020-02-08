package org.abigballofmud.structured.source

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/02/09 4:20
 * @since 1.0
 */
object RateSource {

  def main(args: Array[String]): Unit = {
    // 如果hadoop没有启Kerberos或者从Kerberos获取的用户为null，那么获取HADOOP_USER_NAME环境变量，
    // 并将它的值作为Hadoop执行用户。如果我们没有设置HADOOP_USER_NAME环境变量，那么程序将调用whoami来获取当前用户，并用groups来获取用户所在组
    System.setProperty("HADOOP_USER_NAME", "hive")
    // Spark默认将用户程序运行所在系统的当前登录用户作为用户程序的关联用户
    System.setProperty("user.name", "hive")

    val conf: SparkConf = new SparkConf().
      setMaster("local[2]").
      setAppName("test_structured_streaming")
    // 加这个配置访问集群中的hive
    // https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
    //      .set("spark.sql.warehouse.dir", "/warehouse/tablespace/managed/hive")
    //      .set("metastore.catalog.default", "hive")
    //      .set("hive.metastore.uris", "thrift://hdsp001:9083")

    // 创建StreamingSession
    val spark: SparkSession = getOrCreateSparkSession(conf)
    val df: DataFrame = spark.readStream
      .format("rate")
      // How many rows should be generated per second. default: 1
      .option("rowsPerSecond ", "1")
      // How long to ramp up before the generating speed becomes rowsPerSecond. default: 0s
      .option("rampUpTime ", "5s")
      // The partition number for the generated rows. default: Spark's default parallelism
      .option("numPartitions", "2")
      .load()

    df.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .option("truncate", value = false)
      .start()
      .awaitTermination()

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
