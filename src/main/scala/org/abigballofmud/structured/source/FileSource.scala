package org.abigballofmud.structured.source

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/02/09 4:20
 * @since 1.0
 */
object FileSource {

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
    import spark.implicits._
    val userSchema: StructType = new StructType()
      .add("name", StringType)
      .add("age", LongType)
      .add("sex", StringType)
    val userDf: DataFrame = spark.readStream
      .schema(userSchema)
      .json("E:\\spark_test\\file_source_json")
    //      .csv("E:\\spark_test\\file_source_csv")

    //    val df: Dataset[Row] = userDf.select("name", "age", "sex")
    //      .where("age>18")
    //      .groupBy("name")
    //      .count()

    val ds: Dataset[User] = userDf.as[User]
//      .filter(user => user.age > 18)
      .filter(_.age > 18)

    //    df.writeStream
    ds.writeStream
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

case class User(name: String,
                age: Long,
                sex: String) extends Serializable
