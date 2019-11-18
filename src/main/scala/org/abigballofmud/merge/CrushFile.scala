package org.abigballofmud.merge

import org.apache.hadoop.fs._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * <p>
 * 合并日志文件：目前支持parquet file
 * 1.从fromDir目录中加载数据文件
 * 2.将文件输出到tmpDir，文件以arh字符开头
 * 3.mv tmpDir中的文件到fromDir中
 * 4.删除fromDir目录中非arh开头的文件
 * </p>
 *
 * @author isacc 2019/10/28 16:31
 * @since 1.0
 */
object CrushFile {

  def main(args: Array[String]) {
    // 如果hadoop没有启Kerberos或者从Kerberos获取的用户为null，那么获取HADOOP_USER_NAME环境变量，
    // 并将它的值作为Hadoop执行用户。如果我们没有设置HADOOP_USER_NAME环境变量，那么程序将调用whoami来获取当前用户，并用groups来获取用户所在组
    System.setProperty("HADOOP_USER_NAME", "hive")
    // Spark默认将用户程序运行所在系统的当前登录用户作为用户程序的关联用户
    System.setProperty("user.name", "hive")

    //    if (args.length < 3) {
    //      System.err.println("Usage: CrushFile <dfsUri> <abFromDir> <abTmpDir>")
    //      System.exit(1)
    //    }
    //    val Array(dfsUri, fromDir, tmpDir) = args
    //    val dfsUri = "hdfs://hdsp001:8020"
//    val fromDir = "hdfs://hdsp001:8020/warehouse/tablespace/spark/book_parquet"
//    val tmpDir = "hdfs://hdsp001:8020/warehouse/tablespace/spark/book_parquet_temp"
    val fromDir = "hdfs://hdsp001:8020/warehouse/tablespace/spark/userinfo_parquet"
    val tmpDir = "hdfs://hdsp001:8020/warehouse/tablespace/spark/userinfo_parquet_temp"

    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("spark-crush-file")
      // 加这个配置访问集群中的hive
      // https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
      .set("spark.sql.warehouse.dir", "/warehouse/tablespace/managed/hive")
      .set("metastore.catalog.default", "hive")
      .set("hive.metastore.uris", "thrift://hdsp001:9083")
      .set("spark.sql.parquet.compression.codec", "snappy")

    // 创建StreamingSession
    val sparkSession: SparkSession = getOrCreateSparkSession(conf)
    val sc: SparkContext = sparkSession.sparkContext
    val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)
//    val df: DataFrame = sparkSession.table("test.book_parquet")
    val df: DataFrame = sparkSession.table("test.userinfo_parquet")
    df.repartition(takePartition(fromDir, fs))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(tmpDir)
    // 删除源目录 mv 新文件
    mv(tmpDir, fromDir, fs)
    del(fromDir, fs)
  }

  /** 根据输入目录计算目录大小，并以128*2M大小计算partition */
  def takePartition(src: String, fs: FileSystem): Int = {
    val cos: ContentSummary = fs.getContentSummary(new Path(src))
    val sizeM: Long = cos.getLength / 1024 / 1024
    val partNum: Int = sizeM / 256 match {
      case 0 => 1
      case _ => (sizeM / 256).toInt
    }
    partNum
  }

  /** 将临时目录中的结果文件mv到源目录，并以ahr-为文件前缀 */
  def mv(fromDir: String, toDir: String, fs: FileSystem): Unit = {
    val srcFiles: Array[Path] = FileUtil.stat2Paths(fs.listStatus(new Path(fromDir)))
    for (p: Path <- srcFiles) {
      // 如果是以part开头的文件则修改名称
      if (p.getName.startsWith("part")) {
        fs.rename(p, new Path(toDir + "/ahr-" + p.getName))
      }
    }
    // 删除temp
    fs.delete(new Path(fromDir), true)
  }

  /** 删除原始小文件 */
  def del(fromDir: String, fs: FileSystem): Unit = {
    val files: Array[Path] = FileUtil.stat2Paths(fs.listStatus(new Path(fromDir), new FileFilter()))
    for (f: Path <- files) {
      // 迭代删除文件或目录
      fs.delete(f, true)
    }
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

class FileFilter extends PathFilter {
  @Override def accept(path: Path): Boolean = {
    !path.getName.startsWith("ahr-") && !path.getName.contains("_spark_metadata")
  }
}
