package org.abigballofmud.utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * <p>
 * 测试出最小的coalesce数
 * </p>
 *
 * @author isacc 2019/10/25 10:48
 * @since 1.0
 */
object FileUtils {

  def getCoalesce(fileSystem: FileSystem, path: String, size: Int): Int = {
    var length = 0L
    fileSystem.globStatus(new Path(path))
      .foreach(x => {
        length += x.getLen
      })
    (length / 1024 / 1024 / size).toInt + 1
  }

  def main(args: Array[String]): Unit = {

    val config = new Configuration()
    val fs = FileSystem.get(new URI("hdfs://hdsp001:8020"), config)
    val coalesce = getCoalesce(fs, "/warehouse/tablespace/managed/hive/poc_uaes.db/ods_vin_source_data4", 20)

    println(coalesce)
  }
}
