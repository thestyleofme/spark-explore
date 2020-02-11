package org.abigballofmud.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/02/10 1:13
 * @since 1.0
 */
object CommonUtil {

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
