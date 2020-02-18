package org.abigballofmud.structured.app.writer

import org.abigballofmud.common.CommonUtil
import org.abigballofmud.structured.app.model.{SyncConfig, TopicInfo}
import org.abigballofmud.structured.app.utils.JdbcUtil
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/02/12 20:00:46
 * @since 1.3
 */
//noinspection DuplicatedCode
object JdbcForeachBatchWriter {

  def handle(syncConfig: SyncConfig, batchDF: DataFrame, batchId: Long, spark: SparkSession, columns: List[String]): Unit = {
    import spark.implicits._
    var colList: List[String] = List()
    colList = columns :+ "op"
    batchDF.persist()
    batchDF.show()
    val topicInfo: Dataset[TopicInfo] = batchDF.as[TopicInfo]
    val df: DataFrame = batchDF.toDF().selectExpr(colList: _*)
    if (df.count() > 0) {
      // 同步到jdbc类型数据库中
      JdbcUtil.saveDFtoDBCreateTableIfNotExist(syncConfig, df, columns)
    }
    // 记录topic消费信息
    CommonUtil.recordTopicOffset(topicInfo, syncConfig.syncSpark.sparkAppName)
    batchDF.unpersist()
  }
}
