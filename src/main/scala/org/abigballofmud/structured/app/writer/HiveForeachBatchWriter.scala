package org.abigballofmud.structured.app.writer

import org.abigballofmud.common.CommonUtil
import org.abigballofmud.structured.app.model.{SyncConfig, TopicInfo}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/02/11 11:37
 * @since 1.3
 */
//noinspection DuplicatedCode
object HiveForeachBatchWriter {

  def handle(syncConfig: SyncConfig, batchDF: DataFrame, batchId: Long, spark: SparkSession): Unit = {
    import spark.implicits._
    // hive表中加 op ts
    val columns: List[String] = syncConfig.syncSpark.columns.trim.split(",").toList
    var colList: List[String] = List()
    colList = columns :+ "op" :+ "ts"
    batchDF.persist()
    batchDF.show()
    val topicInfo: Dataset[TopicInfo] = batchDF.as[TopicInfo]
    val hiveTableName: String = syncConfig.syncHive.hiveDatabaseName + "." + syncConfig.syncHive.hiveTableName
    val df: DataFrame = batchDF.toDF().selectExpr(colList: _*)
    if (df.count() > 0) {
      df.write.mode(SaveMode.Append).format("hive").saveAsTable(hiveTableName)
    }
    // 记录topic消费信息
    CommonUtil.recordTopicOffset(topicInfo, syncConfig.syncSpark.sparkAppName)
    batchDF.unpersist()
  }
}
