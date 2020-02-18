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
 * @since spark2.4.x
 */
//noinspection DuplicatedCode
object HiveForeachBatchWriter {

  def handle(syncConfig: SyncConfig, batchDF: DataFrame, batchId: Long, spark: SparkSession,columns: List[String]): Unit = {
    import spark.implicits._
    // hive表中加 op ts
    var colList: List[String] = List()
    colList = columns :+ "op" :+ "ts"
    batchDF.persist()
    batchDF.show()
    val topicInfo: Dataset[TopicInfo] = batchDF.as[TopicInfo]
    val hiveTableName: String = syncConfig.syncFile.hiveDatabaseName + "." + syncConfig.syncFile.hiveTableName
    val df: DataFrame = batchDF.toDF().selectExpr(colList: _*)
    if (df.count() > 0) {
      df.write.mode(SaveMode.Append).format("hive").saveAsTable(hiveTableName)
    }
    // 记录topic消费信息
    CommonUtil.recordTopicOffset(topicInfo, syncConfig.syncSpark.sparkAppName)
    batchDF.unpersist()
  }
}
