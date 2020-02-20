package org.abigballofmud.structured.sink

import java.util.Properties

import com.google.gson.Gson
import org.abigballofmud.structured.app.model.SyncConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/02/18 17:16
 * @since 1.0
 */
//noinspection DuplicatedCode
class MyJdbcSink(options: Map[String, String], outputMode: OutputMode) extends Sink {

  private val gson = new Gson()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val syncConfig: SyncConfig = gson.fromJson(options.get("syncConfig").mkString, classOf[SyncConfig])
    val prop = new Properties()
    prop.setProperty("user", syncConfig.syncJdbc.user)
    prop.setProperty("password", syncConfig.syncJdbc.pwd)
    prop.setProperty("driver", syncConfig.syncJdbc.driver)
    data.write.mode(outputMode.toString).jdbc(syncConfig.syncJdbc.jdbcUrl,
      syncConfig.syncJdbc.schema + "." + syncConfig.syncJdbc.table, prop)
  }

}
