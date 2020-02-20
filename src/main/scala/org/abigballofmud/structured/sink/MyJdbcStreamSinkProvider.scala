package org.abigballofmud.structured.sink

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/02/18 17:33
 * @since 1.0
 */
class MyJdbcStreamSinkProvider extends StreamSinkProvider with DataSourceRegister {

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    new MyJdbcSink(parameters, outputMode)
  }

  override def shortName(): String = "myJdbc"

}
