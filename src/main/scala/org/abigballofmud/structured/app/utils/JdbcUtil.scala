package org.abigballofmud.structured.app.utils

import java.sql.{Connection, DatabaseMetaData, Date, PreparedStatement, ResultSet, Statement, Timestamp}
import java.util.Properties

import com.typesafe.scalalogging.Logger
import org.abigballofmud.common.CommonUtil
import org.abigballofmud.structured.app.model.{SyncConfig, SyncJdbc, SyncSpark}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/02/12 15:02
 * @since 1.0
 */
//noinspection DuplicatedCode
object JdbcUtil {

  private val log = Logger(LoggerFactory.getLogger(JdbcUtil.getClass))

  /**
   * 将DataFrame所有类型(除自增主键外)转换为String后, 使用hikari连接池，向mysql批量写入数据
   *
   * @param syncConfig      SyncConfig
   * @param resultDateFrame DataFrame
   */
  def saveDFtoDBUsePool(syncConfig: SyncConfig, resultDateFrame: DataFrame) {
    val cols: List[String] = syncConfig.syncSpark.columns.trim.split(",").toList
    val columnDataTypes: Array[DataType] = resultDateFrame.schema.fields.map(_.dataType)
    val sql: String = getReplaceSql(syncConfig.syncJdbc.schema, syncConfig.syncJdbc.table, cols)
    resultDateFrame.foreachPartition(partitionRecords => {
      log.info("开始批量保存数据......")
      // 从连接池中获取一个连接
      val conn: Connection = HikariManager.getDbManager(syncConfig.syncJdbc).getConnection
      val preparedStatement: PreparedStatement = conn.prepareStatement(sql)
      // 通过连接获取表名对应数据表的元数据
      val metaData: ResultSet =
        conn.getMetaData.getColumns(null, "%", syncConfig.syncJdbc.schema + "." ++ syncConfig.syncJdbc.table, "%")
      try {
        conn.setAutoCommit(false)
        partitionRecords.foreach(record => {
          // 注意:setString方法从1开始，record.getString()方法从0开始
          log.info("record: {}", record.toString())
          if (record.getAs[String]("op").equals("d")) {
            // delete
            val condition: String = syncConfig.syncJdbc.pk + " = " + record.getAs[String](syncConfig.syncJdbc.pk)
            deleteByCondition(syncConfig.syncJdbc, condition)
          } else {
            // replace
            for (i <- 1 to cols.size) {
              val index: Int = i - 1
              val value: Any = record.get(index)
              val dateType: DataType = columnDataTypes(index)
              if (value != null) {
                // 如果值不为空,将类型转换为String
                preparedStatement.setString(i, value.toString)
                dateType match {
                  case _: ByteType => preparedStatement.setInt(i, record.getAs[Int](index))
                  case _: ShortType => preparedStatement.setInt(i, record.getAs[Int](index))
                  case _: IntegerType => preparedStatement.setInt(i, record.getAs[Int](index))
                  case _: LongType => preparedStatement.setLong(i, record.getAs[Long](index))
                  case _: BooleanType => preparedStatement.setBoolean(i, record.getAs[Boolean](index))
                  case _: FloatType => preparedStatement.setFloat(i, record.getAs[Float](index))
                  case _: DoubleType => preparedStatement.setDouble(i, record.getAs[Double](index))
                  case _: StringType => preparedStatement.setString(i, record.getAs[String](index))
                  case _: TimestampType => preparedStatement.setTimestamp(i, record.getAs[Timestamp](index))
                  case _: DateType => preparedStatement.setDate(i, record.getAs[Date](index))
                  case _ => throw new RuntimeException(s"nonsupport $dateType !!!")
                }
              } else {
                // 如果值为空,将值设为对应类型的空值
                metaData.absolute(i)
                preparedStatement.setNull(i, metaData.getInt("DATA_TYPE"))
              }
            }
            preparedStatement.addBatch()
          }
        })
        preparedStatement.executeBatch()
        conn.commit()
        log.info("批量保存数据完成......")
      } catch {
        case e: Exception => throw new IllegalArgumentException("saveDFtoDBUsePool error,", e)
      } finally {
        preparedStatement.close()
        conn.close()
      }
    })
  }

  /**
   * 拼装replace SQL
   *
   * @param schema    数据库名称
   * @param tableName 表名
   * @param cols      字段名称集合
   * @return replace sql
   */
  def getReplaceSql(schema: String, tableName: String, cols: List[String]): String = {
    var sqlStr: String = "replace into %s.%s(%s) values(".format(schema, tableName, cols.mkString("`", "`, `", "`"))
    for (i <- 1 to cols.size) {
      sqlStr += "?"
      if (i != cols.size) {
        sqlStr += ", "
      }
    }
    sqlStr += ")"
    sqlStr
  }

  /**
   * 从MySql数据库中获取DateFrame
   *
   * @param spark          SparkSession
   * @param syncJdbc       SyncJdbc
   * @param queryCondition 查询条件(可选)
   * @return DateFrame
   */
  def getDFFromMysql(spark: SparkSession, syncJdbc: SyncJdbc, queryCondition: String): DataFrame = {
    val prop = new Properties()
    prop.put("user", syncJdbc.user)
    prop.put("password", syncJdbc.pwd)
    prop.put("driver", syncJdbc.driver)
    if (null == queryCondition || "" == queryCondition)
      spark.read.jdbc(syncJdbc.jdbcUrl, syncJdbc.schema + "." + syncJdbc.table, prop)
    else
      spark.read.jdbc(syncJdbc.jdbcUrl, syncJdbc.schema + "." + syncJdbc.table, prop).where(queryCondition)
  }

  /**
   * 删除表
   *
   * @param syncJdbc SyncJdbc
   * @return
   */
  def dropTable(syncJdbc: SyncJdbc): Boolean = {
    val conn: Connection = HikariManager.getDbManager(syncJdbc).getConnection
    val preparedStatement: Statement = conn.createStatement()
    try {
      preparedStatement.execute(s"drop table ${syncJdbc.schema}.${syncJdbc.table}")
    } catch {
      case e: Exception =>
        log.error(s"drop table error,", e)
        false
    } finally {
      preparedStatement.close()
      conn.close()
    }
  }

  /**
   * 根据条件删除数据
   *
   * @param syncJdbc  SyncJdbc
   * @param condition 删除条件
   * @return
   */
  def deleteByCondition(syncJdbc: SyncJdbc, condition: String): Boolean = {
    val conn: Connection = HikariManager.getDbManager(syncJdbc).getConnection
    val preparedStatement: Statement = conn.createStatement()
    try {
      preparedStatement.execute(s"delete from `${syncJdbc.schema}`.`${syncJdbc.table}` where $condition")
    } catch {
      case e: Exception =>
        log.error(s"delete from table error,", e)
        false
    } finally {
      preparedStatement.close()
      conn.close()
    }
  }

  /**
   * 如果数据表不存在，根据DataFrame的字段创建数据表，数据表字段顺序和dataFrame对应
   *
   * @param syncJdbc SyncJdbc
   * @param df       dataFrame
   * @return
   */
  def createTableIfNotExist(syncJdbc: SyncJdbc, df: DataFrame): AnyVal = {
    val conn: Connection = HikariManager.getDbManager(syncJdbc).getConnection
    val metaData: DatabaseMetaData = conn.getMetaData
    val colResultSet: ResultSet = metaData.getColumns(null, "%", syncJdbc.schema + "." + syncJdbc.table, "%")
    // 如果没有该表,创建数据表
    if (!colResultSet.next()) {
      // 构建建表字符串
      val sb = new StringBuilder(s"CREATE TABLE `${syncJdbc.schema}`.`${syncJdbc.table}` (")
      df.schema.fields.foreach(x => {
        if (x.name.equalsIgnoreCase("op")) {
          return
        }
        if (x.name.equalsIgnoreCase(syncJdbc.pk)) {
          // 如果是字段名是主键，设置主键，整形，自增
          sb.append(s"`${x.name}` bigint(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,")
        } else {
          x.dataType match {
            case _: ByteType => sb.append(s"`${x.name}` int(11) DEFAULT NULL,")
            case _: ShortType => sb.append(s"`${x.name}` int(11) DEFAULT NULL,")
            case _: IntegerType => sb.append(s"`${x.name}` int(11) DEFAULT NULL,")
            case _: LongType => sb.append(s"`${x.name}` bigint(11) DEFAULT NULL,")
            case _: BooleanType => sb.append(s"`${x.name}` tinyint DEFAULT NULL,")
            case _: FloatType => sb.append(s"`${x.name}` float(50) DEFAULT NULL,")
            case _: DoubleType => sb.append(s"`${x.name}` double(38,10) DEFAULT NULL,")
            case _: StringType => sb.append(s"`${x.name}` varchar(50) DEFAULT NULL,")
            case _: TimestampType => sb.append(s"`${x.name}` timestamp DEFAULT current_timestamp,")
            case _: DateType => sb.append(s"`${x.name}` date DEFAULT NULL,")
            case _ => throw new RuntimeException(s"nonsupport ${x.dataType} !!!")
          }
        }
      }
      )
      sb.append(") ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin")
      val createTableSql: String = sb.deleteCharAt(sb.lastIndexOf(',')).toString()
      log.info("create table sql: {}", createTableSql)
      val statement: Statement = conn.createStatement()
      statement.execute(createTableSql)
    }
  }

  /**
   * 验证数据表和DataFrame字段个数，名称，顺序是否一致
   *
   * @param syncJdbc SyncJdbc
   * @param df       dataFrame
   */
  def verifyFieldConsistency(syncJdbc: SyncJdbc, df: DataFrame): Unit = {
    val conn: Connection = HikariManager.getDbManager(syncJdbc).getConnection
    val metaData: DatabaseMetaData = conn.getMetaData
    val colResultSet: ResultSet = metaData.getColumns(null, "%", syncJdbc.schema + "." + syncJdbc.table, "%")
    colResultSet.last()
    val tableFiledNum: Int = colResultSet.getRow
    val dfFiledNum: Int = df.columns.length
    // df里多了个op字段
    if (!tableFiledNum.equals(dfFiledNum - 1)) {
      throw new Exception(s"数据表和DataFrame字段个数不一致!!table--$tableFiledNum but dataFrame--$dfFiledNum")
    }
    for (i <- 1 to tableFiledNum) {
      colResultSet.absolute(i)
      val tableFileName: String = colResultSet.getString("COLUMN_NAME")
      val dfFiledName: String = df.columns.apply(i - 1)
      if (!tableFileName.equals(dfFiledName)) {
        throw new Exception(s"数据表和DataFrame字段名不一致!!table--'$tableFileName' but dataFrame--'$dfFiledName'")
      }
    }
    colResultSet.beforeFirst()
  }

  /**
   * 保存DataFrame到数据库中，如果表不存在的话，会自动创建
   *
   * @param syncConfig      SyncConfig
   * @param resultDateFrame DataFrame
   */
  def saveDFtoDBCreateTableIfNotExist(syncConfig: SyncConfig, resultDateFrame: DataFrame) {
    // 如果没有表，根据DataFrame建表
    createTableIfNotExist(syncConfig.syncJdbc, resultDateFrame)
    // 验证数据表字段和DataFrame字段个数和名称，顺序是否一致
    verifyFieldConsistency(syncConfig.syncJdbc, resultDateFrame)
    // 保存df
    saveDFtoDBUsePool(syncConfig, resultDateFrame)
  }


  def main(args: Array[String]): Unit = {
    //===========拼sql
    //    var cols: List[String] = List()
    //    cols = cols :+ "ts" :+ "topic" :+ "partition" :+ "offset"
    //    val sql: String = getReplaceSql("db", "test", cols)
    //    println(sql)
    //===========表数据转df
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("just_test")
    val syncJdbc: SyncJdbc = SyncJdbc("MYSQL", "", "",
      "com.mysql.jdbc.Driver",
      "jdbc:mysql://dev.hdsp.hand.com:7233/hdsp_test?useUnicode=true&characterEncoding=utf-8&useSSL=false",
      "hdsp_dev", "hdsp_dev", "hdsp_test", "dev_test_demo_0210")
    val spark: SparkSession = CommonUtil.getOrCreateSparkSession(conf)
    //    val df: DataFrame = getDFFromMysql(spark, syncJdbc, "id = 2")
    val df: DataFrame = spark.read.csv("src/main/resources/user.csv").toDF("id", "name")
    df.show()
    //===========删除数据
    //    println(deleteByCondition(syncJdbc, "id = 2"))
    //===========删表
    //    println(dropTable(syncJdbc))
    //===========df写入表 注意 syncSpark不写pk
    val syncConfig: SyncConfig = SyncConfig(SyncSpark("", "id,name", 10, "", "", "", ""), null, null, null, syncJdbc)
    saveDFtoDBCreateTableIfNotExist(syncConfig, df)
  }

}
