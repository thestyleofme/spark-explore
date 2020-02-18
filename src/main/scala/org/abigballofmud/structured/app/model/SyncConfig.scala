package org.abigballofmud.structured.app.model

import redis.clients.jedis.Protocol

/**
 * <p>
 * description
 * </p>
 *
 * @author abigballofmud 2019/10/18 16:16
 * @since 1.0
 */
case class SyncConfig(syncSpark: SyncSpark,
                      syncColumns: java.util.List[SyncColumn],
                      syncFile: SyncFile,
                      syncKafka: SyncKafka,
                      syncRedis: SyncRedis,
                      syncJdbc: SyncJdbc
                     ) extends Serializable

/**
 * 要同步的字段
 *
 * @param colIndex 字段索引，指该字段在表中的顺序
 * @param typeName 字段类型，取值有，number,string,date
 * @param colName  字段名称
 */
case class SyncColumn(colIndex: Int,
                      typeName: String,
                      colName: String) extends Serializable

/**
 * 写文件，如果写hdfs文件到hive表目录即同步数据到hive
 *
 * @param metastoreUris      若写入hive metastoreUris
 * @param hiveDatabaseName   hive库
 * @param hiveTableName      hive表
 * @param format             写入文件类型，如csv, orc, json, parquet, etc.
 * @param writePath          写入文件的路径
 * @param writeMode          写入模式，一般是append
 * @param partitionBy        分区字段，逗号分割
 * @param checkpointLocation checkpoint存放路径
 */
case class SyncFile(metastoreUris: String,
                    hiveDatabaseName: String,
                    hiveTableName: String,
                    format: String,
                    writePath: String,
                    writeMode: String,
                    partitionBy: String,
                    checkpointLocation: String) extends Serializable

/**
 * kafka信息
 *
 * @param kafkaBootstrapServers kafkaBootstrapServers
 * @param kafkaTopic            topic
 * @param initDefaultOffset     初始offset
 */
case class SyncKafka(kafkaBootstrapServers: String,
                     kafkaTopic: String,
                     initDefaultOffset: String) extends Serializable

/**
 * redis缓存kafka消费情况
 *
 * @param redisHost     redisHost
 * @param redisPort     redisPort
 * @param redisPassword redisPassword
 * @param redisDataBase redisDataBase
 */
case class SyncRedis(redisHost: String,
                     redisPort: Int,
                     redisPassword: String,
                     redisDataBase: Int = Protocol.DEFAULT_DATABASE) extends Serializable

/**
 * spark实时同步配置
 *
 * @param sparkAppName appName
 * @param interval     spark采集周期
 * @param writeType    写入类型
 *
 */
case class SyncSpark(sparkAppName: String,
                     interval: Int,
                     writeType: String) extends Serializable

/**
 * jdbc类型同步信息
 *
 * @param dbType   数据库类型
 * @param pk       表的主键
 * @param saveMode 同步模式
 * @param driver   驱动
 * @param jdbcUrl  jdbc url
 * @param user     user
 * @param pwd      password
 * @param schema   数据库
 * @param table    写入的表名
 */
case class SyncJdbc(dbType: String,
                    pk: String,
                    saveMode: String,
                    driver: String,
                    jdbcUrl: String,
                    user: String,
                    pwd: String,
                    schema: String,
                    table: String) extends Serializable