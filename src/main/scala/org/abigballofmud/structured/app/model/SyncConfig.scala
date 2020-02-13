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
                      syncHive: SyncHive,
                      syncKafka: SyncKafka,
                      syncRedis: SyncRedis,
                      syncJdbc: SyncJdbc
                     ) extends Serializable

/**
 * 同步到hive的信息
 *
 * @param metastoreUris    hive metastoreUris
 * @param hiveDatabaseName 写入hive的库
 * @param hiveTableName    写入hive的表
 * @param writeHdfsPath    写入hdfs的路径
 */
case class SyncHive(metastoreUris: String,
                    hiveDatabaseName: String,
                    hiveTableName: String,
                    writeHdfsPath: String) extends Serializable

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
 * @param columns      待同步的字段，分号分割，字段之间不要有空，示例 id,name,age,sex
 * @param interval     spark采集周期
 * @param writeType    写入类型
 *
 */
case class SyncSpark(sparkAppName: String,
                     columns: String,
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