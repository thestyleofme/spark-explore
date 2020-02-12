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

case class SyncHive(metastoreUris: String) extends Serializable

case class SyncKafka(kafkaBootstrapServers: String,
                     kafkaTopic: String,
                     initDefaultOffset: String) extends Serializable

case class SyncRedis(redisHost: String,
                     redisPort: Int,
                     redisPassword: String,
                     redisDataBase: Int = Protocol.DEFAULT_DATABASE) extends Serializable

case class SyncSpark(sparkAppName: String,
                     columns: String,
                     interval: Int,
                     hiveDatabaseName: String,
                     hiveTableName: String,
                     writeType: String,
                     writeHdfsPath: String) extends Serializable

case class SyncJdbc(dbType: String,
                    pk: String,
                    saveMode: String,
                    driver: String,
                    jdbcUrl: String,
                    user: String,
                    pwd: String,
                    schema: String,
                    table: String) extends Serializable