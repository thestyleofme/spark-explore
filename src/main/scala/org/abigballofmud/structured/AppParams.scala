package org.abigballofmud.structured

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2019/11/18 13:46
 * @since 1.0
 */
class AppParams(var filePath: String) {

  private val appProperties: Config = ConfigFactory.parseFile(new File(filePath))

  // spark
  val sparkAppName: String = appProperties.getString("sparkAppName")
  val metastoreUris: String = appProperties.getString("metastoreUris")

  // kafka
  val kafkaBootstrapServers: String = appProperties.getString("kafkaBootstrapServers")

  // redis
  val redisHost: String = appProperties.getString("redisHost")
  val redisPort: Int = appProperties.getInt("redisPort")
  val redisPassword: String = appProperties.getString("redisPassword")

  // sync
  val kafkaTopic: String = appProperties.getString("kafkaTopic")
  val columns: List[String] = appProperties.getString("columns").trim.split(",").toList
  val interval: Long = appProperties.getLong("interval")

  val hiveDatabaseName: String = appProperties.getString("hiveDatabaseName")
  val hiveTableName: String = appProperties.getString("hiveTableName")

}
