package org.abigballofmud.structured.app.utils

import java.sql.Connection
import java.util.Objects

import com.typesafe.scalalogging.Logger
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.abigballofmud.structured.app.model.SyncJdbc
import org.slf4j.LoggerFactory

/**
 * <p>
 * Hikari数据库连接池管理类
 * </p>
 *
 * @author isacc 2020/02/12 14:24
 * @since 1.0
 */
object HikariManager {

  private val log = Logger(LoggerFactory.getLogger(HikariManager.getClass))

  var dbManager: DbPool = _

  def getDbManager(syncJdbc: SyncJdbc): DbPool = {
    if (Objects.isNull(dbManager)) {
      synchronized {
        if (Objects.isNull(dbManager)) {
          dbManager = new DbPool(syncJdbc)
        }
      }
    }
    dbManager
  }

  class DbPool(syncJdbc: SyncJdbc) extends Serializable {

    private val dataSource = {
      try {
        val config = new HikariConfig
        config.setDriverClassName(syncJdbc.driver)
        config.setJdbcUrl(syncJdbc.jdbcUrl)
        config.setUsername(syncJdbc.user)
        config.setPassword(syncJdbc.pwd)
        config.setMaximumPoolSize(200)
        config.setConnectionTimeout(30000)
        config.setMinimumIdle(20)
        config.addDataSourceProperty("cachePrepStmts", "true")
        config.addDataSourceProperty("prepStmtCacheSize", "250")
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
        new HikariDataSource(config)
      } catch {
        case e: Exception => throw new IllegalArgumentException("hikari init error,", e)
      }
    }

    def getConnection: Connection = {
      try {
        dataSource.getConnection()
      } catch {
        case e: Exception =>
          log.error("hikari get connection error,", e)
          null
      }
    }

    def close(): Unit = {
      try {
        dataSource.close()
      } catch {
        case e: Exception =>
          log.error("hikari close error,", e)
      }
    }
  }

}
