package com.flixdb.core

import akka.actor.typed._
import com.typesafe.config.Config
import com.zaxxer.hikari.metrics.prometheus.PrometheusHistogramMetricsTrackerFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.prometheus.client.CollectorRegistry

object HikariCP extends ExtensionId[HikariCP] {

  private val metricsTrackerFactory = {
    new PrometheusHistogramMetricsTrackerFactory(CollectorRegistry.defaultRegistry)
  }

  override def createExtension(system: ActorSystem[_]): HikariCP =
    new HikariCP(system)
}

class HikariCP(system: ActorSystem[_]) extends Extension {

  import HikariCP._

  private def buildHikariConfig(poolName: String, metrics: Boolean): HikariConfig = {
    val typeSafeConfig: Config = system.settings.config
    val pgConfig: Config = typeSafeConfig.getConfig(poolName)
    val user: String = pgConfig.getString("user")
    val pass: String = pgConfig.getString("password")
    val host: String = pgConfig.getString("host")
    val port: Int = pgConfig.getInt("port")
    val db = pgConfig.getString("database")
    val jdbcUrl: String = s"jdbc:postgresql://${host}:${port}/$db"
    val maximumPoolSize: Int = pgConfig.getInt("maximumPoolSize")
    val minimumIdle: Int = pgConfig.getInt("minimumIdle")
    val poolNameJmx: String = poolName

    val config = new HikariConfig
    if (metrics) {
      // waiting for https://github.com/brettwooldridge/HikariCP/pull/1467
      // to get merged
      config.setMetricsTrackerFactory(metricsTrackerFactory)
    }
    config.setDriverClassName(classOf[org.postgresql.Driver].getName)
    config.setJdbcUrl(jdbcUrl)
    config.setUsername(user)
    config.setPassword(pass)
    config.setMaximumPoolSize(maximumPoolSize)
    config.setMinimumIdle(minimumIdle)
    config.setRegisterMbeans(true)
    config.setPoolName(poolNameJmx)
    config.setConnectionTimeout(450) // 250 is the minimum
    config.setValidationTimeout(
      // This property controls the maximum amount of time that a connection will be
      // tested for aliveness. This value must be less than the connectionTimeout.
      // Lowest acceptable validation timeout is 250 ms. Default: 5000
      300
    )

    config
  }

  def startHikariDataSource(name: String, metrics: Boolean): HikariDataSource =
    new HikariDataSource(buildHikariConfig(name, metrics))

}
