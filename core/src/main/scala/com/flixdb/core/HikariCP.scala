package com.flixdb.core

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.typesafe.config.Config
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

object HikariCP extends ExtensionId[HikariCPImpl] with ExtensionIdProvider {

  override def lookup: HikariCP.type = HikariCP

  override def createExtension(system: ExtendedActorSystem) = new HikariCPImpl(system)

}

class HikariCPImpl(system: ExtendedActorSystem) extends Extension {

  private def buildHikariConfig(poolName: String): HikariConfig = {
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
    config.setJdbcUrl(jdbcUrl)
    config.setUsername(user)
    config.setPassword(pass)
    config.setMaximumPoolSize(maximumPoolSize)
    config.setMinimumIdle(minimumIdle)
    config.setRegisterMbeans(true)
    config.setPoolName(poolNameJmx)
    config.setConnectionTimeout(250) // the very minimum
    config.setValidationTimeout(
      // This property controls the maximum amount of time that a connection will be
      // tested for aliveness. This value must be less than the connectionTimeout.
      // Lowest acceptable validation timeout is 250 ms. Default: 5000
      250
    )

    config
  }

  def getPool(name: String): HikariDataSource =
    new HikariDataSource(buildHikariConfig(name))

}
