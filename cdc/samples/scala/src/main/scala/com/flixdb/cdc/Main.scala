package com.flixdb.cdc

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.slf4j.LoggerFactory

class Main

object Main extends App {

  val logger = LoggerFactory.getLogger(classOf[Main])

  implicit val system = ActorSystem()

  val hikariConfig: HikariConfig = {
    val cfg = new HikariConfig
    cfg.setDriverClassName(classOf[org.postgresql.Driver].getName)
    val url = s"jdbc:postgresql://localhost:5432/pgdb1"
    logger.info("JdbcUrl is {}", url)
    cfg.setJdbcUrl(url)
    cfg.setUsername("pguser")
    cfg.setPassword("pguser")
    cfg.setMaximumPoolSize(2)
    cfg.setMinimumIdle(0)
    cfg.setPoolName("pg")
    cfg
  }

  val hikariDataSource: HikariDataSource = new HikariDataSource(hikariConfig)

  hikariDataSource.validate()

  ChangeDataCapture
    .source(hikariDataSource, PgCdcSourceSettings(slotName = "cdc",
      mode = Modes.Get,
      dropSlotOnFinish = true
    ))
    .mapConcat { changeSet => changeSet.changes } // ChangeSet (i.e. a transaction) can include multiple changes
    .collect {
      case r: RowInserted =>
        logger.info("Captured an insert {}", r)
      case r: RowDeleted =>
        logger.info("Captured a delete {}", r)
      case r: RowUpdated =>
        logger.info("Captured an update {}", r)
    }
    .to(Sink.ignore)
    .run()


  import system.dispatcher

  system.whenTerminated.foreach { _ =>
    logger.info("Shutting down connection pool")
    hikariDataSource.close()
  }

}