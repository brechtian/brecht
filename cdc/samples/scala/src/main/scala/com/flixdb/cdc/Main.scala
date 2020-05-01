package com.flixdb.cdc

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.io.StdIn

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

  val killSwitch =
    ChangeDataCapture
      .source(hikariDataSource, PgCdcSourceSettings(slotName = "cdc",
        dropSlotOnFinish = true,
        closeDataSourceOnFinish = true,
        pollInterval = 0.seconds
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
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.ignore)
      .run()


  println(
    s"""
       |_______________________
       |Press RETURN to stop...
       |-----------------------
       |""".stripMargin)

  StdIn.readLine() // let it run until user presses return

  logger.info("Using the KillSwitch")
  killSwitch.shutdown()

  system.terminate()


}