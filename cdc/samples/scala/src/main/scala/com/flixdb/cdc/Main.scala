package com.flixdb.cdc

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}

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
    cfg.setConnectionTimeout(300)
    cfg.setValidationTimeout(250)
    cfg
  }

  val hikariDataSource: HikariDataSource = new HikariDataSource(hikariConfig)

  hikariDataSource.validate()

  val ackFlow: Flow[(Done, AckLogSeqNum), Done, NotUsed] = ChangeDataCapture.ackFlow[Done](hikariDataSource,
    PgCdcAckSettings("cdc", 20, 5.seconds)
  )

  val source = ChangeDataCapture
    .source(hikariDataSource, PgCdcSourceSettings(slotName = "cdc",
      mode = Modes.Peek,
      dropSlotOnFinish = true,
      closeDataSourceOnFinish = true,
      pollInterval = 500.milliseconds,
      maxItems = 4
    ))

  val killSwitch =
    source
      .mapConcat { changeSet => changeSet.changes } // ChangeSet (i.e. a transaction) can include multiple changes
      .wireTap { r: Change =>
        r match {
          case r: RowInserted =>
            logger.info("Captured an insert {}", r)
          case r: RowDeleted =>
            logger.info("Captured a delete {}", r)
          case r: RowUpdated =>
            logger.info("Captured an update {}", r)
        }
      }
      .map(item => (Done, AckLogSeqNum(item.commitLogSeqNum)))
      .via(ackFlow)
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.onComplete {
        case Failure(exception) =>
          logger.debug("Failed", exception)
        case Success(value) =>
          logger.debug("Success")
      }).run()


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