package com.flixdb.cdc.samples

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink}
import com.flixdb.cdc.scaladsl.{AckLogSeqNum, Change, Modes, PgCdcAckSettings, PgCdcSourceSettings, RowDeleted, RowInserted, RowUpdated, _}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}

class Sample1

object Sample1 extends App {

  val logger = LoggerFactory.getLogger(classOf[Sample1])

  implicit val system = ActorSystem()

  val hikariConfig: HikariConfig = {
    val cfg = new HikariConfig
    cfg.setDriverClassName(classOf[org.postgresql.Driver].getName)
    val url = s"jdbc:postgresql://localhost:5432/docker"
    logger.info("JdbcUrl is {}", url)
    cfg.setJdbcUrl(url)
    cfg.setUsername("docker")
    cfg.setPassword("docker")
    cfg.setMaximumPoolSize(2)
    cfg.setMinimumIdle(0)
    cfg.setPoolName("pg")
    cfg.setConnectionTimeout(300)
    cfg.setValidationTimeout(250)
    cfg
  }

  val hikariDataSource: HikariDataSource = new HikariDataSource(hikariConfig)

  hikariDataSource.validate()

  val ackFlow = ChangeDataCapture(hikariDataSource)
    .ackFlow[Change](PgCdcAckSettings("cdc"))

  val source = ChangeDataCapture(hikariDataSource)
    .source(PgCdcSourceSettings(slotName = "cdc",
      mode = Modes.Peek,
      dropSlotOnFinish = true,
      closeDataSourceOnFinish = true,
      pollInterval = 500.milliseconds
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
      .map(item => (item, AckLogSeqNum(item.commitLogSeqNum)))
      .via(ackFlow)
      .wireTap(item => logger.info("Acknowledged: {}", item._1))
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.onComplete {
        case Failure(exception) =>
          logger.info("Failed :-( !", exception)
        case Success(_) =>
          logger.info("Success :-) !")
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