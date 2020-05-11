package com.flixdb.cdc.samples

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink}
import com.flixdb.cdc.scaladsl._
import com.lonelyplanet.prometheus.api.MetricsEndpoint
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}

class Sample2

object Sample2 extends App {

  val logger = LoggerFactory.getLogger(classOf[Sample2])

  implicit val system = ActorSystem()

  import system.dispatcher

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

  val source = ChangeDataCapture(hikariDataSource)
    .source(
      PgCdcSourceSettings(
        slotName = "cdc",
        mode = Modes.Get,
        dropSlotOnFinish = true,
        closeDataSourceOnFinish = true,
        pollInterval = 500.milliseconds
      )
    )
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

  val killSwitch =
    source
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.onComplete {
        case Failure(exception) =>
          logger.info("Failed :-( !", exception)
        case Success(_) =>
          logger.debug("Success :-) !")
      })
      .run()

  import io.prometheus.client.CollectorRegistry
  import io.prometheus.client.dropwizard.DropwizardExports

  CollectorRegistry.defaultRegistry.register(
    new DropwizardExports(ChangeDataCapture.metricsRegistry))

  val metricsEndpoint = new MetricsEndpoint(CollectorRegistry.defaultRegistry)
  val routes = metricsEndpoint.routes
  val bindingFuture = Http().bindAndHandle(routes, interface = "0.0.0.0", port = 9091)


  println(
    s"""
       |_______________________
       |Press RETURN to stop...
       |-----------------------
       |""".stripMargin)

  StdIn.readLine() // let it run until user presses return

  logger.info("Using the KillSwitch")
  killSwitch.shutdown()

  bindingFuture
    .flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ => system.terminate()) // and shutdown when done


}
