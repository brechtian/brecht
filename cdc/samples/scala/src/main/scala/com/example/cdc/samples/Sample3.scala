package com.example.cdc.samples

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.server.Directives._
import akka.stream.KillSwitches
import akka.stream.scaladsl.{BroadcastHub, Keep}
import com.brecht.cdc.{Change, Modes, PgCdcSourceSettings, PostgreSQLInstance, RowDeleted, RowInserted, RowUpdated}
import com.brecht.cdc.scaladsl._
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, _}

import scala.concurrent.duration._
import scala.io.StdIn

class Sample3

object Sample3 extends App with SprayJsonSupport with DefaultJsonProtocol {

  val logger = LoggerFactory.getLogger(classOf[Sample3])

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

  val (killSwitch, source) = ChangeDataCapture(PostgreSQLInstance(hikariDataSource))
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
    .viaMat(KillSwitches.single)(Keep.right)
    .toMat(BroadcastHub.sink(bufferSize = 256))(Keep.both)
    .run()

  val route =
    path("hello") {
      get {
        complete {
          import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._
          source
            .collect {
              case r: RowInserted => r.data
              case r: RowDeleted => r.data
              case r: RowUpdated => r.dataNew
            }
            .map(event => event.toJson.prettyPrint)
            .map((json: String) => ServerSentEvent(json))
            .keepAlive(1.second, () => ServerSentEvent.heartbeat)
        }
      }
    }


  val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

  println(
    s"""
       |______________________________________
       |Server online at http://localhost:8080
       |Press RETURN to stop...
       |--------------------------------------
       |""".stripMargin)

  StdIn.readLine() // let it run until user presses return

  logger.info("Using the KillSwitch")
  killSwitch.shutdown()

  bindingFuture
    .flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ => system.terminate()) // and shutdown when done
}


