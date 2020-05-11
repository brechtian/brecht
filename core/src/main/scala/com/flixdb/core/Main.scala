package com.flixdb.core

import akka.actor.ActorSystem
import akka.event.Logging
import com.flixdb.core.postgresql.PostgreSQL

import scala.concurrent.ExecutionContextExecutor

class Main
object Main extends App with JsonSupport {

  private implicit val system: ActorSystem = ActorSystem("flixdb")

  private val logger = Logging(system, classOf[Main])

  private implicit val executionContext: ExecutionContextExecutor =
    system.dispatcher

  private val flixDbConfiguration = FlixDbConfig(system)
  private val postgreSQL = PostgreSQL(system)

  private val httpInterface = new HttpInterface()

  for {
    validate <- postgreSQL.validate()
    _ = logger.info("Validated connection pool")
    schema <- postgreSQL.createTablesIfNotExists(flixDbConfiguration.defaultNamespace)
    _ = logger.info("Created schema")
    routes <- httpInterface.start()
    _ = logger.info("Started HTTP interface")
    _ = logger.info(Logo.Logo)
  } yield routes

}
