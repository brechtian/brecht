package com.flixdb.core

import akka.actor.ActorSystem
import akka.event.Logging
import akka.actor.typed.scaladsl.adapter._

object Main extends App with JsonSupport {

  implicit val system: ActorSystem = ActorSystem("flixdb")

  private val logger = Logging(system, "Main")

  // print logo
  logger.info(Logo.Logo)

  // start Kafka migrator
  KafkaMigration(system.toTyped)

  // start http interface
  HttpInterface(system.toTyped)

}
