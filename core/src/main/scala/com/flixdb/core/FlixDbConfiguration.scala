package com.flixdb.core

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

object FlixDbConfiguration extends ExtensionId[FlixDbConfigurationImpl] with ExtensionIdProvider {

  override def lookup: FlixDbConfiguration.type = FlixDbConfiguration

  override def createExtension(system: ExtendedActorSystem) =
    new FlixDbConfigurationImpl(system)

}

class FlixDbConfigurationImpl(actorSystem: ExtendedActorSystem) extends Extension {

  private val config = actorSystem.settings.config.getConfig("flixdb")
  val cdcKafkaStreamName = config.getString("change-data-capture-stream-name")

}
