package com.flixdb.core

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

object FlixDbConfig extends ExtensionId[FlixDbConfigImpl] with ExtensionIdProvider {

  override def lookup: FlixDbConfig.type = FlixDbConfig

  override def createExtension(system: ExtendedActorSystem) =
    new FlixDbConfigImpl(system)

}

class FlixDbConfigImpl(actorSystem: ExtendedActorSystem) extends Extension {

  private val config = actorSystem.settings.config.getConfig("flixdb")

  val requestQueueSize: Int = config.getInt("request-queue-size")

  val defaultNamespace: String = config.getString("default-namespace")

  // TODO: this should be defined in configuration
  def getTopicName(namespace: String, stream: String)
    = s"$namespace-$stream"



}
