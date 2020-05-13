package com.flixdb.core

import akka.actor.typed.{ActorSystem, _}

object FlixDbConfig extends ExtensionId[FlixDbConfig] {
  override def createExtension(system: ActorSystem[_]): FlixDbConfig =
    new FlixDbConfig(system)
}

class FlixDbConfig(actorSystem: ActorSystem[_]) extends Extension {

  private val config = actorSystem.settings.config.getConfig("flixdb")

  val requestQueueSize: Int = config.getInt("request-queue-size")

  val concurrentRequests: Int = config.getInt("concurrent-requests")

  // TODO: this should be defined in configuration
  def getTopicName(namespace: String, stream: String) = s"$namespace-$stream"

  // TODO: this should be defined in configuration
  def getTopicNameForTag(namespace: String, tag: String) = s"$namespace-$tag"

}
