package com.flixdb.core

import akka.actor.typed.{ActorSystem, _}
import io.prometheus.client.Counter

object FlixDbConfig extends ExtensionId[FlixDbConfig] {

  private val metricRequestQueueSize = Counter
    .build()
    .name("config_request_queue_size")
    .help("Configuration: request queue size")
    .register()

  private val metricConcurrentRequests = Counter
    .build()
    .name("config_concurrent_requests")
    .help("Configuration: concurrent requests")
    .register()

  override def createExtension(system: ActorSystem[_]): FlixDbConfig =
    new FlixDbConfig(system)
}

class FlixDbConfig(actorSystem: ActorSystem[_]) extends Extension {

  import FlixDbConfig._

  private val config = actorSystem.settings.config.getConfig("flixdb")

  val port = config.getInt("port")
  val host = config.getString("host")

  val promPort = config.getInt("prometheus.port")
  val promHost = config.getString("prometheus.host")

  val requestQueueSize: Int = config.getInt("request-queue-size")

  val concurrentRequests: Int = config.getInt("concurrent-requests")

  // TODO: this should be defined in configuration
  def getTopicName(namespace: String, stream: String) = s"$namespace-$stream"

  // TODO: this should be defined in configuration
  def getTopicNameForTag(namespace: String, tag: String) = s"$namespace-$tag"

  metricRequestQueueSize.inc(requestQueueSize)
  metricConcurrentRequests.inc(concurrentRequests)

}
