package com.flixdb.core

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.flixdb.core.protobuf.GetMsgs.{PbGetEventsRequest, PbGetEventsResult}
import com.flixdb.core.protobuf.PublishMsgs.{PbPublishEventsRequest, PbPublishEventsResult}

import scala.concurrent.Future

object EventStoreController extends ExtensionId[EventStoreControllerImpl] with ExtensionIdProvider {

  override def lookup = EventStoreController

  override def createExtension(system: ExtendedActorSystem) = new EventStoreControllerImpl(system)

}

class EventStoreControllerImpl(system: ExtendedActorSystem) extends Extension {

  // start change data capture stream to Kafka
  private val cdc = CdcStreamingToKafka(system)
  private val entitySharding = EntitySharding(system)

  private val entities = entitySharding.entities

  def getEvents(namespace: String, stream: String, entityId: String): Future[PbGetEventsResult] = {
    import scala.concurrent.duration._
    implicit val timeout = akka.util.Timeout(1.seconds) // TODO: move to configuration
    val pbMsg = PbGetEventsRequest.defaultInstance
      .withNamespace(namespace)
      .withStream(stream)
      .withEntityId(entityId)
    import akka.pattern.ask
    (entities ? pbMsg).mapTo[PbGetEventsResult]
  }

  def publishEvents(pbPublishEventsRequest: PbPublishEventsRequest): Future[PbPublishEventsResult] = {
    import scala.concurrent.duration._
    implicit val timeout = akka.util.Timeout(1.seconds) // TODO: move to configuration
    import akka.pattern.ask
    (entities ? pbPublishEventsRequest).mapTo[PbPublishEventsResult]
  }

}
