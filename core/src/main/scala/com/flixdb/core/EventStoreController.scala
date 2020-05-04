package com.flixdb.core

import akka.actor._
import com.flixdb.core.protobuf.read._
import com.flixdb.core.protobuf.write._

import scala.concurrent.Future

object EventStoreController extends ExtensionId[EventStoreControllerImpl] with ExtensionIdProvider {

  override def lookup: EventStoreController.type = EventStoreController

  override def createExtension(system: ExtendedActorSystem) = new EventStoreControllerImpl(system)

}

class EventStoreControllerImpl(system: ExtendedActorSystem) extends Extension {

  private val cdc = CdcStreamingToKafka(system)
  private val entitySharding = SubStreamSharding(system)

  private val entities = entitySharding.subStreams

  def getEvents(namespace: String, stream: String, subStreamId: String): Future[PbGetEventsResult] = {
    import scala.concurrent.duration._
    implicit val timeout = akka.util.Timeout(1.seconds) // TODO: move to configuration
    val pbMsg = PbGetEventsRequest.defaultInstance
      .withNamespace(namespace)
      .withStream(stream)
      .withSubStreamId(subStreamId)
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
