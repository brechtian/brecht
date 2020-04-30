package com.flixdb.core

import akka.actor.{ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import com.flixdb.core.protobuf.GetMsgs.PbGetEventsRequest
import com.flixdb.core.protobuf.PublishMsgs.PbPublishEventsRequest

object SubStreamSharding extends ExtensionId[SubStreamShardingImpl] with ExtensionIdProvider {

  override def lookup: SubStreamSharding.type = SubStreamSharding

  override def createExtension(system: ExtendedActorSystem) =
    new SubStreamShardingImpl(system)

}

class SubStreamShardingImpl(system: ExtendedActorSystem) extends Extension {

  private def buildId(namespace: String, stream: String, entityId: String): String = {
    s"$namespace-$stream-$entityId"
  }

  private def getId(msg: PbPublishEventsRequest): String = {
    buildId(msg.namespace, msg.stream, msg.subStreamId)
  }

  private def getId(msg: PbGetEventsRequest): String = {
    buildId(msg.namespace, msg.stream, msg.subStreamId)
  }

  private val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg: protobuf.GetMsgs.PbGetEventsRequest =>
      buildId(msg.namespace, msg.stream, msg.subStreamId) -> msg
    case msg: protobuf.PublishMsgs.PbPublishEventsRequest =>
      buildId(msg.namespace, msg.stream, msg.subStreamId) -> msg
  }

  private val numberOfShards = 100

  private val extractShardId: ShardRegion.ExtractShardId = {
    case msg: protobuf.GetMsgs.PbGetEventsRequest =>
      (Math.abs(getId(msg).hashCode % numberOfShards)).toString
    case msg: protobuf.PublishMsgs.PbPublishEventsRequest =>
      (Math.abs(getId(msg).hashCode % numberOfShards)).toString
  }

  val subStreams: ActorRef = ClusterSharding(system).start(
    typeName = "substreams",
    entityProps = Props[SubStreamActor],
    settings = ClusterShardingSettings(system),
    extractEntityId = extractEntityId,
    extractShardId = extractShardId
  )

}
