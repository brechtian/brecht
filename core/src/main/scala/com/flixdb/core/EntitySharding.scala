package com.flixdb.core

import akka.actor.{ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import com.flixdb.core.protobuf.GetMsgs.PbGetEventsRequest
import com.flixdb.core.protobuf.PublishMsgs.PbPublishEventsRequest

object EntitySharding extends ExtensionId[EntityShardingImpl] with ExtensionIdProvider {

  override def lookup = EntitySharding

  override def createExtension(system: ExtendedActorSystem) =
    new EntityShardingImpl(system)

}

class EntityShardingImpl(system: ExtendedActorSystem) extends Extension {

  private def buildId(namespace: String, stream: String, entityId: String) = {
    s"$namespace-$stream-$entityId"
  }

  private def getId(msg: PbPublishEventsRequest) = {
    buildId(msg.namespace, msg.stream, msg.entityId)
  }

  private def getId(msg: PbGetEventsRequest) = {
    buildId(msg.namespace, msg.stream, msg.entityId)
  }

  private val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg: protobuf.GetMsgs.PbGetEventsRequest =>
      buildId(msg.namespace, msg.stream, msg.entityId) -> msg
    case msg: protobuf.PublishMsgs.PbPublishEventsRequest =>
      buildId(msg.namespace, msg.stream, msg.entityId) -> msg
  }

  private val numberOfShards = 100

  private val extractShardId: ShardRegion.ExtractShardId = {
    case msg: protobuf.GetMsgs.PbGetEventsRequest =>
      (getId(msg).hashCode % numberOfShards).toString
    case msg: protobuf.PublishMsgs.PbPublishEventsRequest =>
      (getId(msg).hashCode % numberOfShards).toString
  }

  val entities: ActorRef = ClusterSharding(system).start(
    typeName = "Entity",
    entityProps = Props[EntityActor],
    settings = ClusterShardingSettings(system),
    extractEntityId = extractEntityId,
    extractShardId = extractShardId
  )

}
