package com.flixdb.core.postgresql

import akka.Done
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.{Behaviors, Routers}
import akka.actor.typed.{ActorRef, _}
import com.flixdb.core.{EventEnvelope, FlixDbConfig}

import scala.util.Try

object PostgreSQLActor {

  sealed trait Request

  final case class GetEventsRequest(
      namespace: String,
      stream: String,
      subStreamId: String,
      replyTo: ActorRef[GetEventsResult]
  ) extends Request

  final case class GetEventsResult(eventEnvelopes: Try[List[EventEnvelope]])

  final case class PublishEventsRequest(
      namespace: String,
      eventEnvelopes: List[EventEnvelope],
      replyTo: ActorRef[PublishEventsResult]
  ) extends Request

  final case class PublishEventsResult(result: Try[Done])

  final case class SnapshotRequest(namespace: String, eventEnvelope: EventEnvelope, replyTo: ActorRef[SnapshotResult])
      extends Request

  final case class SnapshotResult(result: Try[Done])

  final case class CreateTablesIfNotExists(namespace: String, replyTo: ActorRef[CreateTablesResult]) extends Request

  final case class CreateTablesResult(result: Try[Done])

  def apply(postgreSQL: PostgresSQLDataAccess): Behavior[Request] = Behaviors.receive { (_, message) =>
    message match {
      case GetEventsRequest(namespace, stream, subStreamId, replyTo) =>
        val r = postgreSQL.trySelectEvents(namespace, stream, subStreamId)
        replyTo ! GetEventsResult(r)
        Behaviors.same
      case PublishEventsRequest(namespace, eventEnvelopes, replyTo) =>
        val r = postgreSQL.tryAppendEvents(namespace, eventEnvelopes)
        replyTo ! PublishEventsResult(r)
        Behaviors.same
      case SnapshotRequest(namespace, eventEnvelope, replyTo) =>
        val r = postgreSQL.trySnapshot(namespace, eventEnvelope)
        replyTo ! SnapshotResult(r)
        Behaviors.same
      case CreateTablesIfNotExists(namespace, replyTo) =>
        val r = postgreSQL.tryCreateTablesIfNotExixts(namespace)
        replyTo ! CreateTablesResult(r)
        Behaviors.same

    }
  }

  def spawn(postgresSQLDataAccessLayer: PostgresSQLDataAccess)(implicit actorSystem: akka.actor.ActorSystem) =
    actorSystem.spawn(
      behavior = PostgreSQLActor.apply(postgresSQLDataAccessLayer),
      name = s"postgresql",
      DispatcherSelector.fromConfig("blocking-io-dispatcher")
    )

}

object PostgreSQL extends ExtensionId[PostgreSQL] {
  override def createExtension(system: ActorSystem[_]): PostgreSQL =
    new PostgreSQL(system)
}

class PostgreSQL(system: ActorSystem[_]) extends Extension {
  private implicit val classicSystem = system.toClassic

  private val config = FlixDbConfig(system)
  private val dataAccess = new PostgresSQLDataAccess()

  private val pool = Routers.pool(poolSize = config.concurrentRequests)(
    Behaviors.supervise(PostgreSQLActor(dataAccess)).onFailure[Exception](SupervisorStrategy.restart)
  )

  private val blockingPool = pool.withRouteeProps(routeeProps = DispatcherSelector.fromConfig("blocking-io-dispatcher"))

  val router: ActorRef[PostgreSQLActor.Request] =
    classicSystem.spawn(blockingPool, "worker-pool", DispatcherSelector.sameAsParent())

}
